// Мёртвый Космос, Licensed under custom terms with restrictions on public hosting and commercial use, full text: https://raw.githubusercontent.com/dead-space-server/space-station-14-fobos/master/LICENSE.TXT
using System.Buffers.Binary;
using System.Reflection;
using Moq;
using NUnit.Framework;
using Robust.Server.Console;
using Robust.Server.Player;
using Robust.Server.Upload;
using Robust.Shared.Log;
using Robust.Shared.Asynchronous;
using Robust.Shared.Replays;
using Robust.Shared.Network;
using Robust.Shared.Network.Transfer;
using Robust.Shared.Player;
using Robust.Shared.Upload;
using Robust.Shared.Utility;

namespace Robust.Shared.IntegrationTests.Network;

[TestFixture]
public sealed class ResourceTransferTests
{
    [Test]
    public async Task ResourceBatchAndLateJoinUseTheUnchangedClientFormat()
    {
        var channel = Mock.Of<INetChannel>(c => c.IsConnected);
        var transfers = new Mock<ITransferManager>();
        MemoryStream download = null!;
        transfers.Setup(t => t.StartTransfer(It.IsAny<INetChannel>(), It.IsAny<TransferStartInfo>()))
            .Returns(() => download = new MemoryStream());
        var (manager, log) = Manager(transfers.Object);
        var net = new Mock<IServerNetManager>();
        net.SetupGet(n => n.Channels).Returns(new[] { channel });
        Set(manager, typeof(NetworkResourceManager), "_serverNetManager", net.Object);
        Set(manager, typeof(SharedNetworkResourceManager), "_replay", Mock.Of<IReplayRecordingManager>());
        var files = new List<(ResPath Relative, byte[] Data)>
        {
            (new ResPath("Prototypes/test.yml"), System.Text.Encoding.UTF8.GetBytes("- type: entity\n  id: TransferFixture\n")),
            (new ResPath("Audio/test.ogg"), new byte[3500000]),
        };
        var published = false;
        manager.ResourcesUploaded += ev => { published = true; Assert.That(ev.Files.Length, Is.EqualTo(files.Count)); };
        using var upload = new MemoryStream();
        await SharedNetworkResourceManager.WriteFileStream(upload, files);
        upload.Position = 0;
        await manager.ReceiveUpload(new TransferReceivedEvent(SharedNetworkResourceManager.TransferKeyNetworkUpload,
            channel, upload));
        Assert.That(published, Is.True);
        foreach (var (path, _) in files) Assert.That(manager.FileExists(path), Is.True);
        await CheckDownload(download.ToArray(), 0);
        Assert.That(manager.SendToNewUser(channel), Is.True);
        await CheckDownload(download.ToArray(), NetworkResourceManager.AckInitial);
        log.Verify(l => l.Warning(It.IsAny<string>()), Times.Never);
        manager.Dispose();

        async Task CheckDownload(byte[] encoded, int ack)
        {
            using var stream = new MemoryStream(encoded);
            var header = new byte[4];
            await stream.ReadExactlyAsync(header);
            Assert.That(BinaryPrimitives.ReadInt32LittleEndian(header), Is.EqualTo(ack));
            using var client = new OriginalClientReader();
            Set(client, typeof(SharedNetworkResourceManager), "Sawmill", log.Object);
            Set(client, typeof(SharedNetworkResourceManager), "_replay", Mock.Of<IReplayRecordingManager>());
            var tasks = new Mock<ITaskManager>();
            tasks.Setup(t => t.RunOnMainThread(It.IsAny<Action>())).Callback<Action>(a => a());
            Set(client, typeof(SharedNetworkResourceManager), "_taskManager", tasks.Object);
            var decoded = await client.Read(stream);
            Assert.That(decoded.Count, Is.EqualTo(files.Count));
            foreach (var (path, data) in files)
            {
                var received = decoded.Single(f => f.Relative == path);
                Assert.That(received.Data.AsSpan().SequenceEqual(data), Is.True);
            }
        }
    }

    [TestCase(0u, 0u)]
    [TestCase(uint.MaxValue, 0u)]
    [TestCase(4u, uint.MaxValue)]
    [TestCase(4u, 10u)]
    public async Task BadOrTruncatedUploadIsContained(uint pathLength, uint dataLength)
    {
        var (manager, log) = Manager(Mock.Of<ITransferManager>());
        var header = new byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(header, pathLength);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4), dataLength);
        var published = false;
        manager.ResourcesUploaded += _ => published = true;
        var stream = new MemoryStream(header);
        var transfer = new TransferReceivedEvent(SharedNetworkResourceManager.TransferKeyNetworkUpload,
            Mock.Of<INetChannel>(c => c.IsConnected), stream);
        await manager.ReceiveUpload(transfer);
        Assert.That(published, Is.False);
        Assert.That(stream.CanRead, Is.False);
        log.Verify(l => l.Warning(It.Is<string>(s => s.Contains("Resource upload"))), Times.Once);
        manager.Dispose();
    }

    [Test]
    public async Task DisconnectedDownloadIsContained()
    {
        var transfers = new Mock<ITransferManager>();
        transfers.Setup(t => t.StartTransfer(It.IsAny<INetChannel>(), It.IsAny<TransferStartInfo>()))
            .Throws(new IOException("Injected disconnect before transfer"));
        var (manager, log) = Manager(transfers.Object);
        await manager.SendToPlayer(Mock.Of<INetChannel>(), [(new ResPath("song.ogg"), new byte[16385])]);
        log.Verify(l => l.Warning(It.Is<string>(s => s.Contains("Resource download"))), Times.Once);
        manager.Dispose();
    }

    [Test]
    public async Task MidDownloadFailureIsContainedAndStreamDisposed()
    {
        var stream = new Mock<Stream>();
        stream.Setup(s => s.WriteAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.FromException(new IOException("Injected disconnect during write")));
        stream.Setup(s => s.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var transfers = new Mock<ITransferManager>();
        transfers.Setup(t => t.StartTransfer(It.IsAny<INetChannel>(), It.IsAny<TransferStartInfo>()))
            .Returns(stream.Object);
        var (manager, log) = Manager(transfers.Object);
        await manager.SendToPlayer(Mock.Of<INetChannel>(), [(new ResPath("test.yml"), new byte[10])]);
        stream.Verify(s => s.DisposeAsync(), Times.Once);
        log.Verify(l => l.Warning(It.Is<string>(s => s.Contains("Resource download"))), Times.Once);
        manager.Dispose();
    }

    private static (NetworkResourceManager Manager, Mock<ISawmill> Log) Manager(ITransferManager transfers)
    {
        var manager = new NetworkResourceManager();
        var log = new Mock<ISawmill>();
        var players = new Mock<IPlayerManager>();
        var session = Mock.Of<ICommonSession>();
        players.Setup(p => p.TryGetSessionByChannel(It.IsAny<INetChannel>(), out session)).Returns(true);
        var controller = new Mock<IConGroupController>();
        controller.Setup(c => c.CanCommand(It.IsAny<ICommonSession>(), "uploadfile")).Returns(true);
        Set(manager, typeof(SharedNetworkResourceManager), "Sawmill", log.Object);
        Set(manager, typeof(SharedNetworkResourceManager), "TransferManager", transfers);
        Set(manager, typeof(NetworkResourceManager), "_playerManager", players.Object);
        Set(manager, typeof(NetworkResourceManager), "_controller", controller.Object);
        return (manager, log);
    }

    private static void Set(object manager, Type type, string name, object value)
        => type.GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(manager, value);

    private sealed class OriginalClientReader : SharedNetworkResourceManager
    {
        public Task<List<(ResPath Relative, byte[] Data)>> Read(Stream stream) => IngestFileStream(stream);
    }
}
