// Мёртвый Космос, Licensed under custom terms with restrictions on public hosting and commercial use, full text: https://raw.githubusercontent.com/dead-space-server/space-station-14-fobos/master/LICENSE.TXT
using System.Buffers.Binary;
using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using Moq;
using NUnit.Framework;
using Robust.Server.Network.Transfer;
using Robust.Shared.Asynchronous;
using Robust.Shared.Configuration;
using Robust.Shared.Log;
using Robust.Shared.Network;
using Robust.Shared.Network.Transfer;
using Robust.Shared.Network.Messages.Transfer;
using Robust.UnitTesting;

namespace Robust.Shared.IntegrationTests.Network;

[TestFixture]
public sealed class TransferTests
{
    private const string Key = "DS14.TransferRegression";
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Test]
    public async Task FragmentedHeaderIsAccepted()
    {
        using var fixture = new Fixture();
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Manager.RegisterTransferMessage(Key, async ev =>
        {
            try
            {
                await using var stream = ev.DataStream;
                using var data = new MemoryStream();
                await stream.CopyToAsync(data);
                received.TrySetResult(data.ToArray());
            }
            catch (Exception e) { received.TrySetException(e); }
        });
        fixture.Impl.BeginRead();
        var header = Header(1, 7);
        fixture.Socket.Enqueue(header[..3], false);
        fixture.Socket.Enqueue(header[3..9], false);
        fixture.Socket.Enqueue(header[9..], true);
        fixture.Socket.Enqueue([1, 2, 3], true);
        Assert.That(await received.Task.WaitAsync(Timeout), Is.EqualTo(new byte[] { 1, 2, 3 }));
        fixture.Net.Verify(c => c.Disconnect(It.IsAny<string>()), Times.Never);
    }

    [Test]
    public async Task RejectedStreamDoesNotBlockNextTransfer()
    {
        using var fixture = new Fixture();
        var next = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var count = 0;
        fixture.Manager.RegisterTransferMessage(Key, ev =>
        {
            ev.DataStream.Dispose();
            if (Interlocked.Increment(ref count) == 2)
                next.TrySetResult();
        });
        fixture.Impl.BeginRead();
        fixture.Socket.Enqueue(Header(1, 1), true);
        // More chunks than the bounded receive queue can hold.
        for (var i = 0; i < 12; i++)
        {
            fixture.Socket.Enqueue(Header(1, i == 11 ? (byte)6 : (byte)4), true);
            fixture.Socket.Enqueue(new byte[16384], true);
        }
        fixture.Socket.Enqueue(Header(2, 3), true);
        await next.Task.WaitAsync(Timeout);
        fixture.Net.Verify(c => c.Disconnect(It.IsAny<string>()), Times.Never);
    }

    [Test]
    public async Task StreamLimitIncludesExactlyTheConfiguredNumber()
    {
        using var fixture = new Fixture();
        fixture.Impl.MaxChannelCount = 1;
        var first = new TaskCompletionSource<Stream>(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Manager.RegisterTransferMessage(Key, ev => first.TrySetResult(ev.DataStream));
        fixture.Impl.BeginRead();
        fixture.Socket.Enqueue(Header(1, 1), true);
        await using var stream = await first.Task.WaitAsync(Timeout);
        fixture.Net.Verify(c => c.Disconnect(It.IsAny<string>()), Times.Never);
        fixture.Socket.Enqueue(Header(2, 1), true);
        await fixture.Disconnected.Task.WaitAsync(Timeout);
        Assert.That(fixture.Manager.Received, Is.EqualTo(1));
    }

    [Test]
    public async Task ReceiveCancellationIsHonored()
    {
        using var fixture = new Fixture();
        var first = new TaskCompletionSource<Stream>(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Manager.RegisterTransferMessage(Key, ev => first.TrySetResult(ev.DataStream));
        fixture.Impl.BeginRead();
        fixture.Socket.Enqueue(Header(1, 1), true);
        await using var stream = await first.Task.WaitAsync(Timeout);
        using var cancel = new CancellationTokenSource();
        var pending = stream.ReadAsync(new byte[1], cancel.Token).AsTask();
        cancel.Cancel();
        try
        {
            await pending.WaitAsync(Timeout);
            Assert.Fail("Cancelled reads must not remain blocked or succeed.");
        }
        catch (OperationCanceledException) { }
    }

    [Test]
    public async Task DisposeCancelsPendingReadBeforeDrainingTheQueue()
    {
        using var fixture = new Fixture();
        var first = new TaskCompletionSource<Stream>(TaskCreationOptions.RunContinuationsAsynchronously);
        var next = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = 0;
        fixture.Manager.RegisterTransferMessage(Key, ev =>
        {
            if (Interlocked.Increment(ref received) == 1) first.TrySetResult(ev.DataStream);
            else { ev.DataStream.Dispose(); next.TrySetResult(); }
        });
        fixture.Impl.BeginRead();
        fixture.Socket.Enqueue(Header(1, 1), true);
        var stream = await first.Task.WaitAsync(Timeout);
        var pending = stream.ReadAsync(new byte[1]).AsTask();
        stream.Dispose();
        Assert.ThrowsAsync<OperationCanceledException>(async () => await pending.WaitAsync(Timeout));
        for (var i = 0; i < 12; i++)
        {
            fixture.Socket.Enqueue(Header(1, i == 11 ? (byte)6 : (byte)4), true);
            fixture.Socket.Enqueue(new byte[16384], true);
        }
        fixture.Socket.Enqueue(Header(2, 3), true);
        await next.Task.WaitAsync(Timeout);
        fixture.Net.Verify(c => c.Disconnect(It.IsAny<string>()), Times.Never);
    }

    [Test]
    public async Task ServerDisconnectClosesPendingRead()
    {
        using var fixture = new Fixture();
        var first = new TaskCompletionSource<Stream>(TaskCreationOptions.RunContinuationsAsynchronously);
        fixture.Manager.RegisterTransferMessage(Key, ev => first.TrySetResult(ev.DataStream));
        using var server = new ServerTransferImplWebSocket(new DummySawmill(), fixture.Manager,
            Mock.Of<IConfigurationManager>(), Mock.Of<INetManager>(), fixture.Net.Object, fixture.Manager.Tasks);
        server.WebSocket = fixture.Socket;
        // Exercise the real server override, including a receiving channel.
        _ = server.ReadThread();
        fixture.Socket.Enqueue(Header(1, 1), true);
        await using var stream = await first.Task.WaitAsync(Timeout);
        var pending = stream.ReadAsync(new byte[1]).AsTask();
        server.Dispose();
        try { Assert.That(await pending.WaitAsync(Timeout), Is.Zero); }
        catch (IOException) { }
        Assert.That(fixture.Socket.State, Is.EqualTo(WebSocketState.Closed));
    }

    [Test]
    public async Task DoubleDisposeDoesNotSendAnotherFinish()
    {
        using var fixture = new Fixture();
        var stream = fixture.Impl.StartTransfer(new TransferStartInfo { MessageKey = Key });
        await stream.WriteAsync(new byte[] { 42 });
        await stream.DisposeAsync();
        var sent = fixture.Socket.Sent;
        await stream.DisposeAsync();
        stream.Dispose();
        Assert.That(fixture.Socket.Sent, Is.EqualTo(sent));
    }

    [TestCase(1)]
    [TestCase(2)]
    public async Task FailedFrameIsNotRetriedDuringDispose(int failedMessage)
    {
        using var fixture = new Fixture();
        fixture.Socket.FailOnSend = failedMessage;
        var stream = fixture.Impl.StartTransfer(new TransferStartInfo { MessageKey = Key });
        Assert.ThrowsAsync<IOException>(async () => await stream.WriteAsync(new byte[16384]));
        Assert.That(stream.CanWrite, Is.False);
        await stream.DisposeAsync();
        Assert.That(fixture.Socket.Sent, Is.EqualTo(failedMessage), "Neither Start nor Finish may follow a failed frame.");
        fixture.Net.Verify(c => c.Disconnect(It.IsAny<string>()), Times.Once);
    }

    [Test]
    public async Task FailedFrameStopsOtherStreamsOnTheSocket()
    {
        using var fixture = new Fixture();
        fixture.Socket.FailOnSend = 2;
        await using var first = fixture.Impl.StartTransfer(new TransferStartInfo { MessageKey = Key });
        await using var second = fixture.Impl.StartTransfer(new TransferStartInfo { MessageKey = Key });
        Assert.ThrowsAsync<IOException>(async () => await first.WriteAsync(new byte[16384]));
        Assert.ThrowsAsync<IOException>(async () => await second.WriteAsync(new byte[1]));
        await first.DisposeAsync();
        await second.DisposeAsync();
        Assert.That(fixture.Socket.Sent, Is.EqualTo(2));
        fixture.Net.Verify(c => c.Disconnect(It.IsAny<string>()), Times.Once);
    }

    [Test]
    public void FailedDisposeCannotCrashFinalizerThread()
    {
        using var fixture = new Fixture();
        fixture.Socket.FailSend = true;
        var weak = FailDispose(fixture.Impl);
        for (var i = 0; i < 3 && weak.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
        Assert.That(weak.IsAlive, Is.False);
        Assert.That(fixture.Socket.Sent, Is.EqualTo(1));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference FailDispose(TestImpl impl)
    {
        var stream = impl.StartTransfer(new TransferStartInfo { MessageKey = Key });
        try { stream.DisposeAsync().AsTask().GetAwaiter().GetResult(); }
        catch (Exception) { }
        return new WeakReference(stream);
    }

    [Test]
    public async Task SendingRequiresAsyncOperationsAndSyncDisposeNeverWaits()
    {
        using var fixture = new Fixture();
        var stream = fixture.Impl.StartTransfer(new TransferStartInfo { MessageKey = Key });
        Assert.Throws<NotSupportedException>(() => stream.Write(new byte[1]));
        Assert.Throws<NotSupportedException>(() => stream.Flush());
        await stream.WriteAsync(new byte[16384], 0, 16384);
        Assert.That(fixture.Socket.Sent, Is.EqualTo(2));
        stream.Dispose();
        Assert.That(fixture.Socket.Sent, Is.EqualTo(2));
        fixture.Net.Verify(c => c.Disconnect(It.IsAny<string>()), Times.Once);
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(9)]
    [TestCase(129)]
    public async Task MalformedHeaderDisconnectsOnlyItsChannel(int size)
    {
        using var fixture = new Fixture();
        fixture.Impl.BeginRead();
        fixture.Socket.Enqueue(new byte[size], true);
        await fixture.Disconnected.Task.WaitAsync(Timeout);
    }

    [TestCase(1, 32, 65537, 0)]
    [TestCase(4, 16, 3500000, 0)]
    [TestCase(4, 8, 3500000, 1)]
    [TestCase(4, 8, 3500000, 2)]
    public async Task ConcurrentLoopbackTransfersKeepEveryByte(int connections, int streams, int bytes, int legacyMode)
    {
        // Real TCP and .NET WebSockets, including 16 KiB chunk boundaries and concurrent song-sized transfers.
        await Task.WhenAll(Enumerable.Range(0, connections).Select(async connection =>
        {
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            using var client = new TcpClient();
            var accept = listener.AcceptTcpClientAsync();
            await client.ConnectAsync((IPEndPoint)listener.LocalEndpoint);
            using var server = await accept;
            using var txSocket = WebSocket.CreateFromStream(client.GetStream(), false, null, TimeSpan.FromMinutes(1));
            using var rxSocket = WebSocket.CreateFromStream(server.GetStream(), true, null, TimeSpan.FromMinutes(1));
            using var fixture = new Fixture();
            using var tx = new TestImpl(fixture.Manager, fixture.Net.Object) { WebSocket = txSocket };
            using var rx = new TestImpl(fixture.Manager, fixture.Net.Object) { WebSocket = rxSocket };
            // The upstream Shared implementations remain unchanged and stand in for the external client engine.
            using var legacyTx = new LegacyClientImpl(fixture.Manager, fixture.Net.Object) { WebSocket = txSocket };
            using var legacyRx = new LegacyClientImpl(fixture.Manager, fixture.Net.Object) { WebSocket = rxSocket };
            var reads = Channel.CreateUnbounded<Task>();
            fixture.Manager.RegisterTransferMessage(Key, ev => reads.Writer.TryWrite(Check(ev.DataStream)));
            if (legacyMode == 2) legacyRx.BeginRead();
            else rx.BeginRead();
            async Task Check(Stream stream)
            {
                await using (stream)
                {
                    var payload = new byte[bytes];
                    await stream.ReadExactlyAsync(payload);
                    var seed = payload[0];
                    var expected = new byte[bytes];
                    for (var i = 0; i < expected.Length; i++) expected[i] = (byte)(seed + i);
                    Assert.That(payload.AsSpan().SequenceEqual(expected), Is.True, $"connection {connection}, stream {seed}");
                    Assert.That(await stream.ReadAsync(new byte[1]), Is.Zero);
                }
            }
            await Task.WhenAll(Enumerable.Range(0, streams).Select(async index =>
            {
                var payload = new byte[bytes];
                for (var i = 0; i < bytes; i++) payload[i] = (byte)(index + i);
                var info = new TransferStartInfo { MessageKey = Key };
                await using var stream = legacyMode == 1 ? legacyTx.StartTransfer(info) : tx.StartTransfer(info);
                await stream.WriteAsync(payload);
            })).WaitAsync(TimeSpan.FromSeconds(60));
            for (var i = 0; i < streams; i++)
                await (await reads.Reader.ReadAsync()).WaitAsync(TimeSpan.FromSeconds(60));
            fixture.Net.Verify(c => c.Disconnect(It.IsAny<string>()), Times.Never);
        }));
    }

    private static byte[] Header(long id, byte flags)
    {
        var key = Encoding.UTF8.GetBytes(Key);
        var header = new byte[(flags & 1) != 0 ? 11 + key.Length : 10];
        header[1] = flags;
        BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(2, 8), id);
        if ((flags & 1) != 0)
        {
            header[10] = (byte)key.Length;
            key.CopyTo(header, 11);
        }
        return header;
    }

    [Test]
    [Explicit("Repeated multi-gigabyte loopback load with the unchanged external client transport.")]
    public async Task RepeatedLegacyClientLoad()
    {
        for (var i = 0; i < 20; i++)
        {
            await ConcurrentLoopbackTransfersKeepEveryByte(4, 8, 3500000, 1);
            await ConcurrentLoopbackTransfersKeepEveryByte(4, 8, 3500000, 2);
        }
    }

    [Test]
    [Explicit("128 simultaneous downloads to 16 unchanged client transports.")]
    public Task ManyLegacyClientDownloads()
        => ConcurrentLoopbackTransfersKeepEveryByte(16, 8, 3500000, 2);

    [Test]
    public async Task ConcurrentDisconnectDoesNotCrashFinalizers()
    {
        using var fixture = new Fixture();
        fixture.Socket.FailSend = true;
        await Task.WhenAll(Enumerable.Range(0, 32).Select(_ => Task.Run(async () =>
        {
            try
            {
                await using var stream = fixture.Impl.StartTransfer(new TransferStartInfo { MessageKey = Key });
                await stream.WriteAsync(new byte[65537]);
            }
            catch (IOException) { }
        }))).WaitAsync(Timeout);
        GC.Collect();
        GC.WaitForPendingFinalizers();
    }

    [Test]
    public async Task LidgrenFallbackKeepsConcurrentSongBytes()
    {
        using var fixture = new Fixture();
        var channel = new Mock<INetChannel>();
        channel.SetupGet(c => c.IsConnected).Returns(true);
        channel.Setup(c => c.CanSendImmediately(It.IsAny<Lidgren.Network.NetDeliveryMethod>(), It.IsAny<int>()))
            .Returns(true);
        using var receiver = new ServerTransferImplLidgren(new DummySawmill(), channel.Object, fixture.Manager,
            Mock.Of<INetManager>(), fixture.Manager.Tasks);
        using var sender = new ServerTransferImplLidgren(new DummySawmill(), channel.Object, fixture.Manager,
            Mock.Of<INetManager>(), fixture.Manager.Tasks);
        channel.Setup(c => c.SendMessage(It.IsAny<NetMessage>())).Callback<NetMessage>(message =>
        {
            var data = ((MsgTransferData)message).Data;
            var copy = ArrayPool<byte>.Shared.Rent(data.Count);
            data.AsSpan().CopyTo(copy);
            receiver.ReceiveData(new MsgTransferData { Data = new ArraySegment<byte>(copy, 0, data.Count) });
        });
        var reads = Channel.CreateUnbounded<Task>();
        fixture.Manager.RegisterTransferMessage(Key, ev => reads.Writer.TryWrite(Check(ev.DataStream)));
        async Task Check(Stream stream)
        {
            await using (stream)
            {
                var data = new byte[3500000];
                await stream.ReadExactlyAsync(data);
                var expected = new byte[data.Length];
                for (var i = 0; i < expected.Length; i++) expected[i] = (byte)(data[0] + i);
                Assert.That(data.AsSpan().SequenceEqual(expected), Is.True);
                Assert.That(await stream.ReadAsync(new byte[1]), Is.Zero);
            }
        }
        await Task.WhenAll(Enumerable.Range(0, 8).Select(index => Task.Run(async () =>
        {
            var data = new byte[3500000];
            for (var i = 0; i < data.Length; i++) data[i] = (byte)(index + i);
            await using var stream = sender.StartTransfer(new TransferStartInfo { MessageKey = Key });
            await stream.WriteAsync(data);
        }))).WaitAsync(Timeout);
        for (var i = 0; i < 8; i++) await (await reads.Reader.ReadAsync()).WaitAsync(Timeout);
    }

    private sealed class Fixture : IDisposable
    {
        public readonly Mock<INetChannel> Net = new();
        public readonly TestSocket Socket = new();
        public readonly TestManager Manager;
        public readonly TestImpl Impl;
        public readonly TaskCompletionSource Disconnected = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Fixture()
        {
            Net.SetupGet(c => c.IsConnected).Returns(true);
            Net.Setup(c => c.Disconnect(It.IsAny<string>())).Callback(() => Disconnected.TrySetResult());
            var tasks = new Mock<ITaskManager>();
            tasks.Setup(t => t.RunOnMainThread(It.IsAny<Action>())).Callback<Action>(a => a());
            Manager = new TestManager(tasks.Object);
            Impl = new TestImpl(Manager, Net.Object) { WebSocket = Socket };
        }
        public void Dispose() => Impl.Dispose();
    }

    private sealed class TestManager : BaseTransferManager
    {
        public int Received;
        public readonly ITaskManager Tasks;
        public TestManager(ITaskManager tasks) : base(LogManager(), NetMessageAccept.Server, tasks) { Tasks = tasks; }
        private static ILogManager LogManager()
        {
            var log = new Mock<ILogManager>();
            log.Setup(l => l.GetSawmill(It.IsAny<string>())).Returns(new DummySawmill());
            return log.Object;
        }
        public void RegisterTransferMessage(string key, Action<TransferReceivedEvent> callback)
            => base.RegisterTransferMessage(key, ev => { Interlocked.Increment(ref Received); callback(ev); });
    }

    private sealed class TestImpl(TestManager manager, INetChannel channel) : IDisposable
    {
        private readonly ServerTransferImplWebSocket _impl = new(new DummySawmill(), manager,
            Mock.Of<IConfigurationManager>(), Mock.Of<INetManager>(), channel, manager.Tasks);
        public WebSocket? WebSocket { get => _impl.WebSocket; set => _impl.WebSocket = value; }
        public int MaxChannelCount { set => _impl.MaxChannelCount = value; }
        public Stream StartTransfer(TransferStartInfo info) => _impl.StartTransfer(info);
        public void BeginRead() => _ = _impl.ReadThread();
        public void Dispose() => _impl.Dispose();
    }

    private sealed class LegacyClientImpl(TestManager manager, INetChannel channel)
        : TransferImplWebSocket(new DummySawmill(), manager, channel)
    {
        public void BeginRead() => ReadThread();
        public override Task ServerInit() => Task.CompletedTask;
        public override Task ClientInit(CancellationToken cancel) => Task.CompletedTask;
    }

    private sealed class TestSocket : WebSocket
    {
        private readonly Channel<(byte[] Data, bool End)> _incoming = Channel.CreateUnbounded<(byte[], bool)>();
        private WebSocketState _state = WebSocketState.Open;
        private (byte[] Data, bool End)? _pending;
        private int _offset;
        public int Sent;
        public bool FailSend;
        public int FailOnSend;
        public override WebSocketCloseStatus? CloseStatus => null;
        public override string? CloseStatusDescription => null;
        public override string? SubProtocol => null;
        public override WebSocketState State => _state;
        public void Enqueue(byte[] data, bool end) => _incoming.Writer.TryWrite((data, end));
        public override void Abort() => Dispose();
        public override void Dispose() { _state = WebSocketState.Closed; _incoming.Writer.TryComplete(); }
        public override Task CloseAsync(WebSocketCloseStatus status, string? description, CancellationToken cancel)
            => Task.CompletedTask;
        public override Task CloseOutputAsync(WebSocketCloseStatus status, string? description, CancellationToken cancel)
            => Task.CompletedTask;
        public override Task SendAsync(ArraySegment<byte> data, WebSocketMessageType type, bool end, CancellationToken cancel)
        {
            var sent = Interlocked.Increment(ref Sent);
            return FailSend || sent == FailOnSend
                ? Task.FromException(new IOException("Injected disconnected socket"))
                : Task.CompletedTask;
        }
        public override async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancel)
        {
            _pending ??= await _incoming.Reader.ReadAsync(cancel);
            var (data, end) = _pending.Value;
            var count = Math.Min(buffer.Count, data.Length - _offset);
            data.AsMemory(_offset, count).CopyTo(buffer.AsMemory());
            _offset += count;
            var finished = _offset == data.Length;
            if (finished) { _pending = null; _offset = 0; }
            return new WebSocketReceiveResult(count, WebSocketMessageType.Binary, end && finished);
        }
    }
}
