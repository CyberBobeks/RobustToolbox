// Мёртвый Космос, Licensed under custom terms with restrictions on public hosting and commercial use, full text: https://raw.githubusercontent.com/dead-space-server/space-station-14-fobos/master/LICENSE.TXT
using System.Reflection;
using Moq;
using NUnit.Framework;
using Robust.Server.Network.Transfer;
using Robust.Server.ServerStatus;
using Robust.Shared.Asynchronous;
using Robust.Shared.Configuration;
using Robust.Shared.Log;
using Robust.Shared.Network;
using Robust.Shared.Network.Messages.Transfer;
using Robust.Shared.Network.Transfer;
using Robust.UnitTesting;

namespace Robust.Shared.IntegrationTests.Network;

[TestFixture]
public sealed class TransferSessionTests
{
    [Test]
    public async Task StaleChannelCannotUseOrRemoveReplacementTransfer()
    {
        var config = new Mock<IConfigurationManager>();
        config.Setup(c => c.GetCVar(CVars.TransferStreamLimit)).Returns(10);
        var net = new Mock<IServerNetManager>();
        var logs = new Mock<ILogManager>();
        logs.Setup(l => l.GetSawmill(It.IsAny<string>())).Returns(new DummySawmill());
        var manager = new ServerTransferManager(config.Object, Mock.Of<IStatusHost>(), net.Object,
            logs.Object, Mock.Of<ITaskManager>());
        manager.Initialize();
        const string key = "TransferSessionTest";
        manager.RegisterTransferMessage(key);
        var user = new NetUserId(Guid.NewGuid());
        var oldChannel = Mock.Of<INetChannel>(c => c.UserId == user && c.IsConnected);
        var newChannel = Mock.Of<INetChannel>(c => c.UserId == user && c.IsConnected);
        var oldHandshake = manager.ServerHandshake(oldChannel);
        Ack(oldChannel);
        await oldHandshake.WaitAsync(TimeSpan.FromSeconds(10));
        net.Raise(n => n.Disconnect += null, new NetDisconnectedArgs(oldChannel, "reconnect"));
        var newHandshake = manager.ServerHandshake(newChannel);
        Ack(newChannel);
        await newHandshake.WaitAsync(TimeSpan.FromSeconds(10));
        net.Raise(n => n.Disconnect += null, new NetDisconnectedArgs(oldChannel, "late duplicate disconnect"));
        var info = new TransferStartInfo { MessageKey = key };
        Assert.Throws<InvalidOperationException>(() => manager.StartTransfer(oldChannel, info));
        Assert.Throws<ArgumentException>(() => manager.StartTransfer(newChannel,
            new TransferStartInfo { MessageKey = "NotRegistered" }));
        await using var stream = manager.StartTransfer(newChannel, info);
        net.Raise(n => n.Disconnect += null, new NetDisconnectedArgs(newChannel, "cleanup"));

        void Ack(INetChannel channel)
            => typeof(ServerTransferManager).GetMethod("RxTransferAckInit", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(manager, [new MsgTransferAckInit { MsgChannel = channel }]);
    }
}
