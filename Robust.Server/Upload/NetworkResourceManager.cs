using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Robust.Server.Console;
using Robust.Server.Player;
using Robust.Shared;
using Robust.Shared.Configuration;
using Robust.Shared.IoC;
using Robust.Shared.Network;
using Robust.Shared.Network.Transfer;
using Robust.Shared.Player;
using Robust.Shared.Upload;
using Robust.Shared.Utility;
using Robust.Shared.ViewVariables;

namespace Robust.Server.Upload;

public sealed class NetworkResourcesUploadedEvent
{
    public ICommonSession Session { get; }
    public ImmutableArray<(ResPath Relative, byte[] Data)> Files { get; }

    internal NetworkResourcesUploadedEvent(ICommonSession session, ImmutableArray<(ResPath, byte[])> files)
    {
        Session = session;
        Files = files;
    }
}

public sealed class NetworkResourceManager : SharedNetworkResourceManager
{
    internal const int AckInitial = 1;

    [Dependency] private readonly IPlayerManager _playerManager = default!;
    [Dependency] private readonly IServerNetManager _serverNetManager = default!;
    [Dependency] private readonly IConfigurationManager _cfgManager = default!;
    [Dependency] private readonly IConGroupController _controller = default!;

    [Obsolete("Use ResourcesUploaded instead")]
    public event Action<ICommonSession, NetworkResourceUploadMessage>? OnResourceUploaded;
    public event Action<NetworkResourcesUploadedEvent>? ResourcesUploaded;

    [ViewVariables] public bool Enabled { get; private set; } = true;
    [ViewVariables] public float SizeLimit { get; private set; }

    internal event Action<INetChannel, int>? AckReceived;

    internal override void Initialize()
    {
        base.Initialize();

        TransferManager.RegisterTransferMessage(TransferKeyNetworkDownload);
        TransferManager.RegisterTransferMessage(TransferKeyNetworkUpload, transfer => _ = ReceiveUpload(transfer)); // DS14

        _cfgManager.OnValueChanged(CVars.ResourceUploadingEnabled, value => Enabled = value, true);
        _cfgManager.OnValueChanged(CVars.ResourceUploadingLimitMb, value => SizeLimit = value, true);

        _serverNetManager.RegisterNetMessage<NetworkResourceAckMessage>(RxAck);
    }

    private void RxAck(NetworkResourceAckMessage message)
    {
        AckReceived?.Invoke(message.MsgChannel, message.Key);
    }

    internal async Task ReceiveUpload(TransferReceivedEvent transfer) // DS14
    {
        // DS14-start: Upload errors must not escape an async-void callback and kill the server.
        try
        {
            await using var stream = transfer.DataStream;
            // DS14-end
            // Do not allow uploading any new resources if it has been disabled.
            // Note: Any resources uploaded before being disabled will still be kept and sent.
            if (!Enabled)
            {
                transfer.Channel.Disconnect("Resource upload not enabled.");
                return;
            }

            if (!_playerManager.TryGetSessionByChannel(transfer.Channel, out var session))
            {
                transfer.Channel.Disconnect("Not in-game");
                return;
            }

            if (!_controller.CanCommand(session, "uploadfile"))
            {
                transfer.Channel.Disconnect("Not authorized");
                return;
            }

            Sawmill.Verbose("Ingesting file uploads from {Session}", session);

            var ingested = await ReadUploadStream(stream); // DS14
            // DS14-start
            if (!transfer.Channel.IsConnected)
                return;
            // DS14-end

            foreach (var (relative, data) in ingested) StoreFile(relative, data); // DS14

            Sawmill.Verbose("Ingesting file uploads complete, distributing...");

            foreach (var channel in _serverNetManager.Channels)
            {
                _ = SendToPlayer(channel, ingested); // DS14
            }

#pragma warning disable CS0618 // Type or member is obsolete
            if (OnResourceUploaded != null)
            {
                foreach (var (relative, data) in ingested)
                {
                    OnResourceUploaded?.Invoke(session, new NetworkResourceUploadMessage
                    {
                        MsgChannel = session.Channel,
                        Data = data,
                        RelativePath = relative
                    });
                }
            }
#pragma warning restore CS0618 // Type or member is obsolete

            ResourcesUploaded?.Invoke(new NetworkResourcesUploadedEvent(session, [..ingested]));
        // DS14-start
        }
        catch (Exception e)
        {
            Sawmill.Warning($"Resource upload from {transfer.Channel} failed: {e}");
        }
        // DS14-end
    }

    protected override void ValidateUpload(uint size)
    {
        if (SizeLimit > 0f && size * BytesToMegabytes > SizeLimit)
            throw new Exception("File upload too large!");
    }

    // DS14-start: Validate both lengths on the server without changing the external client engine.
    private async Task<List<(ResPath Relative, byte[] Data)>> ReadUploadStream(Stream stream)
    {
        var files = new List<(ResPath, byte[])>();
        var header = new byte[8];
        var continuation = new byte[1];
        do
        {
            await stream.ReadExactlyAsync(header);
            var pathLength = BinaryPrimitives.ReadUInt32LittleEndian(header);
            var dataLength = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(4));
            if (pathLength is 0 or > 4096 || dataLength > Array.MaxLength)
                throw new InvalidDataException("Invalid uploaded resource size or path length.");
            ValidateUpload(dataLength);
            var path = new byte[pathLength];
            var data = new byte[dataLength];
            await stream.ReadExactlyAsync(path);
            await stream.ReadExactlyAsync(data);
            files.Add((new ResPath(Encoding.UTF8.GetString(path)), data));
            await stream.ReadExactlyAsync(continuation);
            if (continuation[0] > 1)
                throw new InvalidDataException("Invalid resource continuation flag.");
        } while (continuation[0] != 0);
        return files;
    }
    // DS14-end

    internal bool SendToNewUser(INetChannel channel)
    {
        var allFiles = ContentRoot.GetAllFiles().ToList();
        if (allFiles.Count == 0)
            return false;

        _ = SendToPlayer(channel, allFiles, AckInitial); // DS14
        return true;
    }

    internal async Task SendToPlayer(INetChannel channel, List<(ResPath Relative, byte[] Data)> files, int ack = 0) // DS14
    {
        // DS14-start
        try
        {
            // DS14-end
            await using var stream = TransferManager.StartTransfer(channel,
                new TransferStartInfo
                {
                    MessageKey = TransferKeyNetworkDownload
                });

            var ackBytes = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(ackBytes, ack);
            await stream.WriteAsync(ackBytes);

            await Task.Run(() => WriteFileStream(stream, files)); // DS14: Keep bulk copying and serialization off the game thread.
        // DS14-start
        }
        catch (Exception e)
        {
            Sawmill.Warning($"Resource download to {channel} failed: {e}");
            if (channel.IsConnected)
                channel.Disconnect("Failed to download server resources");
        }
        // DS14-end
    }
}
