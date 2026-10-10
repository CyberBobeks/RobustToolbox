using System;
using System.Buffers;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Channels;
using Robust.Shared.Asynchronous;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Robust.Server.ServerStatus;
using Robust.Shared;
using Robust.Shared.Configuration;
using Robust.Shared.Log;
using Robust.Shared.Network;
using Robust.Shared.Network.Messages.Transfer;
using Robust.Shared.Network.Transfer;
using Robust.Shared.Utility;

namespace Robust.Server.Network.Transfer;

internal sealed class ServerTransferImplWebSocket : ServerTransferImpl // DS14
{
    // DS14-start
    internal const string KeyHeaderName = "RT-Key";
    internal const string UserIdHeaderName = "RT-UserId";
    internal const int RandomKeyBytes = 32;
    private readonly CancellationTokenSource _readCancel = new();
    private readonly CancellationToken _readToken;
    private int _disposed;
    public WebSocket? WebSocket;
    protected override bool BoundedChannel => true;
    // DS14-end

    private readonly IConfigurationManager _cfg;
    private readonly INetManager _netManager;
    private readonly SemaphoreSlim _apiLock = new(1, 1);
    private readonly TaskCompletionSource _connectTcs = new();

    // To authenticate the client doing the HTTP request,
    // we ask that they provide a key we gave them via Lidgren traffic.
    public byte[]? Key;

    public ServerTransferImplWebSocket(
        ISawmill sawmill,
        BaseTransferManager parent,
        IConfigurationManager cfg,
        INetManager netManager,
        INetChannel channel,
        ITaskManager tasks) // DS14
        : base(sawmill, parent, channel, tasks) // DS14
    {
        _readToken = _readCancel.Token; // DS14
        _cfg = cfg;
        _netManager = netManager;
    }

    public override Task ServerInit()
    {
        Key = RandomNumberGenerator.GetBytes(RandomKeyBytes);

        var uriBuilder = new UriBuilder(string.Concat(
            _cfg.GetCVar(CVars.TransferHttpEndpoint).TrimEnd("/"),
            ServerTransferManager.TransferApiUrl));

        uriBuilder.Scheme = uriBuilder.Scheme switch
        {
            "http" => "ws",
            "https" => "wss",
            _ => throw new InvalidOperationException($"Invalid API endpoint scheme: {uriBuilder.Scheme}")
        };

        var url = uriBuilder.ToString();

        Sawmill.Verbose($"Transfer API URL is '{url}'");

        var initMsg = new MsgTransferInit();
        initMsg.HttpInfo = (url, Key);

        _netManager.ServerSendMessage(initMsg, Channel);

        return _connectTcs.Task;
    }

    public override Task ClientInit(CancellationToken cancel)
    {
        throw new NotSupportedException();
    }

    public async Task HandleApiRequest(NetUserId userId, IStatusHandlerContext context)
    {
        using var apiGuard = await _apiLock.WaitGuardAsync(); // DS14

        if (Key == null)
        {
            Sawmill.Warning($"HTTP request failed: UserID '{userId}' tried to connect twice");
            await context.RespondErrorAsync(HttpStatusCode.BadRequest);
            return;
        }

        if (!context.RequestHeaders.TryGetValue(KeyHeaderName, out var keyValue) || keyValue is not [{ } keyValueStr])
        {
            await context.RespondErrorAsync(HttpStatusCode.BadRequest);
            return;
        }

        var buf = new byte[RandomKeyBytes];

        if (!Convert.TryFromBase64String(keyValueStr, buf, out var written) || written != RandomKeyBytes)
        {
            Sawmill.Verbose("HTTP request failed: key is not valid base64 or wrong length");
            await context.RespondErrorAsync(HttpStatusCode.BadRequest);
            return;
        }

        if (!CryptographicOperations.FixedTimeEquals(buf, Key))
        {
            Sawmill.Warning("HTTP request failed: key is wrong");
            await context.RespondErrorAsync(HttpStatusCode.Unauthorized);
            return;
        }

        Sawmill.Debug("Client connect to transfer WS channel: {UserId}", userId);

        WebSocket = await context.AcceptWebSocketAsync();
        // DS14-start
        if (Volatile.Read(ref _disposed) != 0 || !Channel.IsConnected)
        {
            WebSocket.Dispose();
            _connectTcs.TrySetCanceled();
            return;
        }
        // DS14-end

        // We've connected.
        // Clear key so this can't be reconnected to.
        Key = null;

        _connectTcs.TrySetResult();

        _ = ReadThread(); // DS14
    }

    // DS14-start
    public override Stream StartTransfer(TransferStartInfo startInfo)
    {
        if (WebSocket == null)
            throw new InvalidOperationException("Player not connected yet");
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        var id = Interlocked.Increment(ref OutgoingIdCounter);

        return new SendStream(this, id, startInfo.MessageKey);
    }

    internal async Task ReadThread()
    {
        if (Volatile.Read(ref _disposed) != 0) return;
        DebugTools.Assert(WebSocket != null);
        var ws = WebSocket;
        var headerBuffer = ArrayPool<byte>.Shared.Rent(MaxHeaderSize);
        var cancel = _readToken;

        try
        {
            while (!cancel.IsCancellationRequested)
            {
                // A single WebSocket message may require multiple ReceiveAsync calls.
                var headerLength = 0;
                ValueWebSocketReceiveResult receiveResult;
                do
                {
                    if (headerLength == MaxHeaderSize)
                        throw new ProtocolViolationException("Transfer header too large");
                    receiveResult = await ws!.ReceiveAsync(
                        headerBuffer.AsMemory(headerLength, MaxHeaderSize - headerLength), cancel).ConfigureAwait(false);
                    BaseTransferManager.ReceivedDataMetrics.Inc(receiveResult.Count);
                    if (receiveResult.MessageType != WebSocketMessageType.Binary)
                        throw new ProtocolViolationException("Data must be binary!");
                    headerLength += receiveResult.Count;
                } while (!receiveResult.EndOfMessage);
                var receivedData = headerBuffer.AsMemory(0, headerLength);
                ReceiveHeader(receivedData, out var flags, out var transferId, out var channel);

                if ((flags & TransferFlags.HasData) != 0)
                    await ReceiveTransferData(ws!, channel, cancel).ConfigureAwait(false);

                FinishReceivedData(flags, transferId, channel);
            }
        }
        // Local disconnect/shutdown is normal, not a second network error.
        catch (Exception) when (cancel.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            Sawmill.Error($"Error reading transfer socket: {e}");
            DisconnectChannel("Error in transfer socket");
        }
        finally
        {
            base.Dispose();
            ArrayPool<byte>.Shared.Return(headerBuffer);
        }
    }

    private sealed class SendStream : ServerSendStream
    {
        public SendStream(ServerTransferImplWebSocket parent, long id, string key) : base(parent, id, key)
        {
        }

        protected override async ValueTask SendChunkAsync(ArraySegment<byte> buffer, CancellationToken cancel)
        {
            var ws = ((ServerTransferImplWebSocket)Parent).WebSocket!;

            BaseTransferManager.SentDataMetrics.Inc(buffer.Count);

            await ws.SendAsync(
                    buffer,
                    WebSocketMessageType.Binary,
                    endOfMessage: true,
                    cancel)
                .ConfigureAwait(false);
        }
    }

    private static async ValueTask ReceiveTransferData(
        WebSocket ws,
        ChannelWriter<ArraySegment<byte>> channel,
        CancellationToken cancel)
    {
        while (!cancel.IsCancellationRequested)
        {
            var buf = ArrayPool<byte>.Shared.Rent(BufferSize);
            // Return buffers also when receive or queueing fails.
            try
            {
                var result = await ws.ReceiveAsync(buf.AsMemory(0, BufferSize), cancel).ConfigureAwait(false);
                BaseTransferManager.ReceivedDataMetrics.Inc(result.Count);
                if (result.MessageType != WebSocketMessageType.Binary)
                    throw new ProtocolViolationException("Data must be binary!");
                await channel.WriteAsync(new ArraySegment<byte>(buf, 0, result.Count), cancel).ConfigureAwait(false);
                buf = null!; // Ownership passed to ReceiveStream.
                if (result.EndOfMessage)
                    break;
            }
            finally
            {
                if (buf != null)
                    ArrayPool<byte>.Shared.Return(buf);
            }
        }
    }

    // DS14-end

    public override void Dispose()
    {
        // DS14-start
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _connectTcs.TrySetCanceled();
        _readCancel.Cancel();
        base.Dispose();
        WebSocket?.Dispose();
        _readCancel.Dispose();
        // DS14-end
    }
}
