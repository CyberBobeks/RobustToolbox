// Мёртвый Космос, Licensed under custom terms with restrictions on public hosting and commercial use, full text: https://raw.githubusercontent.com/dead-space-server/space-station-14-fobos/master/LICENSE.TXT
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Robust.Shared.Log;
using Robust.Shared.Asynchronous;
using Robust.Shared.Utility;
using Robust.Shared.Network;
using Robust.Shared.Network.Transfer;

namespace Robust.Server.Network.Transfer;

internal abstract class ServerTransferImpl(ISawmill sawmill, BaseTransferManager parent, INetChannel channel, ITaskManager tasks)
    : BaseTransferImpl(sawmill, parent, channel)
{
    // Server-local lifecycle with the existing upstream transfer framing.
    private readonly Dictionary<long, ChannelWriter<ArraySegment<byte>>> _receivingChannels = [];
    private readonly Lock _receivingLock = new();
    private bool _disposed;
    private int _sendFailed;

    private readonly SemaphoreSlim _socketSemaphore = new(1, 1);

    protected void ReceiveHeader(
        ReadOnlyMemory<byte> data,
        out TransferFlags flags,
        out long transferId,
        out ChannelWriter<ArraySegment<byte>> channel)
    {
        ParseHeader(data.Span, out flags, out transferId, out var key);
        ChannelReader<ArraySegment<byte>>? reader = null;
        lock (_receivingLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_receivingChannels.TryGetValue(transferId, out channel!))
            {
                if ((flags & TransferFlags.Start) != 0)
                    throw new ProtocolViolationException("Transfer already started");
                return;
            }

            if ((flags & TransferFlags.Start) == 0)
                throw new ProtocolViolationException($"Received data for unknown transfer {transferId}");
            if (_receivingChannels.Count >= MaxChannelCount)
                throw new ProtocolViolationException("Reached max transfer channel count");

            DebugTools.Assert(key != null);

            Sawmill.Verbose($"Starting transfer stream {transferId} with key {key}");

            var fullChannel = BoundedChannel
                ? System.Threading.Channels.Channel.CreateBounded<ArraySegment<byte>>(
                    new BoundedChannelOptions(4)
                    {
                        SingleReader = true,
                        SingleWriter = true
                    })
                : System.Threading.Channels.Channel.CreateUnbounded<ArraySegment<byte>>(new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = true
                });

            channel = fullChannel.Writer;
            _receivingChannels.Add(transferId, channel);
            reader = fullChannel.Reader;
        }
        Parent.TransferReceived(key!, Channel, new ReceiveStream(reader));
    }

    protected void FinishReceivedData(TransferFlags flags, long transferId, ChannelWriter<ArraySegment<byte>> channel)
    {
        if ((flags & TransferFlags.Finish) != 0)
        {
            Sawmill.Verbose($"Finishing transfer stream {transferId}");
            lock (_receivingLock)
            {
                channel.TryComplete();
                _receivingChannels.Remove(transferId);
            }
        }
    }

    private static void ParseHeader(
        ReadOnlySpan<byte> buf,
        out TransferFlags flags,
        out long transferId,
        out string? key)
    {
        if (buf.Length < 10 || buf[0] != (byte)Opcode.Transfer ||
            (buf[1] & ~(byte)(TransferFlags.Start | TransferFlags.Finish | TransferFlags.HasData)) != 0)
            throw new ProtocolViolationException("Invalid transfer header");
        flags = (TransferFlags)buf[1];
        transferId = BinaryPrimitives.ReadInt64LittleEndian(buf[2..10]);

        if ((flags & TransferFlags.Start) != 0)
        {
            if (buf.Length < 11 || buf[10] is 0 or > MaxKeySize || buf.Length != 11 + buf[10])
                throw new ProtocolViolationException("Invalid transfer key length");
            var keyLength = buf[10];
            key = Encoding.UTF8.GetString(buf.Slice(11, keyLength));
        }
        else
        {
            if (buf.Length != 10) throw new ProtocolViolationException("Invalid transfer header length");
            key = null;
        }
    }

    private sealed class ReceiveStream : SaneStream
    {
        private readonly ChannelReader<ArraySegment<byte>> _bufferChannel;
        private readonly SemaphoreSlim _readLock = new(1, 1);
        private readonly CancellationTokenSource _disposeCancel = new();
        private readonly CancellationToken _disposeToken;

        private ArraySegment<byte> _currentBuffer;
        private int _disposed;

        public override bool CanRead => Volatile.Read(ref _disposed) == 0;

        public ReceiveStream(ChannelReader<ArraySegment<byte>> bufferChannel)
        {
            _bufferChannel = bufferChannel;
            _disposeToken = _disposeCancel.Token;
        }

        public override int Read(Span<byte> buffer)
        {
            ObjectDisposedException.ThrowIf(!CanRead, this);
            _readLock.Wait(_disposeToken);
            try
            {
                ObjectDisposedException.ThrowIf(!CanRead, this);
                var read = 0;
                var remainingSpan = buffer;

                while (remainingSpan.Length > 0)
                {
                    if (_currentBuffer.Array == null || _currentBuffer.Count <= 0)
                    {
                        if (_currentBuffer.Array != null)
                        {
                            ArrayPool<byte>.Shared.Return(_currentBuffer.Array);
                            _currentBuffer = default;
                        }

                        if (!_bufferChannel.TryRead(out _currentBuffer))
                        {
                            // Only block if we haven't read any bytes yet.
                            if (read > 0 || !ReadNewBufferSync())
                                return read;
                        }
                    }

                    DebugTools.Assert(_currentBuffer.Array != null);

                    var remainingBuffer = _currentBuffer.Count;
                    var thisRead = Math.Min(remainingSpan.Length, remainingBuffer);

                    _currentBuffer.AsSpan(0, thisRead).CopyTo(remainingSpan);
                    remainingSpan = remainingSpan[thisRead..];
                    _currentBuffer = _currentBuffer[thisRead..];
                    read += thisRead;
                }

                return read;
            }
            finally
            {
                _readLock.Release();
            }
        }

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(!CanRead, this);
            using var linked = cancellationToken.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeToken)
                : null;
            var cancel = linked?.Token ?? _disposeToken;
            await _readLock.WaitAsync(cancel).ConfigureAwait(false);
            try
            {
                ObjectDisposedException.ThrowIf(!CanRead, this);
                var read = 0;
                var remainingSpan = buffer;

                while (remainingSpan.Length > 0)
                {
                    if (_currentBuffer.Array == null || _currentBuffer.Count <= 0)
                    {
                        if (_currentBuffer.Array != null)
                        {
                            ArrayPool<byte>.Shared.Return(_currentBuffer.Array);
                            _currentBuffer = default;
                        }

                        if (!_bufferChannel.TryRead(out _currentBuffer))
                        {
                            // Only block if we haven't read any bytes yet.
                            if (read > 0 || !await ReadNewBufferAsync(cancel).ConfigureAwait(false))
                                return read;
                        }
                    }

                    DebugTools.Assert(_currentBuffer.Array != null);

                    var remainingBuffer = _currentBuffer.Count;
                    var thisRead = Math.Min(remainingSpan.Length, remainingBuffer);

                    _currentBuffer.AsMemory(0, thisRead).CopyTo(remainingSpan);
                    remainingSpan = remainingSpan[thisRead..];
                    _currentBuffer = _currentBuffer[thisRead..];
                    read += thisRead;
                }

                return read;
            }
            finally
            {
                _readLock.Release();
            }
        }

        private bool ReadNewBufferSync()
        {
            DebugTools.Assert(_currentBuffer.Array == null);

            var waitToRead = _bufferChannel.WaitToReadAsync(_disposeToken);
#pragma warning disable RA0004
            var waitToReadResult = waitToRead.AsTask().Result;
#pragma warning restore RA0004
            if (!waitToReadResult)
                return false;

            return _bufferChannel.TryRead(out _currentBuffer);
        }

        private async Task<bool> ReadNewBufferAsync(CancellationToken cancel)
        {
            DebugTools.Assert(_currentBuffer.Array == null);

            var waitToRead = await _bufferChannel.WaitToReadAsync(cancel).ConfigureAwait(false);
            if (!waitToRead)
                return false;

            return _bufferChannel.TryRead(out _currentBuffer);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (!disposing || Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            _disposeCancel.Cancel();
            // The drain becomes the only reader after any cancelled read has relinquished its buffers.
            _ = DrainAsync();
        }
        private async Task DrainAsync()
        {
            await _readLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_currentBuffer.Array != null)
                    ArrayPool<byte>.Shared.Return(_currentBuffer.Array);
                _currentBuffer = default;
                while (await _bufferChannel.WaitToReadAsync().ConfigureAwait(false))
                {
                    while (_bufferChannel.TryRead(out var buffer))
                        ArrayPool<byte>.Shared.Return(buffer.Array!);
                }
            }
            catch (Exception)
            {
                // Connection shutdown can complete the queue with an error.
            }
            finally
            {
                _readLock.Release();
                _disposeCancel.Dispose();
            }
        }
    }

    protected abstract class ServerSendStream : SaneStream
    {
        protected readonly ServerTransferImpl Parent;
        private readonly long _id;
        private readonly string _key;

        private readonly byte[] _headerBuffer;
        private readonly byte[] _dataBuffer;
        private bool _isFirstTransmission = true;
        private int _bufferPos;
        private bool _disposed;

        public override bool CanWrite => !_disposed && !Parent.SendUnavailable;

        public ServerSendStream(ServerTransferImpl parent, long id, string key)
        {
            // This just has to be < buffer size & < ushort.MaxValue
            // (when accounting for UTF-8 possibly being more code units than UTF-16)
            if (Encoding.UTF8.GetByteCount(key) > MaxKeySize)
                throw new ArgumentException("Key too long");

            Parent = parent;
            _id = id;
            _key = key;

            _headerBuffer = ArrayPool<byte>.Shared.Rent(MaxHeaderSize);
            _dataBuffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            throw new NotSupportedException("Transfer streams require WriteAsync.");
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(buffer);
            return WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
        }

        public override async ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            Parent.CheckSendState();
            cancellationToken.ThrowIfCancellationRequested();
            while (buffer.Length > 0)
            {
                var remainingBufferSpace = _dataBuffer.AsSpan(_bufferPos);
                var thisChunk = Math.Min(remainingBufferSpace.Length, buffer.Length);
                var thisSpan = buffer[..thisChunk];

                thisSpan.Span.CopyTo(remainingBufferSpace);
                _bufferPos += thisChunk;

                if (_bufferPos == _dataBuffer.Length)
                    await FlushAsync(cancellationToken).ConfigureAwait(false);

                buffer = buffer[thisChunk..];
            }
        }

        public override void Flush()
        {
            throw new NotSupportedException("Transfer streams require FlushAsync.");
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            await FlushAsync(finish: false, cancellationToken).ConfigureAwait(false);
        }

        private async ValueTask FlushAsync(bool finish, CancellationToken cancel = default)
        {
            Parent.CheckSendState();
            var headerLength = 10;

            var opcode = Opcode.Transfer;
            var flags = TransferFlags.None;
            if (_isFirstTransmission)
                flags |= TransferFlags.Start;
            if (_bufferPos > 0)
                flags |= TransferFlags.HasData;
            if (finish)
                flags |= TransferFlags.Finish;

            if (flags == TransferFlags.None)
            {
                // Nothing to flush, whatsoever.
                return;
            }

            _headerBuffer[0] = (byte)opcode;
            _headerBuffer[1] = (byte)flags;
            BinaryPrimitives.WriteInt64LittleEndian(_headerBuffer.AsSpan(2..10), _id);

            if (_isFirstTransmission)
            {
                var written = Encoding.UTF8.GetBytes(_key, _headerBuffer.AsSpan(11..));
                DebugTools.Assert(written < byte.MaxValue);
                _headerBuffer[10] = (byte)written;

                headerLength += 1;
                headerLength += written;
            }

            // Send.
            await Parent._socketSemaphore.WaitAsync(cancel).ConfigureAwait(false);
            try
            {
                Parent.CheckSendState();
                await SendChunkAsync(
                        new ArraySegment<byte>(_headerBuffer, 0, headerLength),
                        cancel)
                    .ConfigureAwait(false);
                _isFirstTransmission = false;
                Parent.CheckSendState();

                if (_bufferPos > 0)
                {
                    await SendChunkAsync(
                            new ArraySegment<byte>(_dataBuffer, 0, _bufferPos),
                            cancel)
                        .ConfigureAwait(false);

                    _bufferPos = 0;
                }
            }
            catch
            {
                // Once a header may have escaped, retrying would turn the next header into its payload.
                Parent.FailSending();
                throw;
            }
            finally
            {
                Parent._socketSemaphore.Release();
            }

        }

        protected abstract ValueTask SendChunkAsync(
            ArraySegment<byte> buffer,
            CancellationToken cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
                return;
            _disposed = true;
            try
            {
                // Synchronous cleanup cancels an open transfer; only DisposeAsync sends its Finish frame.
                if (disposing && !_isFirstTransmission && !Parent.SendUnavailable)
                    Parent.FailSending();
            }
            finally
            {
                DisposeCore();
                GC.SuppressFinalize(this);
            }
        }

        public override async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;
            _disposed = true;
            try
            {
                if (!Parent.SendUnavailable)
                    await FlushAsync(finish: true).ConfigureAwait(false);
            }
            finally
            {
                DisposeCore();
                GC.SuppressFinalize(this);
            }
        }

        private void DisposeCore()
        {
            if (_dataBuffer != null) ArrayPool<byte>.Shared.Return(_dataBuffer);
            if (_headerBuffer != null) ArrayPool<byte>.Shared.Return(_headerBuffer);
        }

        ~ServerSendStream()
        {
            // Finalizers only return buffers; a socket error here would terminate the process.
            Dispose(false);
        }
    }

    public override void Dispose()
    {
        lock (_receivingLock)
        {
            if (_disposed)
                return;
            _disposed = true;
            foreach (var channel in _receivingChannels.Values)
                channel.TryComplete();
            _receivingChannels.Clear();
        }
    }

    protected void DisconnectChannel(string reason)
    {
        tasks.RunOnMainThread(() =>
        {
            if (Channel.IsConnected)
                Channel.Disconnect(reason);
        });
    }

    private bool SendUnavailable => Volatile.Read(ref _sendFailed) != 0 || Volatile.Read(ref _disposed);

    private void CheckSendState()
    {
        if (SendUnavailable)
            throw new IOException("The transfer connection is no longer usable.");
    }

    private void FailSending()
    {
        if (Interlocked.Exchange(ref _sendFailed, 1) == 0)
            DisconnectChannel("Error in transfer socket");
    }
}
