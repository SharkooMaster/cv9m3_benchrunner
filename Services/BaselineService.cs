using System.Diagnostics;
using System.Threading.Channels;
using Benchrunner;
using Grpc.Core;
using ZstdSharp;

namespace Benchrunner.Services;

public class BaselineService : Benchrunner.BaselineService.BaselineServiceBase
{
    private const int DefaultBlockSizeMB = 16;
    private static readonly int MaxBlockParallelism = Math.Max(2, Environment.ProcessorCount / 2);

    public override async Task<BaselineResponse> RunBaselines(
        IAsyncStreamReader<BaselineRequest> requestStream, ServerCallContext context)
    {
        var ct = context.CancellationToken;
        string fileName = "";
        int blockSizeBytes = DefaultBlockSizeMB * 1024 * 1024;

        // Read metadata first
        if (await requestStream.MoveNext(ct))
        {
            var first = requestStream.Current;
            if (first.PayloadCase == BaselineRequest.PayloadOneofCase.Metadata)
            {
                fileName = first.Metadata.FileName;
                if (first.Metadata.BlockSizeMb > 0)
                    blockSizeBytes = (int)first.Metadata.BlockSizeMb * 1024 * 1024;
            }
        }

        Console.WriteLine($"[BenchRunner] RunBaselines: streaming {fileName} " +
            $"(blockSize={blockSizeBytes / (1024 * 1024)}MB, parallelism={MaxBlockParallelism})...");

        var totalSw = Stopwatch.StartNew();

        // Bounded channel: block producer (receive loop) → block consumers (compressors)
        var blockChannel = Channel.CreateBounded<byte[]>(
            new BoundedChannelOptions(MaxBlockParallelism + 2)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true
            });

        // Accumulate results from parallel workers
        long totalZstd3 = 0, totalZstd9 = 0, totalZstd19 = 0, totalLz4 = 0;
        long totalZstd3Ticks = 0, totalZstd9Ticks = 0, totalZstd19Ticks = 0, totalLz4Ticks = 0;
        long receivedBytes = 0;
        int blocksDone = 0;

        // Worker tasks: each pulls a block, compresses at all 4 levels in parallel
        var workers = new Task[MaxBlockParallelism];
        for (int w = 0; w < MaxBlockParallelism; w++)
        {
            workers[w] = Task.Run(async () =>
            {
                while (await blockChannel.Reader.WaitToReadAsync(ct))
                {
                    while (blockChannel.Reader.TryRead(out var block))
                    {
                        // Compress this block at all 4 levels in parallel
                        long z3 = 0, z9 = 0, z19 = 0, l4 = 0;
                        long t3 = 0, t9 = 0, t19 = 0, t4 = 0;

                        var tasks = new Task[4];
                        tasks[0] = Task.Run(() =>
                        {
                            var sw = Stopwatch.StartNew();
                            using var c = new CountingStream();
                            using (var cs = new CompressionStream(c, 3)) cs.Write(block);
                            sw.Stop();
                            z3 = c.BytesWritten;
                            t3 = sw.ElapsedTicks;
                        }, ct);

                        tasks[1] = Task.Run(() =>
                        {
                            var sw = Stopwatch.StartNew();
                            using var c = new CountingStream();
                            using (var cs = new CompressionStream(c, 9)) cs.Write(block);
                            sw.Stop();
                            z9 = c.BytesWritten;
                            t9 = sw.ElapsedTicks;
                        }, ct);

                        tasks[2] = Task.Run(() =>
                        {
                            var sw = Stopwatch.StartNew();
                            using var c = new CountingStream();
                            using (var cs = new CompressionStream(c, 19)) cs.Write(block);
                            sw.Stop();
                            z19 = c.BytesWritten;
                            t19 = sw.ElapsedTicks;
                        }, ct);

                        tasks[3] = Task.Run(() =>
                        {
                            var sw = Stopwatch.StartNew();
                            using var c = new CountingStream();
                            using (var cs = new CompressionStream(c, 1)) cs.Write(block);
                            sw.Stop();
                            l4 = c.BytesWritten;
                            t4 = sw.ElapsedTicks;
                        }, ct);

                        await Task.WhenAll(tasks);

                        Interlocked.Add(ref totalZstd3, z3);
                        Interlocked.Add(ref totalZstd9, z9);
                        Interlocked.Add(ref totalZstd19, z19);
                        Interlocked.Add(ref totalLz4, l4);
                        Interlocked.Add(ref totalZstd3Ticks, t3);
                        Interlocked.Add(ref totalZstd9Ticks, t9);
                        Interlocked.Add(ref totalZstd19Ticks, t19);
                        Interlocked.Add(ref totalLz4Ticks, t4);

                        int done = Interlocked.Increment(ref blocksDone);
                        if (done % 10 == 0 || done == 1)
                            Console.WriteLine($"[BenchRunner] {fileName}: block {done} done ({block.Length / (1024 * 1024)}MB)");
                    }
                }
            }, ct);
        }

        // Receive loop: accumulate gRPC chunks into block-sized buffers, push to channel
        byte[] currentBuf = new byte[blockSizeBytes];
        int bufOffset = 0;

        while (await requestStream.MoveNext(ct))
        {
            var req = requestStream.Current;
            if (req.PayloadCase != BaselineRequest.PayloadOneofCase.Chunk) continue;

            var data = req.Chunk.Memory;
            receivedBytes += data.Length;
            int srcOffset = 0;

            while (srcOffset < data.Length)
            {
                int toCopy = Math.Min(data.Length - srcOffset, blockSizeBytes - bufOffset);
                data.Span.Slice(srcOffset, toCopy).CopyTo(currentBuf.AsSpan(bufOffset));
                bufOffset += toCopy;
                srcOffset += toCopy;

                if (bufOffset == blockSizeBytes)
                {
                    await blockChannel.Writer.WriteAsync(currentBuf, ct);
                    currentBuf = new byte[blockSizeBytes];
                    bufOffset = 0;
                }
            }
        }

        // Push last partial block
        if (bufOffset > 0)
        {
            byte[] lastBlock = new byte[bufOffset];
            Buffer.BlockCopy(currentBuf, 0, lastBlock, 0, bufOffset);
            await blockChannel.Writer.WriteAsync(lastBlock, ct);
        }

        blockChannel.Writer.Complete();
        await Task.WhenAll(workers);
        totalSw.Stop();

        double tickFreq = Stopwatch.Frequency;
        Console.WriteLine($"[BenchRunner] Done: {fileName} ({receivedBytes / (1024.0 * 1024):F1} MB, {blocksDone} blocks) " +
            $"zstd3={totalZstd3} zstd9={totalZstd9} zstd19={totalZstd19} lz4={totalLz4} " +
            $"in {totalSw.Elapsed.TotalSeconds:F1}s");

        return new BaselineResponse
        {
            Zstd3Bytes = totalZstd3,
            Zstd9Bytes = totalZstd9,
            Zstd19Bytes = totalZstd19,
            Lz4Bytes = totalLz4,
            Zstd3TimeMs = totalZstd3Ticks * 1000.0 / tickFreq,
            Zstd9TimeMs = totalZstd9Ticks * 1000.0 / tickFreq,
            Zstd19TimeMs = totalZstd19Ticks * 1000.0 / tickFreq,
            Lz4TimeMs = totalLz4Ticks * 1000.0 / tickFreq,
            TotalTimeMs = totalSw.Elapsed.TotalMilliseconds,
        };
    }

    public override Task<ResetResponse> ResetState(ResetRequest request, ServerCallContext context)
    {
        Console.WriteLine($"[BenchRunner] ResetState: no-op (dedup runs client-side)");
        return Task.FromResult(new ResetResponse { Success = true });
    }

    public override Task<SystemInfoResponse> GetSystemInfo(SystemInfoRequest request, ServerCallContext context)
    {
        string cpuModel = "Unknown";
        int cpuCores = Environment.ProcessorCount;
        long totalMemory = 0;

        try
        {
            if (File.Exists("/proc/cpuinfo"))
            {
                foreach (var line in File.ReadLines("/proc/cpuinfo"))
                {
                    if (line.StartsWith("model name", StringComparison.OrdinalIgnoreCase))
                    {
                        int idx = line.IndexOf(':');
                        if (idx >= 0)
                        {
                            cpuModel = line[(idx + 1)..].Trim();
                            break;
                        }
                    }
                }
            }
        }
        catch { }

        try
        {
            if (File.Exists("/proc/meminfo"))
            {
                foreach (var line in File.ReadLines("/proc/meminfo"))
                {
                    if (line.StartsWith("MemTotal:", StringComparison.OrdinalIgnoreCase))
                    {
                        var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length >= 2 && long.TryParse(parts[1], out var kb))
                            totalMemory = kb * 1024;
                        break;
                    }
                }
            }
        }
        catch { }

        var resp = new SystemInfoResponse
        {
            CpuModel = cpuModel,
            CpuCores = cpuCores,
            TotalMemoryBytes = totalMemory,
            Hostname = Environment.MachineName,
            OsInfo = $"{System.Runtime.InteropServices.RuntimeInformation.OSDescription} " +
                     $"({System.Runtime.InteropServices.RuntimeInformation.OSArchitecture})"
        };

        Console.WriteLine($"[BenchRunner] GetSystemInfo: {cpuModel}, {cpuCores} cores, " +
            $"{totalMemory / (1024.0 * 1024 * 1024):F1} GB RAM");

        return Task.FromResult(resp);
    }
}

/// <summary>
/// Stream that counts bytes written and discards the data.
/// </summary>
internal sealed class CountingStream : Stream
{
    public long BytesWritten { get; private set; }
    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => BytesWritten;
    public override long Position
    {
        get => BytesWritten;
        set => throw new NotSupportedException();
    }
    public override void Write(byte[] buffer, int offset, int count) => BytesWritten += count;
    public override void Write(ReadOnlySpan<byte> buffer) => BytesWritten += buffer.Length;
    public override void Flush() { }
    public override Task FlushAsync(CancellationToken ct) => Task.CompletedTask;
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}
