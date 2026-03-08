using System.Diagnostics;
using Benchrunner;
using Grpc.Core;
using ZstdSharp;

namespace Benchrunner.Services;

/// <summary>
/// Stream that counts bytes written and discards the data.
/// Used as a sink for CompressionStream when we only need the compressed size.
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
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct)
    {
        BytesWritten += count;
        return Task.CompletedTask;
    }
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
    {
        BytesWritten += buffer.Length;
        return ValueTask.CompletedTask;
    }
    public override void Flush() { }
    public override Task FlushAsync(CancellationToken ct) => Task.CompletedTask;
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
}

public class BaselineService : Benchrunner.BaselineService.BaselineServiceBase
{
    public override async Task<BaselineResponse> RunBaselines(
        IAsyncStreamReader<BaselineRequest> requestStream, ServerCallContext context)
    {
        string fileName = "";
        long receivedBytes = 0;
        var ct = context.CancellationToken;

        var count3 = new CountingStream();
        var count9 = new CountingStream();
        var count19 = new CountingStream();
        var countLz4 = new CountingStream();

        var zstd3 = new CompressionStream(count3, 3, leaveOpen: true);
        var zstd9 = new CompressionStream(count9, 9, leaveOpen: true);
        var zstd19 = new CompressionStream(count19, 19, leaveOpen: true);
        var lz4 = new CompressionStream(countLz4, 1, leaveOpen: true);

        var sw3 = new Stopwatch();
        var sw9 = new Stopwatch();
        var sw19 = new Stopwatch();
        var swLz4 = new Stopwatch();
        var totalSw = Stopwatch.StartNew();

        try
        {
            while (await requestStream.MoveNext(ct))
            {
                var req = requestStream.Current;
                switch (req.PayloadCase)
                {
                    case BaselineRequest.PayloadOneofCase.Metadata:
                        fileName = req.Metadata.FileName;
                        Console.WriteLine($"[BenchRunner] RunBaselines: streaming {fileName}...");
                        break;

                    case BaselineRequest.PayloadOneofCase.Chunk:
                        var data = req.Chunk.Memory;
                        receivedBytes += data.Length;

                        sw3.Start();
                        zstd3.Write(data.Span);
                        sw3.Stop();

                        sw9.Start();
                        zstd9.Write(data.Span);
                        sw9.Stop();

                        sw19.Start();
                        zstd19.Write(data.Span);
                        sw19.Stop();

                        swLz4.Start();
                        lz4.Write(data.Span);
                        swLz4.Stop();
                        break;
                }
            }

            sw3.Start(); zstd3.Flush(); zstd3.Dispose(); sw3.Stop();
            sw9.Start(); zstd9.Flush(); zstd9.Dispose(); sw9.Stop();
            sw19.Start(); zstd19.Flush(); zstd19.Dispose(); sw19.Stop();
            swLz4.Start(); lz4.Flush(); lz4.Dispose(); swLz4.Stop();
        }
        finally
        {
            zstd3.Dispose();
            zstd9.Dispose();
            zstd19.Dispose();
            lz4.Dispose();
            count3.Dispose();
            count9.Dispose();
            count19.Dispose();
            countLz4.Dispose();
        }

        totalSw.Stop();

        Console.WriteLine($"[BenchRunner] Done: {fileName} ({receivedBytes / (1024.0 * 1024):F1} MB) " +
            $"zstd3={count3.BytesWritten} zstd9={count9.BytesWritten} " +
            $"zstd19={count19.BytesWritten} lz4={countLz4.BytesWritten} " +
            $"in {totalSw.Elapsed.TotalSeconds:F1}s");

        return new BaselineResponse
        {
            Zstd3Bytes = count3.BytesWritten,
            Zstd9Bytes = count9.BytesWritten,
            Zstd19Bytes = count19.BytesWritten,
            Lz4Bytes = countLz4.BytesWritten,
            Zstd3TimeMs = sw3.Elapsed.TotalMilliseconds,
            Zstd9TimeMs = sw9.Elapsed.TotalMilliseconds,
            Zstd19TimeMs = sw19.Elapsed.TotalMilliseconds,
            Lz4TimeMs = swLz4.Elapsed.TotalMilliseconds,
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
