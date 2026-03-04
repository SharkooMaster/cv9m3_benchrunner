using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using Benchrunner;
using Grpc.Core;
using ZstdSharp;

namespace Benchrunner.Services;

public class BaselineService : Benchrunner.BaselineService.BaselineServiceBase
{
    private readonly ConcurrentDictionary<string, byte> _globalChunkHashes = new();

    // Serialize file processing so concurrent calls don't OOM the pod.
    // Each file needs ~3x its size in peak RAM (fileData + zstd output + dedup buffers).
    private readonly SemaphoreSlim _concurrencyGate = new(1, 1);

    private static readonly long MaxFileSizeBytes =
        long.TryParse(Environment.GetEnvironmentVariable("MAX_FILE_SIZE_GB"), out var gb) && gb > 0
            ? gb * 1024L * 1024 * 1024
            : 4L * 1024 * 1024 * 1024;

    public override async Task<BaselineResponse> RunBaselines(
        IAsyncStreamReader<BaselineRequest> requestStream, ServerCallContext context)
    {
        string fileName = "";
        int dedupChunkSize = 1024;
        byte[] fileData;

        {
            var buffer = new MemoryStream();
            while (await requestStream.MoveNext(context.CancellationToken))
            {
                var req = requestStream.Current;
                switch (req.PayloadCase)
                {
                    case BaselineRequest.PayloadOneofCase.Metadata:
                        fileName = req.Metadata.FileName;
                        if (req.Metadata.DedupChunkSize > 0)
                            dedupChunkSize = (int)req.Metadata.DedupChunkSize;
                        break;

                    case BaselineRequest.PayloadOneofCase.Chunk:
                        if (buffer.Length + req.Chunk.Length > MaxFileSizeBytes)
                            throw new RpcException(new Status(StatusCode.ResourceExhausted,
                                $"File exceeds max size of {MaxFileSizeBytes / (1024 * 1024 * 1024)} GB"));
                        req.Chunk.WriteTo(buffer);
                        break;
                }
            }
            fileData = buffer.ToArray();
            buffer.Dispose();
        }

        Console.WriteLine($"[BenchRunner] RunBaselines: {fileName} ({fileData.Length / (1024.0 * 1024):F1} MB), waiting for slot...");

        await _concurrencyGate.WaitAsync(context.CancellationToken);
        try
        {
            return ProcessFile(fileName, fileData, dedupChunkSize);
        }
        finally
        {
            _concurrencyGate.Release();
        }
    }

    private BaselineResponse ProcessFile(string fileName, byte[] fileData, int dedupChunkSize)
    {
        var totalSw = Stopwatch.StartNew();

        // Pre-allocate a shared output buffer to avoid per-compressor heap allocations.
        // ZstdSharp's Wrap returns a new byte[], so we measure and let GC reclaim between levels.
        var sw3 = Stopwatch.StartNew();
        long zstd3;
        using (var c3 = new Compressor(3))
            zstd3 = c3.Wrap(fileData).Length;
        sw3.Stop();

        var sw9 = Stopwatch.StartNew();
        long zstd9;
        using (var c9 = new Compressor(9))
            zstd9 = c9.Wrap(fileData).Length;
        sw9.Stop();

        var sw19 = Stopwatch.StartNew();
        long zstd19;
        using (var c19 = new Compressor(19))
            zstd19 = c19.Wrap(fileData).Length;
        sw19.Stop();

        var swLz4 = Stopwatch.StartNew();
        long lz4;
        using (var cLz4 = new Compressor(1))
            lz4 = cLz4.Wrap(fileData).Length;
        swLz4.Stop();

        // Nudge GC to reclaim the 4 compressed output arrays before dedup allocates more
        GC.Collect(0, GCCollectionMode.Optimized, blocking: false);

        // Exact dedup + zstd
        var swDedup = Stopwatch.StartNew();
        int cs = dedupChunkSize;
        int totalChunks = (fileData.Length + cs - 1) / cs;
        var localUnique = new HashSet<string>();
        int dedupUnique = 0;
        long dedupBytes = 0;

        using var dedupOutput = new MemoryStream();
        using (var zstdStream = new CompressionStream(dedupOutput, 3, leaveOpen: true))
        {
            for (int i = 0; i < totalChunks; i++)
            {
                int offset = i * cs;
                int len = Math.Min(cs, fileData.Length - offset);
                string hash = Convert.ToHexString(SHA256.HashData(fileData.AsSpan(offset, len)));

                bool isGlobalNew = _globalChunkHashes.TryAdd(hash, 0);
                if (localUnique.Add(hash) && isGlobalNew)
                {
                    zstdStream.Write(fileData, offset, len);
                    dedupUnique++;
                    dedupBytes += len;
                }
            }
        }
        long dedupZstd = dedupOutput.Length;
        swDedup.Stop();

        totalSw.Stop();

        Console.WriteLine($"[BenchRunner] Done: {fileName} " +
            $"zstd3={zstd3} zstd9={zstd9} zstd19={zstd19} lz4={lz4} " +
            $"dedup={dedupUnique}/{totalChunks} dedupZstd={dedupZstd} " +
            $"in {totalSw.Elapsed.TotalSeconds:F1}s");

        return new BaselineResponse
        {
            Zstd3Bytes = zstd3,
            Zstd9Bytes = zstd9,
            Zstd19Bytes = zstd19,
            Lz4Bytes = lz4,
            Zstd3TimeMs = sw3.Elapsed.TotalMilliseconds,
            Zstd9TimeMs = sw9.Elapsed.TotalMilliseconds,
            Zstd19TimeMs = sw19.Elapsed.TotalMilliseconds,
            Lz4TimeMs = swLz4.Elapsed.TotalMilliseconds,
            DedupBytes = dedupBytes,
            DedupUnique = dedupUnique,
            DedupTotal = totalChunks,
            DedupZstdBytes = dedupZstd,
            DedupTimeMs = swDedup.Elapsed.TotalMilliseconds,
            TotalTimeMs = totalSw.Elapsed.TotalMilliseconds,
        };
    }

    public override Task<ResetResponse> ResetState(ResetRequest request, ServerCallContext context)
    {
        int cleared = _globalChunkHashes.Count;
        _globalChunkHashes.Clear();
        Console.WriteLine($"[BenchRunner] ResetState: cleared {cleared} dedup hashes");
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
            // /proc/meminfo shows host RAM even inside containers — this is what we want
            // for the report (shows the actual hardware the competitors ran on).
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
