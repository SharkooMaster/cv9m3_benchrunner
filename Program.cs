using Benchrunner.Services;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(o =>
{
    o.ListenAnyIP(5050, lo => lo.Protocols = HttpProtocols.Http2);
});

builder.Services.AddGrpc(o =>
{
    o.MaxReceiveMessageSize = 8 * 1024 * 1024;
    o.MaxSendMessageSize = 8 * 1024 * 1024;
});

builder.Services.AddSingleton<BaselineService>();

var app = builder.Build();

app.MapGrpcService<BaselineService>();

Console.WriteLine("[BenchRunner] Listening on port 5050 (gRPC/HTTP2)");
app.Run();
