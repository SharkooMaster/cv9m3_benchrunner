FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build-env
WORKDIR /app

COPY benchrunner.csproj ./
RUN dotnet restore benchrunner.csproj

COPY . ./
RUN dotnet publish benchrunner.csproj -c Release -o out

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app

COPY --from=build-env /app/out .

EXPOSE 5050
ENTRYPOINT [ "dotnet", "benchrunner.dll" ]
