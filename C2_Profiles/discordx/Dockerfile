FROM itsafeaturemythic/mythic_python_dotnet@sha256:62a9295dc8803d6b9497f7a290540d41cab33a0f138526731c9d51e47b77ff33 AS runtime-base

FROM golang:1.26.0-bookworm@sha256:2a0ba12e116687098780d3ce700f9ce3cb340783779646aafbabed748fa6677c AS server-builder

WORKDIR /src
COPY ["go.mod", "go.sum", "./"]
RUN go mod download
COPY ["discordx/c2_code/go.mod", "discordx/c2_code/go.sum", "./discordx/c2_code/"]
RUN cd discordx/c2_code && go mod download
COPY [".", "."]
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/discordx-container . \
    && cd discordx/c2_code \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/discordx ./cmd/discordx-server

FROM runtime-base

WORKDIR /Mythic/
COPY [".", "."]
COPY --from=server-builder /out/discordx-container /usr/local/bin/discordx-container
COPY --from=server-builder /out/discordx /usr/local/bin/discordx-server
RUN chmod 0755 /usr/local/bin/discordx-container /usr/local/bin/discordx-server \
    && mkdir -p /var/lib/discordx \
    && chmod 0700 /var/lib/discordx
CMD ["/usr/local/bin/discordx-container"]
