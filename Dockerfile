FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum VERSION ./
RUN go mod download && mkdir -p internal/version && cp VERSION internal/version/VERSION.txt
COPY cmd ./cmd
COPY internal ./internal
# /data must exist and be owned by nonroot so fresh named volumes inherit it;
# distroless nonroot cannot write to root-owned mount points.
RUN mkdir -p /data && chown 65534:65534 /data
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/dualroute-gateway ./cmd/gateway && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/edgetunnel-config ./cmd/edgetunnel-config && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/control-plane ./cmd/control-plane

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build --chown=65534:65534 /data /data
COPY --from=build /out/dualroute-gateway /dualroute-gateway
COPY --from=build /out/edgetunnel-config /edgetunnel-config
COPY --from=build /out/control-plane /control-plane
EXPOSE 13337 13338 13339
ENTRYPOINT ["/dualroute-gateway"]
