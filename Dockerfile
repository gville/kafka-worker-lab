FROM golang:1.26 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/outbox-publisher ./cmd/outbox-publisher \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/outbox-publisher/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/outbox-publisher /usr/local/bin/outbox-publisher
COPY --from=build /out/healthcheck /usr/local/bin/healthcheck

USER nonroot:nonroot
EXPOSE 8081

ENTRYPOINT ["/usr/local/bin/outbox-publisher"]
