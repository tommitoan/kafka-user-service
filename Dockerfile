# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM alpine:3.22
RUN adduser -D -u 10001 app
WORKDIR /app
COPY --from=build /out/server ./server
COPY config.yaml ./config.yaml
COPY migrations ./migrations
USER app
EXPOSE 8085
ENTRYPOINT ["./server"]
