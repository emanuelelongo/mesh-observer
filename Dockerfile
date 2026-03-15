FROM golang:1.22-alpine AS builder

WORKDIR /src

ARG TARGETOS=linux
ARG TARGETARCH=arm64

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/mesh-observer .

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=builder /out/mesh-observer /mesh-observer

ENTRYPOINT ["/mesh-observer"]
