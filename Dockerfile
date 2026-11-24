FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/quorumkv ./cmd/quorumkv \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/qkdump ./cmd/qkdump \
 && mkdir /out/data

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/quorumkv /out/qkdump /usr/local/bin/
COPY --from=build --chown=65532:65532 /out/data /data
ENV QK_DATA_DIR=/data
EXPOSE 7001
ENTRYPOINT ["/usr/local/bin/quorumkv"]
