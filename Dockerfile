# syntax=docker/dockerfile:1
FROM golang AS build

WORKDIR /workspace

COPY . .
RUN --mount=type=cache,target=/root/cache/go-build \
  --mount=type=cache,target=/go/pkg/mod \
  go build ./cmd/dra-driver-template

FROM scratch

COPY --from=build /workspace/dra-driver-template /
ENTRYPOINT ["/dra-driver-template"]
