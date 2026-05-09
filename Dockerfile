# syntax=docker/dockerfile:1
FROM golang AS build

WORKDIR /workspace

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 go build ./cmd/dra-driver-sandbox

FROM scratch

COPY --from=build /workspace/dra-driver-sandbox /
ENTRYPOINT ["/dra-driver-sandbox"]
