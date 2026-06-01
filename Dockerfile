# syntax=docker/dockerfile:1

# The command to build
ARG cmd

FROM --platform=$BUILDPLATFORM golang AS build

ARG cmd
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath ./cmd/${cmd}

FROM scratch

ARG cmd

COPY --from=build /workspace/${cmd} /cmd
ENTRYPOINT ["/cmd"]
