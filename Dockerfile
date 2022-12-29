FROM golang:1.19.4 as build

WORKDIR /app

# START GIT PRIVATE SECTION - delete if not using private packages
ARG GIT_INSTEAD_OF=ssh://git@github.com/
ARG GO_ARGS=""

# Need ssh for private packages
RUN mkdir /root/.ssh && echo "# github.com\n|1|ljja8g3oSggsnjO9rsrgs7Udx2s=|I6pPqynzf/0nwAnJ3LQ4n9n6Gc8= ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=\n|1|rFMG6UlqGl4xrNGGKf6FYK56sMU=|bLF794kw2BGoKCjiN696DX+dMh4= ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=" >> /root/.ssh/known_hosts
RUN go env -w GOPRIVATE=github.com/<YOUR ORG>
RUN git config --global url."${GIT_INSTEAD_OF}".insteadOf https://github.com/
# END GIT PRIVATE SECTION

COPY go.* /app/

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=ssh \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build $GO_ARGS -o /app/outbin

# Need glibc
FROM gcr.io/distroless/base

ENTRYPOINT ["/app/outbin"]
COPY --from=build /app/outbin /app/
