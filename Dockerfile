# Multi-stage build: compile with the full Go toolchain, run on a minimal,
# non-root, shell-less base. Keeps the shipped image small and reduces attack
# surface (no shell, no package manager, nothing an attacker could use post-exploit).
FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache dependency downloads separately from source changes.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# CGO_ENABLED=0: static binary, no libc dependency, required for distroless/static.
ARG TARGET=controlplane
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${TARGET}

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
