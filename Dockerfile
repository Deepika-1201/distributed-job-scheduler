# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/jobscheduler ./cmd/jobscheduler && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/demo-worker ./cmd/demo-worker
# RDS's CA bundle, so database connections can verify the server (sslmode=verify-full, LLD §22.3).
ADD https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /out/rds-global-bundle.pem

# docker build --target demo-worker: the example worker pool.
FROM gcr.io/distroless/static-debian12:nonroot AS demo-worker
COPY --from=build /out/demo-worker /demo-worker
USER nonroot:nonroot
ENTRYPOINT ["/demo-worker"]

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/jobscheduler /jobscheduler
COPY --from=build --chmod=644 /out/rds-global-bundle.pem /etc/ssl/certs/rds-global-bundle.pem
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/jobscheduler"]
