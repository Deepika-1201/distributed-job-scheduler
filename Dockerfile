# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/jobscheduler ./cmd/jobscheduler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/jobscheduler /jobscheduler
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/jobscheduler"]
