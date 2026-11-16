FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /fleetstate .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /fleetstate /fleetstate
ENV GOMEMLIMIT=200MiB GOMAXPROCS=2
EXPOSE 8000
USER nonroot
ENTRYPOINT ["/fleetstate"]