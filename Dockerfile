FROM golang:1.20-alpine AS build_base

RUN apk add alpine-sdk
RUN apk --update add build-base
RUN apk --update add git


# Set the Current Working Directory inside the container
WORKDIR /app

# We want to populate the module cache based on the go.{mod,sum} files.
COPY go.mod .
COPY go.sum .

RUN go env -w GOPRIVATE=gitlab.com/dohome-2020/*

RUN git config \
    --global \
    url."https://nikom.san:glpat-Masb63XaLNUdMdaWySWo@gitlab.com".insteadOf \
    "https://gitlab.com"

RUN go mod download

COPY . .

# Build the Go app
# RUN go build -o ./out .
# https://github.com/confluentinc/confluent-kafka-go/issues/461#issuecomment-617591791
RUN GOOS=linux GOARCH=amd64 go build -tags musl -o ./out .


# Start fresh from a smaller image
FROM alpine:3.15.0
RUN apk add ca-certificates

COPY --from=build_base /app/out /app

# This container exposes port 8080 to the outside world
EXPOSE 4000 443

# Run the binary program produced by `go install`
CMD ["/app"]
