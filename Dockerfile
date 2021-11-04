FROM golang:alpine as golang
RUN apk add --no-cache git
WORKDIR $GOPATH/src/xqledger/rdbreader
COPY . ./
ADD resources/application.yml ./
RUN CGO_ENABLED=0 go install -ldflags '-extldflags "-static"'

FROM alpine:latest as alpine
RUN apk --no-cache add tzdata zip ca-certificates
WORKDIR /usr/share/zoneinfo
RUN zip -r -0 /zoneinfo.zip .

FROM scratch
COPY --from=golang /go/bin/rdbreader /app
COPY --from=golang /go/src/xqledger/rdbreader/resources/application.yml ./
ENV ZONEINFO /zoneinfo.zip
COPY --from=alpine /zoneinfo.zip /
COPY --from=alpine /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["/app"]