FROM golang:1.16-alpine

WORKDIR /go/src
ENV PATH="/go/bin:${PATH}"

RUN apk update && apk add librdkafka-dev

CMD ["tail", "-f", "/dev/null"]