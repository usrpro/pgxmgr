FROM golang:latest as build

RUN go get -v \
    github.com/fulldump/goconfig \
    github.com/inconshreveable/log15 \
    github.com/jackc/pgx \
    github.com/usrpro/dotpgx

ADD *.go /go/src/github.com/usrpro/pgxmgr/

RUN go install -v \
    github.com/usrpro/pgxmgr

# Service specific layer
ADD runner/*.go /go/src/github.com/usrpro/pgxmgr/runner/
WORKDIR /go/src/github.com/usrpro/pgxmgr/runner
RUN go build -v

##########################################################################
FROM debian:stretch-slim

RUN apt-get -y update \
    && apt-get -y install ca-certificates \
    && rm -rf /var/lib/apt/lists

RUN mkdir /migrations

COPY --from=build /go/src/github.com/usrpro/pgxmgr/runner/runner /usr/local/bin/runner

ENTRYPOINT ["runner"]
