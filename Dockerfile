FROM rust:latest as builder

RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /usr/src/chord_dht
COPY ./chord ./chord
COPY ./demo ./demo

WORKDIR /usr/src/chord_dht/demo

RUN cargo build --release

FROM ubuntu:22.04

ENV IP=none
ENV MODE=new

COPY --from=builder /usr/src/chord_dht/demo/target/release/demo /usr/local/bin/chord_demo

CMD ["sh", "-c", "chord_demo IP=$IP MODE=$MODE"]
