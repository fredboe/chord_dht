FROM rust:latest as builder

RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /usr/src/chord_dht
RUN cargo new --lib chord
RUN cargo new demo
COPY ./chord/proto ./chord/proto
COPY ./chord/build.rs ./chord/build.rs
COPY ./chord/Cargo.toml ./chord/Cargo.toml
COPY ./demo/Cargo.toml ./demo/Cargo.toml

RUN echo "fn main() {}" > ./demo/src/main.rs
RUN cargo build --release --manifest-path ./demo/Cargo.toml

RUN rm -rf chord/src/*
RUN rm -rf demo/src/*

COPY ./chord/src ./chord/src
COPY ./demo/src ./demo/src

# Touch so that docker recognizes that something has changed
RUN touch chord/src/lib.rs
RUN touch demo/src/main.rs
RUN cargo build --release --manifest-path ./demo/Cargo.toml

FROM ubuntu:22.04

ENV IP=none
ENV MODE=new

COPY --from=builder /usr/src/chord_dht/demo/target/release/demo /usr/local/bin/chord_demo

CMD ["sh", "-c", "chord_demo", "IP=$IP", "MODE=$MODE"]
