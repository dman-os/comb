set shell := ["sh", "-c"]
set dotenv-load

alias b := build
build:
    zig build

alias r := run
run:
    zig build run

alias t := test
test:
    zig build test

bench +ARGS:
    zig test src/benches.zig  -O ReleaseFast {{ARGS}}
