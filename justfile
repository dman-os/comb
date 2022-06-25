set shell := ["sh", "-c"]
set dotenv-load

alias r := run
run:
    zig build run

alias t := test
test:
    zig build test
