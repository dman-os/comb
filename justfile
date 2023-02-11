set shell := ["sh", "-c"]
set dotenv-load

default:
  @just --list --unsorted

alias b := build
build:
    zig build

alias r := run
run:
    zig build run

alias t := test
test:
    zig build test

test-f +FILTER:
    zig test src/main.zig -freference-trace --test-filter {{FILTER}}

bench *ARGS:
    zig test src/benches.zig -O ReleaseFast {{ARGS}}

clean:
    find -name tmpfs | sudo xargs umount 2> /dev/null || true
    sudo rm zig-cache/ zig-out/ -r 2> /dev/null || true
