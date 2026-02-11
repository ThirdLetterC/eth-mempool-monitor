default:
    @just --list

build:
    zig build

build-mimalloc:
    zig build -Dmimalloc=true

build-sanitize:
    zig build -Dsanitizers=true

build-release:
    zig build -Doptimize=ReleaseFast

build-valgrind:
    zig build -Dvalgrind=true

format:
    find src include -type f \( -name '*.c' -o -name '*.h' \) -print0 | xargs -0 clang-format -i

valgrind-rpc-control *args:
    zig build -Dvalgrind=true valgrind-rpc-control -- {{args}}

run *args:
    zig build run-example -- {{args}}

run-secure *args:
    zig build run-example -- --secure {{args}}

run-insecure *args:
    zig build run-example -- --insecure {{args}}

run-config config="config.toml" *args:
    zig build run-example -- --config {{config}} {{args}}
