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
    uv run ruff format .
    uv run mdformat docs/PYTHON_CLIENT.md
    uv run yamlfix compose.yml compose.prod.yml

check-c-format:
    find src include -type f \( -name '*.c' -o -name '*.h' \) -print0 | xargs -0 clang-format --dry-run --Werror

python-tools:
    uv venv --python 3.13
    uv sync --only-dev

python-check:
    uv run --frozen ruff format --check .
    uv run --frozen ruff check .
    uv run --frozen mdformat --check docs/PYTHON_CLIENT.md
    uv run --frozen yamlfix --check compose.yml compose.prod.yml
    uv run --frozen pyright
    uv run --frozen python -m unittest discover -s tests

valgrind-rpc-control *args:
    zig build -Dvalgrind=true valgrind-rpc-control -- {{args}}

run *args:
    zig build run-example -- {{args}}

run-secure *args:
    zig build run-example -- --secure {{args}}

run-insecure *args:
    zig build run-example -- --insecure {{args}}

run-config config="conf/config.toml" *args:
    zig build run-example -- --config {{config}} {{args}}
