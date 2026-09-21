#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
build_dir=${BUILD_DIR:-"${project_dir}/build/tests"}
parallel_jobs=${CMAKE_BUILD_PARALLEL_LEVEL:-}

if [ -z "${parallel_jobs}" ]; then
    if command -v nproc >/dev/null 2>&1; then
        parallel_jobs=$(nproc)
    elif command -v sysctl >/dev/null 2>&1; then
        parallel_jobs=$(sysctl -n hw.ncpu 2>/dev/null || printf '2\n')
    else
        parallel_jobs=2
    fi
fi

case "${parallel_jobs}" in
    ''|*[!0-9]*|0)
        echo "CMAKE_BUILD_PARALLEL_LEVEL must be a positive integer" >&2
        exit 2
        ;;
esac

sanitizer_args=
if [ "${1:-}" = "--sanitizers" ]; then
    sanitizer_args="-DENABLE_TEST_SANITIZERS=ON"
elif [ "$#" -ne 0 ]; then
    echo "usage: $0 [--sanitizers]" >&2
    exit 2
fi

cmake -S "${project_dir}/tests" -B "${build_dir}" \
    -DCMAKE_BUILD_TYPE=Debug \
    ${sanitizer_args}
cmake --build "${build_dir}" --parallel "${parallel_jobs}"
ctest --test-dir "${build_dir}" --output-on-failure
