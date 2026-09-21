#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
build_root=${BUILD_ROOT:-/tmp/mod-gemini-audio-stream-build}
parallel_jobs=${CMAKE_BUILD_PARALLEL_LEVEL:-$(nproc)}

case "${parallel_jobs}" in
    ''|*[!0-9]*|0)
        echo "CMAKE_BUILD_PARALLEL_LEVEL must be a positive integer" >&2
        exit 2
        ;;
esac
export CMAKE_BUILD_PARALLEL_LEVEL="${parallel_jobs}"

BUILD_DIR="${build_root}/unit" "${project_dir}/tests/run-unit.sh" --sanitizers

cmake -S "${project_dir}" -B "${build_root}/module" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build "${build_root}/module" --parallel "${parallel_jobs}"
cmake --install "${build_root}/module"
ldconfig

exec "${project_dir}/tests/integration/run.sh"
