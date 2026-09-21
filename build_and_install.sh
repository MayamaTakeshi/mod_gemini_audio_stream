#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

cmake -S . -B build -DENABLE_LOCAL=ON   # ENABLE_LOCAL points pkg-config at /usr/local/freeswitch
cmake --build build -j
cmake --install build                    # installs mod_gemini_audio_stream.so into the FS modules dir


echo Success
