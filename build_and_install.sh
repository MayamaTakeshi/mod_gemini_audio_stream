#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# ENABLE_LOCAL selects the local FreeSWITCH pkg-config directory.
# Release also optimizes the bundled IXWebSocket library.
cmake -S . -B build -DENABLE_LOCAL=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
cmake --install build                    # installs mod_gemini_audio_stream.so into the FS modules dir


echo Success
