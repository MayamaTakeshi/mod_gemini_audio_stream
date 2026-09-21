cmake_minimum_required(VERSION 3.18)
project(mod_gemini_audio_stream_tests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(MODULE_SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/..")

enable_testing()
add_subdirectory(unit)
