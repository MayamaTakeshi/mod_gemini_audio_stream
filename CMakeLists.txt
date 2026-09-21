cmake_minimum_required(VERSION 3.18)

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" MOD_GEMINI_AUDIO_STREAM_VERSION LIMIT_COUNT 1)
if(NOT MOD_GEMINI_AUDIO_STREAM_VERSION MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
    message(FATAL_ERROR "VERSION must contain a semantic version in MAJOR.MINOR.PATCH format")
endif()

project(mod_gemini_audio_stream
        VERSION "${MOD_GEMINI_AUDIO_STREAM_VERSION}"
        DESCRIPTION "Audio streaming module for FreeSWITCH."
        HOMEPAGE_URL "https://github.com/VoiSmart/mod_openai_realtime")

include(GNUInstallDirs)

option(ENABLE_LOCAL "Enable local compile/debug specific" OFF)
if(ENABLE_LOCAL)
    set(ENV{PKG_CONFIG_PATH} "/usr/local/freeswitch/lib/pkgconfig:$ENV{PKG_CONFIG_PATH}")
endif()
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

find_package(PkgConfig REQUIRED)
find_package(SpeexDSP REQUIRED)
find_package(Threads REQUIRED)

pkg_check_modules(FreeSWITCH REQUIRED IMPORTED_TARGET freeswitch)
pkg_get_variable(FS_MOD_DIR freeswitch modulesdir)
message(STATUS "FreeSWITCH modules dir: ${FS_MOD_DIR}")

set(IXWEBSOCKET_INSTALL OFF CACHE BOOL "Disable ixwebsocket installation")
option(USE_TLS "Use TLS for secure WebSocket connections" ON)
add_subdirectory(libs/IXWebSocket)

add_library(mod_gemini_audio_stream SHARED
    mod_gemini_audio_stream.c
    mod_gemini_audio_stream.h
    gemini_live_streamer_glue.h
    gemini_live_streamer_glue.cpp
    stream_protocol.cpp
    stream_protocol.h
    base64.cpp
)

set_target_properties(mod_gemini_audio_stream PROPERTIES
    PREFIX ""
    POSITION_INDEPENDENT_CODE ON
    C_EXTENSIONS ON
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS ON
)

target_compile_features(mod_gemini_audio_stream PRIVATE c_std_99)

target_include_directories(mod_gemini_audio_stream PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/libs/IXWebSocket
    ${SPEEXDSP_INCLUDE_DIRS}
)

target_link_libraries(mod_gemini_audio_stream PRIVATE
    PkgConfig::FreeSWITCH
    ${SPEEXDSP_LIBRARIES}
    ixwebsocket
    Threads::Threads
)

install(TARGETS ${PROJECT_NAME}
        COMPONENT ${PROJECT_NAME}
        DESTINATION ${FS_MOD_DIR})
