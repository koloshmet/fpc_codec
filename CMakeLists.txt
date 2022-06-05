cmake_minimum_required(VERSION 3.12)
project(fpc_codec)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD 11)

add_compile_options(-Werror -Wall -Wextra -Wpedantic -Wno-unused-but-set-variable)

if ("${CMAKE_BUILD_TYPE}" STREQUAL Debug)
    add_compile_options(-fsanitize=undefined)
    add_link_options(-fsanitize=undefined)
endif()

add_executable(fpc_codec_data data.cpp)
add_executable(fpc_codec_stress stress.cpp)
