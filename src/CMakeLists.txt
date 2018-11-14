cmake_minimum_required(VERSION 3.9)
project(WoDServer)

if(UNIX AND NOT APPLE)
    set(LINUX TRUE)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")

if (LINUX)
    set(CMAKE_BUILD_TYPE Release)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
    set(Boost_USE_STATIC_LIBS   ON)
endif()

set(BOOST_COMPONENTS system filesystem)

if (LINUX)
    set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
    #set(CMAKE_EXE_LINKER_FLAGS "-static")
endif()

find_package(Threads REQUIRED)

# Late 2017 TODO: remove the following checks and always use std::regex
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.9)
        set(BOOST_COMPONENTS ${BOOST_COMPONENTS} regex)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_BOOST_REGEX")
    endif()
endif()

find_package(Boost COMPONENTS ${BOOST_COMPONENTS} REQUIRED)
include_directories(SYSTEM ${Boost_INCLUDE_DIR})

include_directories(./include
                    /usr/include
                    /usr/local/include
                    /usr/local/include/simple-web-server
                    /usr/local/include/rapidjson/
                    /usr/local/include/snappy)

if(APPLE)
    set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl")
endif()


set(SOURCE_FILES main.cpp ./include/Server.h Server.cpp Store.cpp EntityHandler.cpp StoreManager.cpp base64.cpp xxhash.c)

add_executable(wodserver ${SOURCE_FILES})

if (LINUX)
    link_directories(/usr/lib/x86_64-linux-gnu /usr/local/lib)
    target_link_libraries(wodserver /usr/local/lib/librocksdb.a /usr/lib/x86_64-linux-gnu/libsnappy.a /usr/lib/x86_64-linux-gnu/libz.a /usr/lib/x86_64-linux-gnu/libzstd.a /usr/lib/x86_64-linux-gnu/liblz4.a /usr/lib/x86_64-linux-gnu/libbz2.a)
else()
    link_directories(/usr/local/lib)
    target_link_libraries(wodserver rocksdb snappy z lz4)
endif()

target_link_libraries(wodserver ${Boost_LIBRARIES})
target_link_libraries(wodserver ${CMAKE_THREAD_LIBS_INIT})


set(TEST_SOURCE_FILES Tests.cpp Server.cpp Store.cpp EntityHandler.cpp StoreManager.cpp base64.cpp xxhash.c)
add_executable(wodservertests ${TEST_SOURCE_FILES})

target_link_libraries(wodservertests ${Boost_LIBRARIES})

if (LINUX)
    link_directories(/usr/lib/x86_64-linux-gnu /usr/local/lib)
    target_link_libraries(wodservertests /usr/local/lib/librocksdb.a /usr/lib/x86_64-linux-gnu/libsnappy.a /usr/lib/x86_64-linux-gnu/libz.a /usr/lib/x86_64-linux-gnu/libzstd.a /usr/lib/x86_64-linux-gnu/liblz4.a /usr/lib/x86_64-linux-gnu/libbz2.a /usr/lib/x86_64-linux-gnu/libdl.so)
else()
    link_directories(/usr/local/lib)
    target_link_libraries(wodservertests rocksdb snappy z lz4)
endif()

target_link_libraries(wodservertests ${CMAKE_THREAD_LIBS_INIT})
