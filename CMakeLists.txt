cmake_minimum_required(VERSION 3.10.2)

project(Tweetoscope VERSION 4.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_BUILD_TYPE Release)


find_package(PkgConfig)

pkg_check_modules(GAML      REQUIRED gaml)
pkg_check_modules(CPPKAFKA  REQUIRED cppkafka)
pkg_check_modules(RDKAFKA   REQUIRED rdkafka)
find_package(Boost COMPONENTS filesystem REQUIRED)

add_subdirectory(generator)
add_subdirectory(collector)







