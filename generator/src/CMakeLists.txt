

add_executable(tweet-generator tweet-generator.cpp)
install(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/tweet-generator DESTINATION bin)

