project(Tweetoscope-Collector VERSION 1.0)

include_directories(${Boost_INCLUDE_DIRS} ${CPPKAFKA_INCLUDE_DIRS} ${RDKAFKA_STATIC_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/)

link_libraries(${CPPKAFKA_LIBRARIES} pthread)
link_libraries(${RDKAFKA_LIBRARIES} pthread)

add_library(collector
         SHARED
         ${CMAKE_CURRENT_SOURCE_DIR}/src/Collector.cpp)

install(TARGETS collector
        DESTINATION lib)

file(GLOB 
    headerfiles 
    ${CMAKE_CURRENT_SOURCE_DIR}/src/tweet*.hpp
    )

install(FILES ${headerfiles} DESTINATION include/${CMAKE_PROJECT_NAME})
link_libraries(collector pthread)



add_subdirectory(src)
enable_testing()
add_subdirectory(tests)

