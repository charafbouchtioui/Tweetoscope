project(Tweetoscope-generator VERSION 1.0)


include_directories(${Boost_INCLUDE_DIRS} ${CPPKAFKA_INCLUDE_DIRS} 
        ${RDKAFKA_STATIC_INCLUDE_DIRS} ${GAML_INCLUDE_DIRS})

        
link_libraries(${CPPKAFKA_LIBRARIES} pthread)
link_libraries(${RDKAFKA_LIBRARIES} pthread)
link_libraries(${GAML_LIBRARIES} pthread)
        

add_subdirectory(src)