find_package (Boost COMPONENTS unit_test_framework REQUIRED)

add_executable(collector-tests collector-tests.cpp)
target_include_directories (collector-tests
      PUBLIC  ${Boost_INCLUDE_DIRS})
      
target_compile_definitions(collector-tests PUBLIC BOOST_TEST_DYN_LINK)
target_link_libraries(collector-tests
	  ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}
	  )

add_test(NAME collector-tests
        COMMAND collector-tests)
