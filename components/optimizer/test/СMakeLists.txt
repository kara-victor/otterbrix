project(test_optimizer)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_conjunction_simplification.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::optimizer
    otterbrix::sql
    Catch2::Catch2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})