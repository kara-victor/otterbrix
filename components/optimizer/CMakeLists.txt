project(optimizer CXX)

set(SOURCE_${PROJECT_NAME}
        optimizer.cpp

        rules/conjunction_simplification.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        otterbrix::logical_plan
        otterbrix::expressions
        otterbrix::document
        otterbrix::vector
        otterbrix::serialization
        Boost::boost
        magic_enum::magic_enum
        msgpackc-cxx
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()