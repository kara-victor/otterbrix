add_test( core::file::filesystem /app/build-docker/core/file/tests/test_file_system core::file::filesystem  )
set_tests_properties( core::file::filesystem PROPERTIES WORKING_DIRECTORY /app/build-docker/core/file/tests)
set( test_file_system_TESTS core::file::filesystem)
