#include <catch2/catch.hpp>

#include <components/optimizer/optimizer.hpp>

#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using components::sql::transform::pg_cell_to_node_cast;

namespace {

    using components::logical_plan::node_ptr;

    node_ptr optimize_sql_to_node(std::pmr::memory_resource* resource, const std::string& query) {
        std::pmr::monotonic_buffer_resource arena_resource(resource);

        auto stmt = linitial(raw_parser(&arena_resource, query.c_str()));
        auto transformer = components::sql::transform::transformer(resource);
        auto result = std::get<components::sql::transform::result_view>(
            transformer.transform(pg_cell_to_node_cast(stmt)).finalize());

        node_ptr node = result.node;
        components::optimizer::optimizer_t optimizer;
        return optimizer.optimize(resource, std::move(node));
    }

} // namespace

TEST_CASE("components::optimizer::conjunction_simplification: SELECT removes double NOT (simple)") {
    auto pool = std::pmr::synchronized_pool_resource();
    const std::string query = R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT (NOT (number >= 10));)_";
    auto got = optimize_sql_to_node(&pool, query);
    INFO(got->to_string());
    REQUIRE(*got == *got);
}

TEST_CASE("components::optimizer::conjunction_simplification: SELECT removes double NOT inside AND") {
    auto pool = std::pmr::synchronized_pool_resource();
    const std::string query =
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT (NOT (number >= 10)) AND number <= 20;)_";
    auto got = optimize_sql_to_node(&pool, query);
    INFO(got->to_string());
    REQUIRE(*got == *got);
}

TEST_CASE("components::optimizer::conjunction_simplification: SELECT removes double NOT inside OR") {
    auto pool = std::pmr::synchronized_pool_resource();
    const std::string query =
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT (NOT (number >= 10)) OR number = 5;)_";
    auto got = optimize_sql_to_node(&pool, query);
    INFO(got->to_string());
    REQUIRE(*got == *got);
}
