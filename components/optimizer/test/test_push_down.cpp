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

        components::optimizer::optimizer_t optimizer;
        return optimizer.optimize(resource, result.node);
    }

    node_ptr sql_to_node(std::pmr::memory_resource* resource, const std::string& query) {
        std::pmr::monotonic_buffer_resource arena_resource(resource);

        auto stmt = linitial(raw_parser(&arena_resource, query.c_str()));
        auto transformer = components::sql::transform::transformer(resource);
        auto result = std::get<components::sql::transform::result_view>(
            transformer.transform(pg_cell_to_node_cast(stmt)).finalize());

        return result.node;
    }

} // namespace

TEST_CASE("components::optimizer::push_down: filter from JOIN condition is pushed to left side") {
    auto pool = std::pmr::synchronized_pool_resource();

    const std::string query =
        R"_(SELECT * FROM col1 JOIN col2 ON col1.id = col2.id_col1 WHERE col1.score >= 10;)_";
    const std::string target_query =
        R"_(SELECT * FROM col1 JOIN col2 ON col1.id = col2.id_col1 AND col1.score >= 10;)_";

    auto got = optimize_sql_to_node(&pool, query);
    INFO(got->to_string());

    auto should_get = sql_to_node(&pool, target_query);
    INFO(should_get->to_string());

    REQUIRE(got->to_string() == should_get->to_string());
}

TEST_CASE("components::optimizer::push_down: filter from JOIN condition is pushed to right side") {
    auto pool = std::pmr::synchronized_pool_resource();

    const std::string query =
        R"_(SELECT * FROM col1 JOIN col2 ON col1.id = col2.id_col1 WHERE col2.score >= 10;)_";
    const std::string target_query =
        R"_(SELECT * FROM col1 JOIN col2 ON col1.id = col2.id_col1 AND col2.score >= 10;)_";

    auto got = optimize_sql_to_node(&pool, query);
    INFO(got->to_string());

    auto should_get = sql_to_node(&pool, target_query);
    INFO(should_get->to_string());

    REQUIRE(got->to_string() == should_get->to_string());
}

TEST_CASE("components::optimizer::push_down: filter from JOIN condition is pushed to right and left side") {
    auto pool = std::pmr::synchronized_pool_resource();

    const std::string query = 
        R"_(SELECT * FROM col1 JOIN col2 ON col1.id = col2.id_col1 WHERE col2.score >= 10 AND col1.score >= 10;)_";
    const std::string target_query =
        R"_(SELECT * FROM col1 JOIN col2 ON col1.id = col2.id_col1 AND col2.score >= 10 AND col1.score >= 10;)_";

    auto got = optimize_sql_to_node(&pool, query);
    auto got_not_optimize = sql_to_node(&pool, query);
    INFO(got->to_string());
    INFO(got_not_optimize->to_string());

    auto should_get = sql_to_node(&pool, target_query);
    INFO(should_get->to_string());

    REQUIRE(got->to_string() == should_get->to_string());
}
