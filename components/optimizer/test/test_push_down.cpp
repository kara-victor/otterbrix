#include <catch2/catch.hpp>

#include <components/optimizer/optimizer.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <vector>

using components::sql::transform::pg_cell_to_node_cast;

namespace {

    using components::logical_plan::node_ptr;

    node_ptr sql_to_node(std::pmr::memory_resource* resource, const std::string& query) {
        std::pmr::monotonic_buffer_resource arena_resource(resource);
        auto stmt = linitial(raw_parser(&arena_resource, query.c_str()));
        auto transformer = components::sql::transform::transformer(resource);
        auto result = std::get<components::sql::transform::result_view>(
            transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        return result.node;
    }

    node_ptr optimize_sql_to_node(std::pmr::memory_resource* resource, const std::string& query) {
        components::optimizer::optimizer_t optimizer;
        return optimizer.optimize(resource, sql_to_node(resource, query));
    }

} // namespace

TEST_CASE("components::optimizer::push_down(sql): inner join + WHERE left_only AND right_only") {
    auto pool = std::pmr::synchronized_pool_resource();
    const std::string query =
        R"_(SELECT * FROM col1 JOIN col2 ON col1.id = col2.id_col1 WHERE col1.left_value >= 10 AND col2.right_value < 20;)_";
    const std::string target_query =
        R"_(SELECT * FROM col1 JOIN col2 ON col1.id = col2.id_col1 AND col1.left_value >= 10 AND col2.right_value < 20;)_";

    auto got = optimize_sql_to_node(&pool, query);
    auto should_get = optimize_sql_to_node(&pool, target_query);
    INFO(got->to_string());
    INFO(should_get->to_string());
    REQUIRE(got->to_string() == should_get->to_string());
}

TEST_CASE("components::optimizer::push_down(sql): inner join + WHERE left_only AND mixed") {
    auto pool = std::pmr::synchronized_pool_resource();
    const std::string query =
        R"_(SELECT * FROM col1 JOIN col2 ON col1.id = col2.id_col1 WHERE col1.left_value >= 10 AND col1.id = col2.id_col1;)_";

    auto got = optimize_sql_to_node(&pool, query);
    const std::string got_string = got->to_string();
    INFO(got_string);
    REQUIRE(got_string.find("$match: {") != std::string::npos);
    REQUIRE(got_string.find("\"left_value\": {$gte: #0}") != std::string::npos);
    REQUIRE(got_string.find("\"id\": {$eq: \"id_col1\"}") != std::string::npos);
}

TEST_CASE("components::optimizer::push_down(sql): inner join + WHERE left_only OR right_only") {
    auto pool = std::pmr::synchronized_pool_resource();
    const std::string query =
        R"_(SELECT * FROM col1 JOIN col2 ON col1.id = col2.id_col1 WHERE col1.left_value >= 10 OR col2.right_value < 20;)_";

    auto got = optimize_sql_to_node(&pool, query);
    auto original = sql_to_node(&pool, query);
    INFO(got->to_string());
    INFO(original->to_string());
    REQUIRE(got->to_string() == original->to_string());
}

TEST_CASE("components::optimizer::push_down(sql): outer joins are safe mode (no pushdown)") {
    auto pool = std::pmr::synchronized_pool_resource();
    const std::vector<std::string> queries = {
        R"_(SELECT * FROM col1 LEFT JOIN col2 ON col1.id = col2.id_col1 WHERE col1.left_value >= 10 AND col2.right_value < 20;)_",
        R"_(SELECT * FROM col1 RIGHT JOIN col2 ON col1.id = col2.id_col1 WHERE col1.left_value >= 10 AND col2.right_value < 20;)_",
        R"_(SELECT * FROM col1 FULL JOIN col2 ON col1.id = col2.id_col1 WHERE col1.left_value >= 10 AND col2.right_value < 20;)_"};

    for (const auto& query : queries) {
        auto got = optimize_sql_to_node(&pool, query);
        auto original = sql_to_node(&pool, query);
        INFO(query);
        INFO(got->to_string());
        INFO(original->to_string());
        REQUIRE(got->to_string() == original->to_string());
    }
}
