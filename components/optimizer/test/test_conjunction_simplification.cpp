#include <catch2/catch.hpp>

#include <components/optimizer/optimizer.hpp>

#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace {

    using components::logical_plan::node_ptr;

    std::string optimize_sql_to_string(std::pmr::memory_resource* resource, const std::string& query) {
        std::pmr::monotonic_buffer_resource arena_resource(resource);

        auto stmt = linitial(raw_parser(&arena_resource, query.c_str()));
        auto transformer = components::sql::transform::transformer(resource);
        auto result = std::get<components::sql::transform::result_view>(
            transformer.transform(pg_cell_to_node_cast(stmt)).finalize());

        node_ptr node = result.node;
        components::optimizer::optimizer_t optimizer;
        node = optimizer.optimize(resource, std::move(node));
        return node->to_string();
    }

} // namespace

TEST_CASE("components::optimizer::conjunction_simplification: SELECT removes double NOT (simple)") {
    auto pool = std::pmr::synchronized_pool_resource();

    const std::string query = R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT (NOT (number >= 10));)_";

    // До оптимизации (как в sql тестах) было бы:
    // $aggregate: {$match: {$not: [$not: ["number": {$gte: #0}]]}}
    // После conjunction_simplification должно стать без $not.
    REQUIRE(optimize_sql_to_string(&pool, query) == R"_($aggregate: {$match: {"number": {$gte: #0}}})_");
}

TEST_CASE("components::optimizer::conjunction_simplification: SELECT removes double NOT inside AND") {
    auto pool = std::pmr::synchronized_pool_resource();

    const std::string query =
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT (NOT (number >= 10)) AND number <= 20;)_";

    // Ожидаем, что первая часть упростится до ("number": {$gte: #0}),
    // а AND сохранится.
    REQUIRE(optimize_sql_to_string(&pool, query) ==
            R"_($aggregate: {$match: {$and: ["number": {$gte: #0}, "number": {$lte: #1}]}})_");
}

TEST_CASE("components::optimizer::conjunction_simplification: SELECT removes double NOT inside OR") {
    auto pool = std::pmr::synchronized_pool_resource();

    const std::string query =
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT (NOT (number >= 10)) OR number = 5;)_";

    REQUIRE(optimize_sql_to_string(&pool, query) ==
            R"_($aggregate: {$match: {$or: ["number": {$gte: #0}, "number": {$eq: #1}]}})_");
}