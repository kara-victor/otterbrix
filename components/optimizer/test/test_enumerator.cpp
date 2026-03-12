#include <catch2/catch.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/optimizer/cbo/enumerator.hpp>
#include <components/optimizer/optimizer.hpp>

namespace {

    using components::expressions::compare_type;
    using components::expressions::make_compare_expression;
    using components::expressions::side_t;
    using components::logical_plan::join_type;
    using components::logical_plan::node_ptr;
    using key = components::expressions::key_t;

    collection_full_name_t col(std::string_view collection) { return {"db", std::string(collection)}; }

    node_ptr make_inner_join_plan(std::pmr::memory_resource* resource) {
        auto join = components::logical_plan::make_node_join(resource, col("join"), join_type::inner);
        join->append_child(components::logical_plan::make_node_aggregate(resource, col("left")));
        join->append_child(components::logical_plan::make_node_aggregate(resource, col("right")));
        join->append_expression(make_compare_expression(resource,
                                                        compare_type::eq,
                                                        key(resource, "id", side_t::left),
                                                        key(resource, "id", side_t::right)));
        return join;
    }

} // namespace

TEST_CASE("components::optimizer::cbo::enumerator: produces base and swapped inner join plans") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto candidates = components::optimizer::cbo::enumerator_t{}.enumerate(&pool, make_inner_join_plan(&pool));

    REQUIRE(candidates.size() >= 2);
    REQUIRE(candidates[0].reason == "rule-based plan without conjunction_simplification");
    REQUIRE(candidates[1].reason == "inner join left/right swap");
}

TEST_CASE("components::optimizer::optimizer_t: chooses a plan from CBO candidates") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto optimized = components::optimizer::optimizer_t{}.optimize(&pool, make_inner_join_plan(&pool));

    REQUIRE(optimized != nullptr);
    REQUIRE(optimized->type() == components::logical_plan::node_type::join_t);
}