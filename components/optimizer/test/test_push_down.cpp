#include <catch2/catch.hpp>

#include <components/optimizer/rules/push_down.hpp>

#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using components::sql::transform::pg_cell_to_node_cast;

namespace {

    using components::expressions::compare_expression_ptr;
    using components::expressions::compare_type;
    using components::expressions::expression_ptr;
    using components::expressions::key_t;
    using components::expressions::make_compare_expression;
    using components::expressions::make_compare_union_expression;
    using components::expressions::side_t;
    using components::logical_plan::collection_full_name_t;
    using components::logical_plan::join_type;
    using components::logical_plan::make_node_aggregate;
    using components::logical_plan::make_node_join;
    using components::logical_plan::make_node_match;
    using components::logical_plan::node_ptr;

    node_ptr apply_push_down_to_node(std::pmr::memory_resource* resource, const std::string& query) {
        std::pmr::monotonic_buffer_resource arena_resource(resource);

    compare_expression_ptr left_score_gte(std::pmr::memory_resource* resource, uint16_t param) {
        return make_compare_expression(
            resource, compare_type::gte, key_t(resource, "score", side_t::left), core::parameter_id_t(param));
    }

        return components::optimizer::rules::push_down_t{}.apply(resource, result.node);
    }

    compare_expression_ptr unknown_constant(std::pmr::memory_resource* resource, uint16_t param) {
        return make_compare_expression(
            resource, compare_type::gte, key_t(resource, "limit", side_t::undefined), core::parameter_id_t(param));
    }

    std::size_t occurrences_count(const std::string& text, const std::string& token) {
        std::size_t count = 0;
        std::size_t pos = 0;
        while ((pos = text.find(token, pos)) != std::string::npos) {
            ++count;
            pos += token.size();
        }
        return count;
    }

    node_ptr make_basic_inner_join(std::pmr::memory_resource* resource) {
        auto join = make_node_join(resource, col("join"), join_type::inner);
        join->append_child(make_node_aggregate(resource, col("col1")));
        join->append_child(make_node_aggregate(resource, col("col2")));
        join->append_expression(eq_join_id(resource));
        return join;
    }

} // namespace

TEST_CASE("components::optimizer::push_down: join expression is pushed to left child") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto plan = make_basic_inner_join(&pool);
    plan->append_expression(left_score_gte(&pool, 0));

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, plan);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"id\": {$eq: \"id_col1\"}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #0}}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 1);
}

TEST_CASE("components::optimizer::push_down: join expression is pushed to right child") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto plan = make_basic_inner_join(&pool);
    plan->append_expression(right_score_gte(&pool, 0));

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, plan);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"id\": {$eq: \"id_col1\"}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #0}}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 1);
}

TEST_CASE("components::optimizer::push_down: join expression is pushed to both children") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto both = make_compare_union_expression(&pool, compare_type::union_and);
    both->append_child(left_score_gte(&pool, 0));
    both->append_child(right_score_gte(&pool, 1));

    auto plan = make_basic_inner_join(&pool);
    plan->append_expression(both);

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, plan);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"id\": {$eq: \"id_col1\"}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #0}}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #1}}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 2);
}

TEST_CASE("components::optimizer::push_down: predicates using both sides stay on join") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto plan = make_basic_inner_join(&pool);
    plan->append_expression(make_compare_expression(&pool,
                                                    compare_type::gte,
                                                    key_t(&pool, "left_score", side_t::left),
                                                    key_t(&pool, "right_score", side_t::right)));

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, plan);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"left_score\": {$gte: \"right_score\"}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 0);
}

TEST_CASE("components::optimizer::push_down: unknown-side predicate stays on join") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto plan = make_basic_inner_join(&pool);
    plan->append_expression(unknown_constant(&pool, 5));

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, plan);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"limit\": {$gte: #5}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 0);
}

TEST_CASE("components::optimizer::push_down: non-inner join keeps side-specific predicates on join") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto got = apply_push_down_to_node(&pool, query);
    INFO(got->to_string());

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, join);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("$type: left") != std::string::npos);
    REQUIRE(got_str.find("\"score\": {$gte: #0}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 0);
}

TEST_CASE("components::optimizer::push_down: match above inner join is distributed to join children") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto join = make_basic_inner_join(&pool);
    auto top_match_expr = make_compare_union_expression(&pool, compare_type::union_and);
    top_match_expr->append_child(left_score_gte(&pool, 0));
    top_match_expr->append_child(right_score_gte(&pool, 1));

    auto got = apply_push_down_to_node(&pool, query);
    INFO(got->to_string());

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, top_match);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"id\": {$eq: \"id_col1\"}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #0}}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #1}}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 2);
}

TEST_CASE("components::optimizer::push_down: aggregate child matches are pushed through join") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto aggregate = make_node_aggregate(&pool, col("root"));
    auto join = make_basic_inner_join(&pool);
    auto left_match = make_node_match(&pool, col("left_filter"), left_score_gte(&pool, 2));
    auto right_match = make_node_match(&pool, col("right_filter"), right_score_gte(&pool, 3));

    auto got = apply_push_down_to_node(&pool, query);
    auto got_not_optimize = sql_to_node(&pool, query);
    INFO(got->to_string());
    INFO(got_not_optimize->to_string());

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, aggregate);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"id\": {$eq: \"id_col1\"}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #2}}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #3}}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 2);
}
