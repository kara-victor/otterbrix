#include <catch2/catch.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/optimizer/rules/push_down.hpp>

namespace {

    using components::expressions::compare_expression_ptr;
    using components::expressions::compare_type;
    using components::expressions::expression_ptr;
    using components::expressions::make_compare_expression;
    using components::expressions::make_compare_union_expression;
    using components::expressions::side_t;
    using components::logical_plan::join_type;
    using components::logical_plan::node_ptr;
    using key = components::expressions::key_t;

    components::collection_full_name_t col(std::string_view collection) {
        return {"db", std::string(collection)};
    }

    compare_expression_ptr left_score_gte(std::pmr::memory_resource* resource, uint16_t param) {
        return make_compare_expression(
            resource, compare_type::gte, key(resource, "score", side_t::left), core::parameter_id_t(param));
    }

    compare_expression_ptr right_score_gte(std::pmr::memory_resource* resource, uint16_t param) {
        return make_compare_expression(
            resource, compare_type::gte, key(resource, "score", side_t::right), core::parameter_id_t(param));
    }

    compare_expression_ptr eq_join_id(std::pmr::memory_resource* resource) {
        return make_compare_expression(
            resource, compare_type::eq, key(resource, "id", side_t::left), key(resource, "id_col1", side_t::right));
    }

    compare_expression_ptr unknown_constant(std::pmr::memory_resource* resource, uint16_t param) {
        return make_compare_expression(
            resource, compare_type::gte, key(resource, "limit", side_t::undefined), core::parameter_id_t(param));
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

    node_ptr make_join(std::pmr::memory_resource* resource, join_type type = join_type::inner) {
        auto join = components::logical_plan::make_node_join(resource, col("join"), type);
        join->append_child(components::logical_plan::make_node_aggregate(resource, col("col1")));
        join->append_child(components::logical_plan::make_node_aggregate(resource, col("col2")));
        join->append_expression(eq_join_id(resource));
        return join;
    }

} // namespace

TEST_CASE("components::optimizer::push_down: join expression is pushed to left child") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto plan = make_join(&pool);
    plan->append_expression(left_score_gte(&pool, 0));

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, plan);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"id\": {$eq: \"id_col1\"}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #0}}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 1);
}

TEST_CASE("components::optimizer::push_down: join expression is pushed to right child") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto plan = make_join(&pool);
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

    auto plan = make_join(&pool);
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

    auto plan = make_join(&pool);
    plan->append_expression(make_compare_expression(&pool,
                                                    compare_type::gte,
                                                    key(&pool, "left_score", side_t::left),
                                                    key(&pool, "right_score", side_t::right)));

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, plan);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"left_score\": {$gte: \"right_score\"}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 0);
}

TEST_CASE("components::optimizer::push_down: unknown-side predicate stays on join") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto plan = make_join(&pool);
    plan->append_expression(unknown_constant(&pool, 5));

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, plan);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"limit\": {$gte: #5}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 0);
}

TEST_CASE("components::optimizer::push_down: non-inner join keeps side-specific predicates on join") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto join = make_join(&pool, join_type::left);
    join->append_expression(left_score_gte(&pool, 0));

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, join);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("$type: left") != std::string::npos);
    REQUIRE(got_str.find("\"score\": {$gte: #0}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 0);
}

TEST_CASE("components::optimizer::push_down: match above inner join is distributed to join children") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto join = make_join(&pool);
    auto top_match_expr = make_compare_union_expression(&pool, compare_type::union_and);
    top_match_expr->append_child(left_score_gte(&pool, 0));
    top_match_expr->append_child(right_score_gte(&pool, 1));

    auto top_match = components::logical_plan::make_node_match(&pool, col("top"), top_match_expr);
    top_match->append_child(join);

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, top_match);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"id\": {$eq: \"id_col1\"}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #0}}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #1}}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 2);
}

TEST_CASE("components::optimizer::push_down: aggregate child matches are pushed through join") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto aggregate = components::logical_plan::make_node_aggregate(&pool, col("root"));
    auto join = make_join(&pool);
    auto left_match = components::logical_plan::make_node_match(&pool, col("left_filter"), left_score_gte(&pool, 2));
    auto right_match =
        components::logical_plan::make_node_match(&pool, col("right_filter"), right_score_gte(&pool, 3));

    aggregate->append_child(left_match);
    aggregate->append_child(join);
    aggregate->append_child(right_match);

    auto got = components::optimizer::rules::push_down_t{}.apply(&pool, aggregate);
    const auto got_str = got->to_string();

    REQUIRE(got_str.find("\"id\": {$eq: \"id_col1\"}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #2}}") != std::string::npos);
    REQUIRE(got_str.find("$match: {\"score\": {$gte: #3}}") != std::string::npos);
    REQUIRE(occurrences_count(got_str, "$match:") == 2);
}
