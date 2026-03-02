#include <catch2/catch.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/optimizer/rules/push_down.hpp>

namespace {
    using components::expressions::compare_type;
    using components::expressions::expression_ptr;
    using components::expressions::key_t;
    using components::expressions::make_compare_expression;
    using components::expressions::make_compare_union_expression;
    using components::expressions::side_t;
    using components::logical_plan::join_type;
    using components::logical_plan::make_node_aggregate;
    using components::logical_plan::make_node_join;
    using components::logical_plan::make_node_match;
    using components::logical_plan::node_ptr;

    node_ptr make_scan(std::pmr::memory_resource* resource, const char* db, const char* collection) {
        return make_node_aggregate(resource, collection_full_name_t(db, collection));
    }

    expression_ptr left_only(std::pmr::memory_resource* resource) {
        return make_compare_expression(resource,
                                       compare_type::gte,
                                       key_t(resource, "left_value", side_t::left),
                                       core::parameter_id_t{1});
    }

    expression_ptr right_only(std::pmr::memory_resource* resource) {
        return make_compare_expression(resource,
                                       compare_type::lt,
                                       key_t(resource, "right_value", side_t::right),
                                       core::parameter_id_t{2});
    }

    expression_ptr mixed(std::pmr::memory_resource* resource) {
        return make_compare_expression(resource,
                                       compare_type::eq,
                                       key_t(resource, "left_id", side_t::left),
                                       key_t(resource, "right_id", side_t::right));
    }

    node_ptr make_root_join(std::pmr::memory_resource* resource, join_type type, const expression_ptr& expr) {
        auto join = make_node_join(resource, collection_full_name_t("db", "joined"), type);
        join->append_child(make_scan(resource, "db", "left_collection"));
        join->append_child(make_scan(resource, "db", "right_collection"));
        join->append_expression(expr);

        auto root = make_node_aggregate(resource, collection_full_name_t("db", "root"));
        root->append_child(join);
        return root;
    }
} // namespace

TEST_CASE("components::optimizer::push_down(manual): inner + left/right predicates") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto and_expr = make_compare_union_expression(&pool, compare_type::union_and);
    and_expr->append_child(left_only(&pool));
    and_expr->append_child(right_only(&pool));

    auto root = make_root_join(&pool, join_type::inner, and_expr);
    root = components::optimizer::rules::push_down_t{}.apply(&pool, std::move(root));

    auto join = root->children().front();
    REQUIRE(join->children().at(0)->type() == components::logical_plan::node_type::match_t);
    REQUIRE(join->children().at(1)->type() == components::logical_plan::node_type::match_t);
    REQUIRE(join->expressions().empty());
}

TEST_CASE("components::optimizer::push_down(manual): inner + left AND mixed") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto and_expr = make_compare_union_expression(&pool, compare_type::union_and);
    and_expr->append_child(left_only(&pool));
    and_expr->append_child(mixed(&pool));

    auto root = make_root_join(&pool, join_type::inner, and_expr);
    root = components::optimizer::rules::push_down_t{}.apply(&pool, std::move(root));

    auto join = root->children().front();
    REQUIRE(join->children().at(0)->type() == components::logical_plan::node_type::match_t);
    REQUIRE(join->children().at(1)->type() != components::logical_plan::node_type::match_t);
    REQUIRE(join->expressions().size() == 1);
    REQUIRE(join->expressions().front()->to_string() == mixed(&pool)->to_string());
}

TEST_CASE("components::optimizer::push_down(manual): inner + left OR right no push") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto or_expr = make_compare_union_expression(&pool, compare_type::union_or);
    or_expr->append_child(left_only(&pool));
    or_expr->append_child(right_only(&pool));

    auto root = make_root_join(&pool, join_type::inner, or_expr);
    const auto before = root->to_string();
    root = components::optimizer::rules::push_down_t{}.apply(&pool, std::move(root));

    REQUIRE(root->to_string() == before);
}

TEST_CASE("components::optimizer::push_down(manual): outer joins safe mode") {
    auto pool = std::pmr::synchronized_pool_resource();

    auto and_expr = make_compare_union_expression(&pool, compare_type::union_and);
    and_expr->append_child(left_only(&pool));
    and_expr->append_child(right_only(&pool));

    for (auto type : {join_type::left, join_type::right, join_type::full}) {
        auto root = make_root_join(&pool, type, and_expr);
        const auto before = root->to_string();
        root = components::optimizer::rules::push_down_t{}.apply(&pool, std::move(root));
        REQUIRE(root->to_string() == before);
    }
}
