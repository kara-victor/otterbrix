#include <catch2/catch.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/scan/index_scan.hpp>
#include <components/physical_plan_generator/impl/create_plan_match.hpp>
#include <components/physical_plan_generator/impl/index_selection_helpers.hpp>
#include <components/planner/optimizer.hpp>
#include <services/collection/context_storage.hpp>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

// ================================================================
// Helper: build a match node with a single expression
// ================================================================
static node_ptr make_match_with_expr(std::pmr::memory_resource* r, const expression_ptr& expr) {
    return make_node_match(r, core::dbname_t{database_name}, core::relname_t{collection_name}, expr);
}

static key make_resolved_key(std::pmr::memory_resource* r, const char* name, side_t side, size_t col) {
    key result{r, name, side};
    std::pmr::vector<size_t> path{r};
    path.push_back(col);
    result.set_path(std::move(path));
    return result;
}

static resolved_table_metadata_t make_resolved_table_metadata(size_t column_count,
                                                              components::catalog::oid_t table_oid =
                                                                  components::catalog::oid_t{9001}) {
    resolved_table_metadata_t md;
    md.table_oid = table_oid;
    md.namespace_oid = components::catalog::oid_t{9000};
    md.name = collection_name;
    for (size_t i = 0; i < column_count; ++i) {
        resolved_column_metadata_t col;
        col.attname = "c" + std::to_string(i);
        col.attnum = static_cast<std::int32_t>(i + 1);
        col.chunk_position = static_cast<std::int32_t>(i);
        md.columns.push_back(std::move(col));
    }
    return md;
}

static node_aggregate_ptr make_aggregate_with_match_expr(std::pmr::memory_resource* r,
                                                        const expression_ptr& match_expr) {
    auto aggregate = make_node_aggregate(r, core::dbname_t{database_name}, core::relname_t{collection_name});
    aggregate->set_table_oid(components::catalog::oid_t{9001});

    auto projection = make_scalar_expression(r, scalar_type::get_field, make_resolved_key(r, "c1", side_t::left, 1));
    auto group = make_node_group(r, core::dbname_t{database_name}, core::relname_t{collection_name});
    group->append_expression(projection);

    aggregate->append_child(group);
    aggregate->append_child(make_node_match(r, core::dbname_t{database_name}, core::relname_t{collection_name}, match_expr));
    return aggregate;
}

static node_aggregate_ptr make_prunable_aggregate_plan(std::pmr::memory_resource* r) {
    auto pid_params = make_parameter_node(r);
    auto pid = pid_params->add_parameter(int64_t(10));
    auto predicate = make_compare_expression(r, compare_type::gt, make_resolved_key(r, "c2", side_t::left, 2), pid);
    return make_aggregate_with_match_expr(r, predicate);
}


struct filter_pushdown_plan_t {
    node_sequence_ptr sequence;
    node_aggregate_ptr parent;
    node_aggregate_ptr left;
    node_aggregate_ptr right;
    node_join_ptr join;
};

static node_ptr find_child_of_type(const node_ptr& node, node_type type) {
    for (const auto& child : node->children()) {
        if (child && child->type() == type) {
            return child;
        }
    }
    return nullptr;
}

static node_ptr find_match_child(const node_ptr& node) { return find_child_of_type(node, node_type::match_t); }

static filter_pushdown_plan_t make_filter_pushdown_plan(std::pmr::memory_resource* r,
                                                        join_type type,
                                                        const expression_ptr& predicate,
                                                        const expression_ptr& existing_left_predicate = nullptr) {
    constexpr components::catalog::oid_t left_oid{9101};
    constexpr components::catalog::oid_t right_oid{9102};

    auto left_resolve = make_node_catalog_resolve_table(r, core::dbname_t{database_name}, core::relname_t{"left"});
    left_resolve->set_resolved_metadata(make_resolved_table_metadata(3, left_oid));
    auto right_resolve = make_node_catalog_resolve_table(r, core::dbname_t{database_name}, core::relname_t{"right"});
    right_resolve->set_resolved_metadata(make_resolved_table_metadata(2, right_oid));

    auto left = make_node_aggregate(r, core::dbname_t{database_name}, core::relname_t{"left"});
    left->set_table_oid(left_oid);
    if (existing_left_predicate) {
        auto left_match = make_node_match(r, core::dbname_t{database_name}, core::relname_t{"left"}, existing_left_predicate);
        left_match->set_table_oid(left_oid);
        left->append_child(left_match);
    }

    auto right = make_node_aggregate(r, core::dbname_t{database_name}, core::relname_t{"right"});
    right->set_table_oid(right_oid);

    auto join = make_node_join(r, core::dbname_t{database_name}, core::relname_t{}, type);
    join->append_child(left);
    join->append_child(right);

    auto parent = make_node_aggregate(r, core::dbname_t{database_name}, core::relname_t{});
    parent->append_child(join);
    auto parent_match = make_node_match(r, core::dbname_t{database_name}, core::relname_t{}, predicate);
    parent->append_child(parent_match);

    node_sequence_ptr sequence{new node_sequence_t(r)};
    sequence->append_child(left_resolve);
    sequence->append_child(right_resolve);
    sequence->append_child(parent);

    return {sequence, parent, left, right, join};
}

static compare_expression_ptr require_single_compare(const node_ptr& match) {
    REQUIRE(match);
    REQUIRE(match->expressions().size() == 1);
    REQUIRE(match->expressions()[0]->group() == expression_group::compare);
    return reinterpret_cast<const compare_expression_ptr&>(match->expressions()[0]);
}

// ================================================================
// Pipeline: default pre-validate keeps existing constant folding behavior
// ================================================================
TEST_CASE("optimizer_pipeline::pre_validate_default_folds_constants") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);
    auto node = make_match_with_expr(&resource,
                                     make_compare_expression(&resource,
                                                             compare_type::eq,
                                                             key(&resource, "field", side_t::left),
                                                             expression_ptr(scalar)));

    components::planner::optimizer_context_t context{&resource, params.get(), {}};
    auto result = components::planner::optimizer_pipeline_t(context).run_pre_validate(node);

    REQUIRE(result.get() == node.get());
    auto* folded = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(folded->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(folded->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 5);
}

// ================================================================
// Pipeline: constant folding can be disabled
// ================================================================
TEST_CASE("optimizer_pipeline::pre_validate_can_disable_constant_folding") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);
    auto node = make_match_with_expr(&resource,
                                     make_compare_expression(&resource,
                                                             compare_type::eq,
                                                             key(&resource, "field", side_t::left),
                                                             expression_ptr(scalar)));

    components::planner::optimizer_options_t options;
    options.enable_constant_folding = false;
    components::planner::optimizer_context_t context{&resource, params.get(), options};
    components::planner::optimizer_pipeline_t(context).run_pre_validate(node);

    auto* unchanged = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(unchanged->params().size() == 2);
    REQUIRE(std::get<core::parameter_id_t>(unchanged->params()[0]) == id0);
    REQUIRE(std::get<core::parameter_id_t>(unchanged->params()[1]) == id1);
    REQUIRE(params->parameter(id0).value<int64_t>() == 2);
}

// ================================================================
// Pipeline: default post-validate keeps existing hash-join rewrite behavior
// ================================================================
TEST_CASE("optimizer_pipeline::post_validate_default_rewrites_hash_join") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto join = make_node_join(&resource, core::dbname_t{}, core::relname_t{}, join_type::inner);
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    make_resolved_key(&resource, "l", side_t::left, 0),
                                                    make_resolved_key(&resource, "r", side_t::right, 0)));

    components::planner::optimizer_context_t context{&resource, nullptr, {}};
    auto result = components::planner::optimizer_pipeline_t(context).run_post_validate(join);

    REQUIRE(result->type() == node_type::hash_join_t);
}

// ================================================================
// Pipeline: hash-join rewrite can be disabled
// ================================================================
TEST_CASE("optimizer_pipeline::post_validate_can_disable_hash_join_rewrite") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto join = make_node_join(&resource, core::dbname_t{}, core::relname_t{}, join_type::inner);
    join->append_expression(make_compare_expression(&resource,
                                                    compare_type::eq,
                                                    make_resolved_key(&resource, "l", side_t::left, 0),
                                                    make_resolved_key(&resource, "r", side_t::right, 0)));

    components::planner::optimizer_options_t options;
    options.enable_hash_join_rewrite = false;
    components::planner::optimizer_context_t context{&resource, nullptr, options};
    auto result = components::planner::optimizer_pipeline_t(context).run_post_validate(join);

    REQUIRE(result.get() == join.get());
    REQUIRE(result->type() == node_type::join_t);
}


// ================================================================
// Pipeline: filter pushdown moves left-only predicates below inner join
// ================================================================
TEST_CASE("optimizer_pipeline::filter_pushdown_default_moves_left_only_filter") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(10));
    auto predicate = make_compare_expression(&resource,
                                             compare_type::gt,
                                             make_resolved_key(&resource, "l1", side_t::left, 1),
                                             pid);
    auto plan = make_filter_pushdown_plan(&resource, join_type::inner, predicate);

    components::planner::optimizer_context_t context{&resource, nullptr, {}};
    components::planner::optimizer_pipeline_t(context).run_post_validate(plan.sequence);

    REQUIRE_FALSE(find_match_child(plan.parent));
    auto left_match = find_match_child(plan.left);
    auto left_predicate = require_single_compare(left_match);
    REQUIRE(std::get<key>(left_predicate->left()).side() == side_t::undefined);
    REQUIRE(std::get<key>(left_predicate->left()).path()[0] == 1);
    REQUIRE_FALSE(find_match_child(plan.right));
}

// ================================================================
// Pipeline: filter pushdown remaps right-side joined column indices
// ================================================================
TEST_CASE("optimizer_pipeline::filter_pushdown_default_moves_right_only_filter_with_remap") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(20));
    auto predicate = make_compare_expression(&resource,
                                             compare_type::lt,
                                             make_resolved_key(&resource, "r1", side_t::right, 4),
                                             pid);
    auto plan = make_filter_pushdown_plan(&resource, join_type::inner, predicate);

    components::planner::optimizer_context_t context{&resource, nullptr, {}};
    components::planner::optimizer_pipeline_t(context).run_post_validate(plan.sequence);

    REQUIRE_FALSE(find_match_child(plan.parent));
    auto right_match = find_match_child(plan.right);
    auto right_predicate = require_single_compare(right_match);
    const auto& remapped_key = std::get<key>(right_predicate->left());
    REQUIRE(remapped_key.side() == side_t::undefined);
    REQUIRE(remapped_key.path()[0] == 1);
    REQUIRE_FALSE(find_match_child(plan.left));
}

// ================================================================
// Pipeline: mixed-side predicates remain above join
// ================================================================
TEST_CASE("optimizer_pipeline::filter_pushdown_keeps_mixed_side_filter_above_join") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto predicate = make_compare_expression(&resource,
                                             compare_type::eq,
                                             make_resolved_key(&resource, "l0", side_t::left, 0),
                                             make_resolved_key(&resource, "r0", side_t::right, 3));
    auto plan = make_filter_pushdown_plan(&resource, join_type::inner, predicate);

    components::planner::optimizer_context_t context{&resource, nullptr, {}};
    components::planner::optimizer_pipeline_t(context).run_post_validate(plan.sequence);

    REQUIRE(find_match_child(plan.parent));
    REQUIRE_FALSE(find_match_child(plan.left));
    REQUIRE_FALSE(find_match_child(plan.right));
}

// ================================================================
// Pipeline: OR and function predicates remain above join
// ================================================================
TEST_CASE("optimizer_pipeline::filter_pushdown_keeps_unsupported_filter_above_join") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto pid0 = params->add_parameter(int64_t(10));
    auto pid1 = params->add_parameter(int64_t(20));
    auto left = make_compare_expression(&resource, compare_type::gt, make_resolved_key(&resource, "l1", side_t::left, 1), pid0);
    auto right = make_compare_expression(&resource, compare_type::lt, make_resolved_key(&resource, "l2", side_t::left, 2), pid1);
    auto predicate = make_compare_union_expression(&resource, compare_type::union_or);
    predicate->append_child(left);
    predicate->append_child(right);
    auto plan = make_filter_pushdown_plan(&resource, join_type::inner, predicate);

    components::planner::optimizer_context_t context{&resource, nullptr, {}};
    components::planner::optimizer_pipeline_t(context).run_post_validate(plan.sequence);

    REQUIRE(find_match_child(plan.parent));
    REQUIRE_FALSE(find_match_child(plan.left));
    REQUIRE_FALSE(find_match_child(plan.right));
}

// ================================================================
// Pipeline: filter pushdown can be disabled
// ================================================================
TEST_CASE("optimizer_pipeline::filter_pushdown_can_be_disabled") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(10));
    auto predicate = make_compare_expression(&resource,
                                             compare_type::gt,
                                             make_resolved_key(&resource, "l1", side_t::left, 1),
                                             pid);
    auto plan = make_filter_pushdown_plan(&resource, join_type::inner, predicate);

    components::planner::optimizer_options_t options;
    options.enable_filter_pushdown = false;
    components::planner::optimizer_context_t context{&resource, nullptr, options};
    components::planner::optimizer_pipeline_t(context).run_post_validate(plan.sequence);

    REQUIRE(find_match_child(plan.parent));
    REQUIRE_FALSE(find_match_child(plan.left));
    REQUIRE_FALSE(find_match_child(plan.right));
}

// ================================================================
// Pipeline: pushed predicates merge with an existing child match
// ================================================================
TEST_CASE("optimizer_pipeline::filter_pushdown_merges_existing_child_match") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto existing_pid = params->add_parameter(int64_t(1));
    auto pushed_pid = params->add_parameter(int64_t(10));
    auto existing = make_compare_expression(&resource,
                                            compare_type::gte,
                                            make_resolved_key(&resource, "l0", side_t::undefined, 0),
                                            existing_pid);
    auto pushed = make_compare_expression(&resource,
                                          compare_type::gt,
                                          make_resolved_key(&resource, "l1", side_t::left, 1),
                                          pushed_pid);
    auto plan = make_filter_pushdown_plan(&resource, join_type::inner, pushed, existing);

    components::planner::optimizer_context_t context{&resource, nullptr, {}};
    components::planner::optimizer_pipeline_t(context).run_post_validate(plan.sequence);

    REQUIRE_FALSE(find_match_child(plan.parent));
    auto left_match = find_match_child(plan.left);
    auto merged = require_single_compare(left_match);
    REQUIRE(merged->type() == compare_type::union_and);
    REQUIRE(merged->children().size() == 2);
}

// ================================================================
// Pipeline: outer joins are not changed by filter pushdown
// ================================================================
TEST_CASE("optimizer_pipeline::filter_pushdown_skips_outer_joins") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(10));
    auto predicate = make_compare_expression(&resource,
                                             compare_type::gt,
                                             make_resolved_key(&resource, "l1", side_t::left, 1),
                                             pid);
    auto plan = make_filter_pushdown_plan(&resource, join_type::left, predicate);

    components::planner::optimizer_context_t context{&resource, nullptr, {}};
    components::planner::optimizer_pipeline_t(context).run_post_validate(plan.sequence);

    REQUIRE(find_match_child(plan.parent));
    REQUIRE_FALSE(find_match_child(plan.left));
    REQUIRE_FALSE(find_match_child(plan.right));
}

// ================================================================
// Pipeline: column pruning is enabled by default
// ================================================================
TEST_CASE("optimizer_pipeline::post_validate_default_enables_column_pruning") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto aggregate = make_prunable_aggregate_plan(&resource);

    components::planner::optimizer_context_t context{&resource, nullptr, {}};
    components::planner::optimizer_pipeline_t(context).run_post_validate(aggregate);

    const std::vector<size_t> expected{1, 2};
    REQUIRE(aggregate->projected_cols() == expected);
}

// ================================================================
// Pipeline: column pruning can still be disabled explicitly
// ================================================================
TEST_CASE("optimizer_pipeline::post_validate_can_disable_column_pruning") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto aggregate = make_prunable_aggregate_plan(&resource);

    components::planner::optimizer_options_t options;
    options.enable_column_pruning = false;
    components::planner::optimizer_context_t context{&resource, nullptr, options};
    components::planner::optimizer_pipeline_t(context).run_post_validate(aggregate);

    REQUIRE(aggregate->projected_cols().empty());
}

// ================================================================
// Pipeline: WHERE on a non-selected column adds it to projected_cols
// ================================================================
TEST_CASE("optimizer_pipeline::column_pruning_where_adds_non_selected_column") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto aggregate = make_prunable_aggregate_plan(&resource);
    auto resolve =
        make_node_catalog_resolve_table(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});
    resolve->set_resolved_metadata(make_resolved_table_metadata(3));

    node_sequence_ptr sequence{new node_sequence_t(&resource)};
    sequence->append_child(resolve);
    sequence->append_child(aggregate);

    components::planner::optimizer_context_t context{&resource, nullptr, {}};
    components::planner::optimizer_pipeline_t(context).run_post_validate(sequence);

    const std::vector<size_t> expected{1, 2};
    REQUIRE(aggregate->projected_cols() == expected);
}

// ================================================================
// Pipeline: unsupported match expressions keep read-all-columns fallback
// ================================================================
TEST_CASE("optimizer_pipeline::column_pruning_unsupported_match_expression_falls_back") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::vector<param_storage> args{&resource};
    args.emplace_back(make_resolved_key(&resource, "c2", side_t::left, 2));
    auto function_expr = make_function_expression(&resource, std::string{"unknown_predicate"}, std::move(args));
    auto aggregate = make_aggregate_with_match_expr(&resource, function_expr);

    components::planner::optimizer_context_t context{&resource, nullptr, {}};
    components::planner::optimizer_pipeline_t(context).run_post_validate(aggregate);

    REQUIRE(aggregate->projected_cols().empty());
}

// ================================================================
// T1. Scalar folding: add
// ================================================================
TEST_CASE("optimizer::scalar_fold_add") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);

    auto result = components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    REQUIRE(std::holds_alternative<core::parameter_id_t>(s->params()[0]));
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 5);
}

// ================================================================
// T2. Scalar folding: subtract
// ================================================================
TEST_CASE("optimizer::scalar_fold_subtract") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::subtract);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 7);
}

// ================================================================
// T3. Scalar folding: multiply
// ================================================================
TEST_CASE("optimizer::scalar_fold_multiply") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(4));
    auto id1 = params->add_parameter(int64_t(5));

    auto scalar = make_scalar_expression(&resource, scalar_type::multiply);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 20);
}

// ================================================================
// T4. Scalar folding: divide
// ================================================================
TEST_CASE("optimizer::scalar_fold_divide") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::divide);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 3);
}

// ================================================================
// T5. Scalar folding: mod
// ================================================================
TEST_CASE("optimizer::scalar_fold_mod") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::mod);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 1);
}

// ================================================================
// T6. Compare folding: eq true
// ================================================================
TEST_CASE("optimizer::compare_fold_eq_true") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T7. Compare folding: eq false
// ================================================================
TEST_CASE("optimizer::compare_fold_eq_false") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(7));

    auto comp = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T8. Compare folding: gt true
// ================================================================
TEST_CASE("optimizer::compare_fold_gt_true") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9. Compare folding: lt false
// ================================================================
TEST_CASE("optimizer::compare_fold_lt_false") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9a. Compare folding: ne true
// ================================================================
TEST_CASE("optimizer::compare_fold_ne_true") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(7));

    auto comp = make_compare_expression(&resource, compare_type::ne, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9b. Compare folding: ne false
// ================================================================
TEST_CASE("optimizer::compare_fold_ne_false") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::ne, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9c. Compare folding: gte true (equal)
// ================================================================
TEST_CASE("optimizer::compare_fold_gte_true_equal") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9d. Compare folding: gte true (greater)
// ================================================================
TEST_CASE("optimizer::compare_fold_gte_true_greater") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9e. Compare folding: gte false
// ================================================================
TEST_CASE("optimizer::compare_fold_gte_false") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::gte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9f. Compare folding: lte true (equal)
// ================================================================
TEST_CASE("optimizer::compare_fold_lte_true_equal") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9g. Compare folding: lte true (less)
// ================================================================
TEST_CASE("optimizer::compare_fold_lte_true_less") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T9h. Compare folding: lte false
// ================================================================
TEST_CASE("optimizer::compare_fold_lte_false") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::lte, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_false);
}

// ================================================================
// T9i. Compare folding: lt true
// ================================================================
TEST_CASE("optimizer::compare_fold_lt_true") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(int64_t(10));

    auto comp = make_compare_expression(&resource, compare_type::lt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T10. No folding: key + param (mixed)
// ================================================================
TEST_CASE("optimizer::no_fold_key_param") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));

    auto comp = make_compare_expression(&resource, compare_type::eq, key(&resource, "field", side_t::left), id0);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::eq);
}

// ================================================================
// T11. No folding: NULL param
// ================================================================
TEST_CASE("optimizer::no_fold_null_param") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(components::types::logical_value_t{
        &resource,
        components::types::complex_logical_type{components::types::logical_type::NA}});
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 2);
}

// ================================================================
// T12. No folding: group node (skip non-match)
// ================================================================
TEST_CASE("optimizer::no_fold_group_node") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add, key(&resource, "result"));
    scalar->append_param(id0);
    scalar->append_param(id1);

    std::vector<expression_ptr> expressions;
    expressions.emplace_back(std::move(scalar));
    auto group_node =
        make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, expressions);

    components::planner::optimize(&resource, group_node, params.get());

    // Group expressions should NOT be folded
    auto* s = static_cast<scalar_expression_t*>(group_node->expressions()[0].get());
    REQUIRE(s->params().size() == 2);
}

// ================================================================
// T13. Nested folding: scalar inside compare
// ================================================================
TEST_CASE("optimizer::nested_scalar_in_compare") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    // Scalar should fold to 1 param = 5
    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 5);

    // Compare should stay eq (not folded since one side is key)
    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::eq);
}

// ================================================================
// T14. Division by zero: skip
// ================================================================
TEST_CASE("optimizer::div_by_zero_skip") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(0));

    auto scalar = make_scalar_expression(&resource, scalar_type::divide);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    // Division by zero may fold (returns 0 in otterbrix) or may throw — either is acceptable.
    // The optimizer handles both via try/catch.
    // We just verify no crash occurred and params are in a valid state.
    REQUIRE((s->params().size() == 1 || s->params().size() == 2));
}

// ================================================================
// T15. Union AND: children fold independently
// ================================================================
TEST_CASE("optimizer::union_and_fold") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));
    auto id2 = params->add_parameter(int64_t(10));

    auto child1 = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto child2 = make_compare_expression(&resource, compare_type::gt, key(&resource, "field", side_t::left), id2);

    auto union_and = make_compare_union_expression(&resource, compare_type::union_and);
    union_and->append_child(child1);
    union_and->append_child(child2);

    auto node = make_match_with_expr(&resource, union_and);
    components::planner::optimize(&resource, node, params.get());

    auto* c1 = static_cast<compare_expression_t*>(child1.get());
    REQUIRE(c1->type() == compare_type::all_true);

    auto* c2 = static_cast<compare_expression_t*>(child2.get());
    REQUIRE(c2->type() == compare_type::gt); // unchanged
}

// ================================================================
// T16. Union OR: children fold independently
// ================================================================
TEST_CASE("optimizer::union_or_fold") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(7));
    auto id2 = params->add_parameter(int64_t(10));
    auto id3 = params->add_parameter(int64_t(3));

    auto child1 = make_compare_expression(&resource, compare_type::eq, id0, id1);
    auto child2 = make_compare_expression(&resource, compare_type::gt, id2, id3);

    auto union_or = make_compare_union_expression(&resource, compare_type::union_or);
    union_or->append_child(child1);
    union_or->append_child(child2);

    auto node = make_match_with_expr(&resource, union_or);
    components::planner::optimize(&resource, node, params.get());

    auto* c1 = static_cast<compare_expression_t*>(child1.get());
    REQUIRE(c1->type() == compare_type::all_false);

    auto* c2 = static_cast<compare_expression_t*>(child2.get());
    REQUIRE(c2->type() == compare_type::all_true);
}

// ================================================================
// T17. Deep nested scalar: (2+3)*4
// ================================================================
TEST_CASE("optimizer::deep_nested_scalar") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));
    auto id2 = params->add_parameter(int64_t(4));

    auto inner = make_scalar_expression(&resource, scalar_type::add);
    inner->append_param(id0);
    inner->append_param(id1);

    auto outer = make_scalar_expression(&resource, scalar_type::multiply);
    outer->append_param(expression_ptr(inner));
    outer->append_param(id2);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(outer));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    // Inner folds: 2+3=5, outer folds: 5*4=20
    auto* s = static_cast<scalar_expression_t*>(outer.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 20);
}

// ================================================================
// T18. Triple nested: ((2+3)*4)+1
// ================================================================
TEST_CASE("optimizer::triple_nested_scalar") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));
    auto id2 = params->add_parameter(int64_t(4));
    auto id3 = params->add_parameter(int64_t(1));

    auto add_inner = make_scalar_expression(&resource, scalar_type::add);
    add_inner->append_param(id0);
    add_inner->append_param(id1);

    auto mul_mid = make_scalar_expression(&resource, scalar_type::multiply);
    mul_mid->append_param(expression_ptr(add_inner));
    mul_mid->append_param(id2);

    auto add_outer = make_scalar_expression(&resource, scalar_type::add);
    add_outer->append_param(expression_ptr(mul_mid));
    add_outer->append_param(id3);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(add_outer));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(add_outer.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<int64_t>() == 21);
}

// ================================================================
// T19. Scalar folding: double arithmetic
// ================================================================
TEST_CASE("optimizer::scalar_fold_double") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(double(2.5));
    auto id1 = params->add_parameter(double(1.5));

    auto scalar = make_scalar_expression(&resource, scalar_type::add);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<double>() == Approx(4.0));
}

// ================================================================
// T20. Scalar folding: mixed int * double
// ================================================================
TEST_CASE("optimizer::scalar_fold_mixed_types") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(3));
    auto id1 = params->add_parameter(double(2.5));

    auto scalar = make_scalar_expression(&resource, scalar_type::multiply);
    scalar->append_param(id0);
    scalar->append_param(id1);

    auto comp = make_compare_expression(&resource,
                                        compare_type::eq,
                                        key(&resource, "field", side_t::left),
                                        expression_ptr(scalar));
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* s = static_cast<scalar_expression_t*>(scalar.get());
    REQUIRE(s->params().size() == 1);
    auto new_id = std::get<core::parameter_id_t>(s->params()[0]);
    REQUIRE(params->parameter(new_id).value<double>() == Approx(7.5));
}

// ================================================================
// T21. Compare folding: double comparison
// ================================================================
TEST_CASE("optimizer::compare_fold_double") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(double(3.14));
    auto id1 = params->add_parameter(double(2.71));

    auto comp = make_compare_expression(&resource, compare_type::gt, id0, id1);
    auto node = make_match_with_expr(&resource, comp);
    components::planner::optimize(&resource, node, params.get());

    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);
}

// ================================================================
// T22. Aggregate pipeline: match → group → sort (match folds, group/sort untouched)
// ================================================================
TEST_CASE("optimizer::aggregate_match_folds_group_not") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(5));
    auto id1 = params->add_parameter(int64_t(5));
    auto id2 = params->add_parameter(int64_t(2));
    auto id3 = params->add_parameter(int64_t(3));

    auto aggregate = make_node_aggregate(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});

    // Child 0: match(eq, #0=5, #1=5)
    auto comp = make_compare_expression(&resource, compare_type::eq, id0, id1);
    aggregate->append_child(
        make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, comp));

    // Child 1: group with scalar(add, #2=2, #3=3)
    auto scalar = make_scalar_expression(&resource, scalar_type::add, key(&resource, "result"));
    scalar->append_param(id2);
    scalar->append_param(id3);
    std::vector<expression_ptr> group_exprs;
    group_exprs.emplace_back(std::move(scalar));
    aggregate->append_child(
        make_node_group(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, group_exprs));

    components::planner::optimize(&resource, aggregate, params.get());

    // Match should fold to all_true
    auto* c = static_cast<compare_expression_t*>(comp.get());
    REQUIRE(c->type() == compare_type::all_true);

    // Group scalar should NOT fold (stays 2 params)
    auto* gs = static_cast<scalar_expression_t*>(aggregate->children()[1]->expressions()[0].get());
    REQUIRE(gs->params().size() == 2);
}

// ================================================================
// T23. Multiple match nodes in aggregate
// ================================================================
TEST_CASE("optimizer::multiple_match_nodes") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(10));
    auto id1 = params->add_parameter(int64_t(5));
    auto id2 = params->add_parameter(int64_t(3));
    auto id3 = params->add_parameter(int64_t(10));

    auto aggregate = make_node_aggregate(&resource, core::dbname_t{database_name}, core::relname_t{collection_name});

    auto comp1 = make_compare_expression(&resource, compare_type::gt, id0, id1);
    aggregate->append_child(
        make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, comp1));

    auto comp2 = make_compare_expression(&resource, compare_type::lt, id2, id3);
    aggregate->append_child(
        make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, comp2));

    components::planner::optimize(&resource, aggregate, params.get());

    auto* c1 = static_cast<compare_expression_t*>(comp1.get());
    REQUIRE(c1->type() == compare_type::all_true);

    auto* c2 = static_cast<compare_expression_t*>(comp2.get());
    REQUIRE(c2->type() == compare_type::all_true);
}

// ================================================================
// T24. mirror_compare: lt ↔ gt
// ================================================================
TEST_CASE("optimizer::mirror_compare_lt_gt") {
    using namespace services::planner::impl;
    REQUIRE(mirror_compare(compare_type::lt) == compare_type::gt);
    REQUIRE(mirror_compare(compare_type::gt) == compare_type::lt);
}

// ================================================================
// T25. mirror_compare: lte ↔ gte
// ================================================================
TEST_CASE("optimizer::mirror_compare_lte_gte") {
    using namespace services::planner::impl;
    REQUIRE(mirror_compare(compare_type::lte) == compare_type::gte);
    REQUIRE(mirror_compare(compare_type::gte) == compare_type::lte);
}

// ================================================================
// T26. mirror_compare: eq/ne symmetric
// ================================================================
TEST_CASE("optimizer::mirror_compare_symmetric") {
    using namespace services::planner::impl;
    REQUIRE(mirror_compare(compare_type::eq) == compare_type::eq);
    REQUIRE(mirror_compare(compare_type::ne) == compare_type::ne);
}

// ================================================================
// T27. has_index_on: positive (single-field)
// ================================================================
TEST_CASE("optimizer::has_index_on_positive") {
    auto resource = std::pmr::synchronized_pool_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "age"));
    ctx.indexed_keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(key(&resource, "age")) == true);
}

// ================================================================
// T28. has_index_on: negative (no match)
// ================================================================
TEST_CASE("optimizer::has_index_on_negative") {
    auto resource = std::pmr::synchronized_pool_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "age"));
    ctx.indexed_keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(key(&resource, "name")) == false);
}

// ================================================================
// T29. has_index_on: multi-field index skip
// ================================================================
TEST_CASE("optimizer::has_index_on_multi_field_skip") {
    auto resource = std::pmr::synchronized_pool_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});

    components::logical_plan::keys_base_storage_t keys(&resource);
    keys.push_back(key(&resource, "a"));
    keys.push_back(key(&resource, "b"));
    ctx.indexed_keys.push_back(std::move(keys));

    REQUIRE(ctx.has_index_on(key(&resource, "a")) == false);
}

// ================================================================
// T30. has_index_on: empty indexed_keys
// ================================================================
TEST_CASE("optimizer::has_index_on_empty") {
    auto resource = std::pmr::synchronized_pool_resource();
    services::context_storage_t ctx(&resource, log_t{}, core::date::timezone_offset_t{});

    REQUIRE(ctx.has_index_on(key(&resource, "any")) == false);
}

// ================================================================
// Diagnostic: parameter copy chain
// ================================================================
TEST_CASE("optimizer::param_copy_survives") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto id0 = params->add_parameter(int64_t(2));
    auto id1 = params->add_parameter(int64_t(3));

    // Overwrite id0 with 5 (like optimizer does)
    params->set_parameter(id0, components::types::logical_value_t(&resource, int64_t(5)));
    REQUIRE(params->parameter(id0).value<int64_t>() == 5);

    // take_parameters (like dispatcher does)
    auto taken = params->take_parameters();
    REQUIRE(taken.parameters.count(id0) == 1);
    REQUIRE(taken.parameters.at(id0).value<int64_t>() == 5);
    REQUIRE(taken.parameters.count(id1) == 1);

    // Copy (like actor message chain does)
    storage_parameters copy1 = taken;
    REQUIRE(copy1.parameters.count(id0) == 1);
    REQUIRE(copy1.parameters.at(id0).value<int64_t>() == 5);

    // Another copy
    storage_parameters copy2 = copy1;
    REQUIRE(copy2.parameters.count(id0) == 1);
    REQUIRE(copy2.parameters.at(id0).value<int64_t>() == 5);

    // Copy via move
    storage_parameters moved = std::move(copy2);
    REQUIRE(moved.parameters.count(id0) == 1);
    REQUIRE(moved.parameters.at(id0).value<int64_t>() == 5);
}

static services::context_storage_t make_context_with_oid(std::pmr::memory_resource* resource,
                                                         components::catalog::oid_t oid,
                                                         const components::logical_plan::storage_parameters* params) {
    services::context_storage_t ctx(resource, log_t{}, {});
    ctx.known_oids.insert(oid);
    ctx.parameters = params;
    return ctx;
}

static services::context_storage_t make_context_with_oid(std::pmr::memory_resource* resource,
                                                         components::catalog::oid_t oid,
                                                         const components::logical_plan::parameter_node_t* params) {
    return make_context_with_oid(resource, oid, params ? &params->parameters() : nullptr);
}

static void add_single_field_index(services::context_storage_t& ctx,
                                   std::pmr::memory_resource* resource,
                                   const char* field,
                                   components::logical_plan::index_type type) {
    components::logical_plan::keys_base_storage_t keys(resource);
    keys.push_back(key(resource, field));
    ctx.indexed_keys.push_back(keys);

    components::index::index_description_t desc{
        components::logical_plan::keys_base_storage_t(resource),
        type,
    };
    desc.keys.push_back(key(resource, field));
    ctx.indexed_descriptions.push_back(std::move(desc));
}

TEST_CASE("create_plan_match::eq_uses_index_scan_hashed_preferred") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(42));
    constexpr auto table_oid = components::catalog::oid_t{777};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, "age", components::logical_plan::index_type::hashed);

    auto node = make_node_match(&resource,
                                core::dbname_t{database_name},
                                core::relname_t{collection_name},
                                make_compare_expression(&resource, compare_type::eq, key(&resource, "age"), pid));
    node->set_table_oid(table_oid);

    auto op = services::planner::impl::create_plan_match(ctx, node, components::logical_plan::limit_t::unlimit());
    REQUIRE(op->type() == components::operators::operator_type::index_scan);
    auto* scan = static_cast<components::operators::index_scan*>(op.get());
    REQUIRE(scan->compare_type() == compare_type::eq);
    REQUIRE(scan->preferred_index_type() == components::logical_plan::index_type::hashed);
}

TEST_CASE("create_plan_match::range_uses_index_scan_single_preferred") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(30));
    constexpr auto table_oid = components::catalog::oid_t{778};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, "age", components::logical_plan::index_type::single);

    auto node = make_node_match(&resource,
                                core::dbname_t{database_name},
                                core::relname_t{collection_name},
                                make_compare_expression(&resource, compare_type::gte, key(&resource, "age"), pid));
    node->set_table_oid(table_oid);

    auto op = services::planner::impl::create_plan_match(ctx, node, components::logical_plan::limit_t::unlimit());
    REQUIRE(op->type() == components::operators::operator_type::index_scan);
    auto* scan = static_cast<components::operators::index_scan*>(op.get());
    REQUIRE(scan->compare_type() == compare_type::gte);
    REQUIRE(scan->preferred_index_type() == components::logical_plan::index_type::single);
}

TEST_CASE("create_plan_match::range_with_only_hashed_falls_back_to_full_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(30));
    constexpr auto table_oid = components::catalog::oid_t{779};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, "age", components::logical_plan::index_type::hashed);

    auto node = make_node_match(&resource,
                                core::dbname_t{database_name},
                                core::relname_t{collection_name},
                                make_compare_expression(&resource, compare_type::gt, key(&resource, "age"), pid));
    node->set_table_oid(table_oid);

    auto op = services::planner::impl::create_plan_match(ctx, node, components::logical_plan::limit_t::unlimit());
    REQUIRE(op->type() == components::operators::operator_type::full_scan);
}

TEST_CASE("create_plan_match::key_on_right_mirrors_compare_type_for_index_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(30));
    constexpr auto table_oid = components::catalog::oid_t{780};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, "age", components::logical_plan::index_type::single);

    auto node = make_node_match(&resource,
                                core::dbname_t{database_name},
                                core::relname_t{collection_name},
                                make_compare_expression(&resource, compare_type::lt, pid, key(&resource, "age")));
    node->set_table_oid(table_oid);

    auto op = services::planner::impl::create_plan_match(ctx, node, components::logical_plan::limit_t::unlimit());
    REQUIRE(op->type() == components::operators::operator_type::index_scan);
    auto* scan = static_cast<components::operators::index_scan*>(op.get());
    REQUIRE(scan->compare_type() == compare_type::gt);
}

TEST_CASE("create_plan_match::union_compare_uses_full_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto params = make_parameter_node(&resource);
    auto pid = params->add_parameter(int64_t(30));
    constexpr auto table_oid = components::catalog::oid_t{781};

    auto ctx = make_context_with_oid(&resource, table_oid, params.get());
    add_single_field_index(ctx, &resource, "age", components::logical_plan::index_type::single);

    auto union_expr = make_compare_union_expression(&resource, compare_type::union_and);
    union_expr->append_child(make_compare_expression(&resource, compare_type::gte, key(&resource, "age"), pid));

    auto node =
        make_node_match(&resource, core::dbname_t{database_name}, core::relname_t{collection_name}, union_expr);
    node->set_table_oid(table_oid);

    auto op = services::planner::impl::create_plan_match(ctx, node, components::logical_plan::limit_t::unlimit());
    REQUIRE(op->type() == components::operators::operator_type::match);
    REQUIRE(op->left() != nullptr);
    REQUIRE(op->left()->type() == components::operators::operator_type::full_scan);
}
