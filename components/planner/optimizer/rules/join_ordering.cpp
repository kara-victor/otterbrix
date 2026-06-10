#include "join_ordering.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>

namespace components::planner::optimizer {

    namespace {
        namespace expr = components::expressions;
        namespace lp = components::logical_plan;
        using oid_t = components::catalog::oid_t;

        struct relation_t {
            lp::node_ptr node;
            oid_t oid{components::catalog::INVALID_OID};
            std::string alias;
            std::vector<std::string> columns;
            double rows{0.0};
            size_t original_index{0};
        };

        struct edge_t {
            size_t first{0};
            size_t second{0};
            size_t first_col{0};
            size_t second_col{0};
            expr::key_t first_key;
            expr::key_t second_key;
            size_t original_index{0};

            explicit edge_t(std::pmr::memory_resource* resource)
                : first_key(resource)
                , second_key(resource) {}
        };

        struct key_update_t {
            expr::key_t* key;
            size_t new_index;
        };

        double saturated_mul(double lhs, double rhs) {
            if (lhs <= 0.0 || rhs <= 0.0) {
                return 0.0;
            }
            const double max = std::numeric_limits<double>::max();
            return lhs > max / rhs ? max : lhs * rhs;
        }

        double saturated_add(double lhs, double rhs) {
            const double max = std::numeric_limits<double>::max();
            return lhs > max - rhs ? max : lhs + rhs;
        }

        double join_rows(double lhs, double rhs, const join_cost_model_t& model) {
            return std::max(1.0, saturated_mul(saturated_mul(lhs, rhs), model.equality_join_selectivity));
        }

        double join_cost(double lhs, double rhs, const join_cost_model_t& model) {
            return saturated_add(saturated_add(lhs, rhs), join_rows(lhs, rhs, model));
        }

        bool is_range(expr::compare_type type) {
            return type == expr::compare_type::lt || type == expr::compare_type::lte ||
                   type == expr::compare_type::gt || type == expr::compare_type::gte;
        }

        double filter_selectivity(const expr::expression_ptr& expression, const join_cost_model_t& model) {
            if (!expression || expression->group() != expr::expression_group::compare) {
                return 1.0;
            }
            const auto* compare = static_cast<const expr::compare_expression_t*>(expression.get());
            if (compare->type() == expr::compare_type::union_and) {
                double result = 1.0;
                for (const auto& child : compare->children()) {
                    result *= filter_selectivity(child, model);
                }
                return result;
            }
            const bool key_param =
                (std::holds_alternative<expr::key_t>(compare->left()) &&
                 std::holds_alternative<core::parameter_id_t>(compare->right())) ||
                (std::holds_alternative<core::parameter_id_t>(compare->left()) &&
                 std::holds_alternative<expr::key_t>(compare->right()));
            if (!key_param) {
                return 1.0;
            }
            if (compare->type() == expr::compare_type::eq) {
                return model.equality_filter_selectivity;
            }
            return is_range(compare->type()) ? model.range_filter_selectivity : 1.0;
        }

        double estimate_leaf_rows(const lp::node_ptr& leaf, std::uint64_t row_count, const join_cost_model_t& model) {
            double selectivity = 1.0;
            for (const auto& child : leaf->children()) {
                if (child && child->type() == lp::node_type::match_t && !child->expressions().empty()) {
                    selectivity *= filter_selectivity(child->expressions().front(), model);
                }
            }
            const double rows = static_cast<double>(row_count);
            return std::clamp(rows * selectivity, row_count == 0 ? 0.0 : 1.0, rows);
        }

        using metadata_map_t = std::unordered_map<oid_t, const lp::resolved_table_metadata_t*>;

        metadata_map_t collect_metadata(const lp::node_ptr& root) {
            metadata_map_t result;
            std::vector<lp::node_ptr> stack{root};
            while (!stack.empty()) {
                auto node = std::move(stack.back());
                stack.pop_back();
                if (node->type() == lp::node_type::catalog_resolve_table_t) {
                    const auto* resolve = static_cast<const lp::node_catalog_resolve_table_t*>(node.get());
                    if (resolve->resolved_metadata()) {
                        result[resolve->resolved_metadata()->table_oid] = &*resolve->resolved_metadata();
                    }
                }
                for (const auto& child : node->children()) {
                    if (child) {
                        stack.push_back(child);
                    }
                }
            }
            return result;
        }

        bool is_base_leaf(const lp::node_ptr& node) {
            if (!node || node->type() != lp::node_type::aggregate_t ||
                node->table_oid() == components::catalog::INVALID_OID) {
                return false;
            }
            for (const auto& child : node->children()) {
                if (child && (child->type() == lp::node_type::join_t || child->type() == lp::node_type::hash_join_t ||
                              child->type() == lp::node_type::aggregate_t)) {
                    return false;
                }
            }
            return true;
        }

        bool flatten_left_deep(const lp::node_ptr& node,
                               std::vector<lp::node_ptr>& leaves,
                               std::vector<expr::expression_ptr>& predicates) {
            if (is_base_leaf(node)) {
                leaves.push_back(node);
                return true;
            }
            if (!node || node->type() != lp::node_type::join_t || node->children().size() != 2 ||
                node->expressions().size() != 1) {
                return false;
            }
            const auto* join = static_cast<const lp::node_join_t*>(node.get());
            const auto& predicate = node->expressions().front();
            if (join->type() != lp::join_type::inner || !predicate ||
                predicate->group() != expr::expression_group::compare) {
                return false;
            }
            const auto* compare = static_cast<const expr::compare_expression_t*>(predicate.get());
            if (compare->type() != expr::compare_type::eq ||
                !std::holds_alternative<expr::key_t>(compare->left()) ||
                !std::holds_alternative<expr::key_t>(compare->right()) || !is_base_leaf(node->children()[1])) {
                return false;
            }
            if (!flatten_left_deep(node->children()[0], leaves, predicates)) {
                return false;
            }
            leaves.push_back(node->children()[1]);
            predicates.push_back(predicate);
            return true;
        }

        std::optional<size_t> relation_for_key(const expr::key_t& key, const std::vector<relation_t>& relations) {
            if (key.storage().size() < 2 || key.storage().back() == "*") {
                return std::nullopt;
            }
            std::optional<size_t> match;
            for (size_t i = 0; i < relations.size(); ++i) {
                for (size_t part = 0; part + 1 < key.storage().size(); ++part) {
                    if (key.storage()[part].compare(relations[i].alias) == 0) {
                        if (match && *match != i) {
                            return std::nullopt;
                        }
                        match = i;
                    }
                }
            }
            return match;
        }

        std::optional<size_t> column_for_key(const expr::key_t& key, const relation_t& relation) {
            const auto& name = key.storage().back();
            for (size_t i = 0; i < relation.columns.size(); ++i) {
                if (name.compare(relation.columns[i]) == 0) {
                    return i;
                }
            }
            return std::nullopt;
        }

        std::optional<edge_t> make_edge(std::pmr::memory_resource* resource,
                                        const expr::expression_ptr& predicate,
                                        const std::vector<relation_t>& relations,
                                        size_t original_index) {
            const auto* compare = static_cast<const expr::compare_expression_t*>(predicate.get());
            const auto& left_key = std::get<expr::key_t>(compare->left());
            const auto& right_key = std::get<expr::key_t>(compare->right());
            auto left_rel = relation_for_key(left_key, relations);
            auto right_rel = relation_for_key(right_key, relations);
            if (!left_rel || !right_rel || *left_rel == *right_rel) {
                return std::nullopt;
            }
            auto left_col = column_for_key(left_key, relations[*left_rel]);
            auto right_col = column_for_key(right_key, relations[*right_rel]);
            if (!left_col || !right_col) {
                return std::nullopt;
            }
            edge_t edge(resource);
            edge.first = *left_rel;
            edge.second = *right_rel;
            edge.first_col = *left_col;
            edge.second_col = *right_col;
            edge.first_key = left_key;
            edge.second_key = right_key;
            edge.original_index = original_index;
            return edge;
        }

        const edge_t* connecting_edge(const std::vector<edge_t>& edges,
                                      const std::unordered_set<size_t>& joined,
                                      size_t outside) {
            for (const auto& edge : edges) {
                if ((edge.first == outside && joined.count(edge.second)) ||
                    (edge.second == outside && joined.count(edge.first))) {
                    return &edge;
                }
            }
            return nullptr;
        }

        bool key_is_wildcard(const expr::key_t& key) {
            return (!key.storage().empty() && key.storage().back() == "*") ||
                   (!key.path().empty() && key.path().front() == SIZE_MAX);
        }

        bool collect_key_update(expr::key_t& key,
                                const std::vector<relation_t>& relations,
                                const std::vector<size_t>& offsets,
                                std::vector<key_update_t>& updates) {
            if (key_is_wildcard(key)) {
                return false;
            }
            if (key.path().empty()) {
                return true;
            }
            auto relation = relation_for_key(key, relations);
            if (!relation) {
                return false;
            }
            auto column = column_for_key(key, relations[*relation]);
            if (!column) {
                return false;
            }
            updates.push_back({&key, offsets[*relation] + *column});
            return true;
        }

        bool collect_expression_updates(const expr::expression_ptr& expression,
                                        const std::vector<relation_t>& relations,
                                        const std::vector<size_t>& offsets,
                                        std::vector<key_update_t>& updates);

        bool collect_param_updates(expr::param_storage& param,
                                   const std::vector<relation_t>& relations,
                                   const std::vector<size_t>& offsets,
                                   std::vector<key_update_t>& updates) {
            if (std::holds_alternative<expr::key_t>(param)) {
                return collect_key_update(std::get<expr::key_t>(param), relations, offsets, updates);
            }
            if (std::holds_alternative<expr::expression_ptr>(param)) {
                return collect_expression_updates(std::get<expr::expression_ptr>(param), relations, offsets, updates);
            }
            return true;
        }

        bool collect_expression_updates(const expr::expression_ptr& expression,
                                        const std::vector<relation_t>& relations,
                                        const std::vector<size_t>& offsets,
                                        std::vector<key_update_t>& updates) {
            if (!expression) {
                return true;
            }
            switch (expression->group()) {
                case expr::expression_group::compare: {
                    auto* compare = static_cast<expr::compare_expression_t*>(expression.get());
                    if (!collect_param_updates(compare->left(), relations, offsets, updates) ||
                        !collect_param_updates(compare->right(), relations, offsets, updates)) {
                        return false;
                    }
                    for (const auto& child : compare->children()) {
                        if (!collect_expression_updates(child, relations, offsets, updates)) {
                            return false;
                        }
                    }
                    return true;
                }
                case expr::expression_group::scalar: {
                    auto* scalar = static_cast<expr::scalar_expression_t*>(expression.get());
                    if (scalar->type() == expr::scalar_type::star_expand ||
                        !collect_key_update(scalar->key(), relations, offsets, updates)) {
                        return false;
                    }
                    for (auto& param : scalar->params()) {
                        if (!collect_param_updates(param, relations, offsets, updates)) {
                            return false;
                        }
                    }
                    return true;
                }
                case expr::expression_group::aggregate: {
                    auto* aggregate = static_cast<expr::aggregate_expression_t*>(expression.get());
                    for (auto& param : aggregate->params()) {
                        if (!collect_param_updates(param, relations, offsets, updates)) {
                            return false;
                        }
                    }
                    return true;
                }
                case expr::expression_group::sort:
                    return collect_key_update(static_cast<expr::sort_expression_t*>(expression.get())->key(),
                                              relations,
                                              offsets,
                                              updates);
                case expr::expression_group::function: {
                    auto* function = static_cast<expr::function_expression_t*>(expression.get());
                    for (auto& arg : function->args()) {
                        if (!collect_param_updates(arg, relations, offsets, updates)) {
                            return false;
                        }
                    }
                    return true;
                }
                default:
                    return false;
            }
        }

        bool collect_parent_updates(const lp::node_ptr& aggregate,
                                    const lp::node_ptr& join_root,
                                    const std::vector<relation_t>& relations,
                                    const std::vector<size_t>& offsets,
                                    std::vector<key_update_t>& updates) {
            std::vector<lp::node_ptr> stack{aggregate};
            while (!stack.empty()) {
                auto node = std::move(stack.back());
                stack.pop_back();
                if (node == join_root) {
                    continue;
                }
                for (const auto& expression : node->expressions()) {
                    if (!collect_expression_updates(expression, relations, offsets, updates)) {
                        return false;
                    }
                }
                for (const auto& child : node->children()) {
                    if (child && child != join_root && child->type() != lp::node_type::aggregate_t) {
                        stack.push_back(child);
                    }
                }
            }
            return true;
        }

        expr::key_t positioned_key(std::pmr::memory_resource* resource,
                                   const expr::key_t& source,
                                   expr::side_t side,
                                   size_t index) {
            expr::key_t key = source;
            key.set_side(side);
            std::pmr::vector<size_t> path{resource};
            path.push_back(index);
            key.set_path(std::move(path));
            return key;
        }

        lp::node_ptr make_join(std::pmr::memory_resource* resource,
                               const lp::node_ptr& left,
                               const lp::node_ptr& right,
                               const edge_t& edge,
                               const std::vector<size_t>& current_order,
                               const std::vector<relation_t>& relations) {
            size_t left_relation = edge.first;
            size_t left_col = edge.first_col;
            size_t right_col = edge.second_col;
            const expr::key_t* left_key = &edge.first_key;
            const expr::key_t* right_key = &edge.second_key;
            if (std::find(current_order.begin(), current_order.end(), edge.second) != current_order.end()) {
                left_relation = edge.second;
                left_col = edge.second_col;
                right_col = edge.first_col;
                left_key = &edge.second_key;
                right_key = &edge.first_key;
            }
            size_t offset = 0;
            for (auto relation : current_order) {
                if (relation == left_relation) {
                    break;
                }
                offset += relations[relation].columns.size();
            }
            auto predicate = expr::make_compare_expression(resource,
                                                           expr::compare_type::eq,
                                                           positioned_key(resource, *left_key, expr::side_t::left, offset + left_col),
                                                           positioned_key(resource, *right_key, expr::side_t::right, right_col));
            auto join = lp::make_node_join(resource, core::dbname_t{}, core::relname_t{}, lp::join_type::inner);
            join->append_child(left);
            join->append_child(right);
            join->append_expression(predicate);
            return join;
        }

        bool try_reorder_aggregate(const lp::node_ptr& aggregate,
                                   const metadata_map_t& metadata,
                                   const table_statistics_map_t& statistics,
                                   const join_cost_model_t& model,
                                   const std::vector<std::string>* forced_order) {
            if (!aggregate || aggregate->type() != lp::node_type::aggregate_t) {
                return false;
            }
            lp::node_ptr join_root;
            for (const auto& child : aggregate->children()) {
                if (child && child->type() == lp::node_type::join_t) {
                    if (join_root) {
                        return false;
                    }
                    join_root = child;
                }
            }
            if (!join_root) {
                return false;
            }

            std::vector<lp::node_ptr> leaves;
            std::vector<expr::expression_ptr> predicates;
            if (!flatten_left_deep(join_root, leaves, predicates) || leaves.size() < 2 || predicates.size() + 1 != leaves.size()) {
                return false;
            }

            std::vector<relation_t> relations;
            relations.reserve(leaves.size());
            std::unordered_set<std::string> aliases;
            for (size_t i = 0; i < leaves.size(); ++i) {
                const auto oid = leaves[i]->table_oid();
                auto stat = statistics.find(oid);
                auto md = metadata.find(oid);
                if (md == metadata.end() || !md->second ||
                    (!forced_order && (stat == statistics.end() || !stat->second.row_count))) {
                    return false;
                }
                const auto* leaf = static_cast<const lp::node_aggregate_t*>(leaves[i].get());
                std::string alias = leaves[i]->result_alias().empty()
                                        ? static_cast<const std::string&>(leaf->relname())
                                        : leaves[i]->result_alias();
                if (alias.empty() || !aliases.insert(alias).second) {
                    return false;
                }
                relation_t relation;
                relation.node = leaves[i];
                relation.oid = oid;
                relation.alias = std::move(alias);
                relation.original_index = i;
                relation.rows = stat != statistics.end() && stat->second.row_count
                                    ? estimate_leaf_rows(leaves[i], *stat->second.row_count, model)
                                    : 1.0;
                for (const auto& column : md->second->columns) {
                    relation.columns.push_back(column.attname);
                }
                if (relation.columns.empty()) {
                    return false;
                }
                relations.push_back(std::move(relation));
            }

            std::vector<edge_t> edges;
            edges.reserve(predicates.size());
            for (size_t i = 0; i < predicates.size(); ++i) {
                auto edge = make_edge(aggregate->resource(), predicates[i], relations, i);
                if (!edge) {
                    return false;
                }
                edges.push_back(std::move(*edge));
            }

            std::vector<size_t> order;
            std::vector<const edge_t*> selected_edges;
            double current_rows = 0.0;
            if (forced_order) {
                if (forced_order->size() != relations.size()) {
                    return false;
                }
                std::unordered_map<std::string, size_t> by_alias;
                for (size_t i = 0; i < relations.size(); ++i) {
                    by_alias.emplace(relations[i].alias, i);
                }
                std::unordered_set<size_t> joined;
                for (const auto& alias : *forced_order) {
                    auto it = by_alias.find(alias);
                    if (it == by_alias.end() || !joined.insert(it->second).second) {
                        return false;
                    }
                    order.push_back(it->second);
                    if (order.size() == 1) {
                        current_rows = relations[it->second].rows;
                        continue;
                    }
                    // connecting_edge expects the new relation to be outside.
                    joined.erase(it->second);
                    const auto* edge = connecting_edge(edges, joined, it->second);
                    joined.insert(it->second);
                    if (!edge) {
                        return false;
                    }
                    selected_edges.push_back(edge);
                    current_rows = join_rows(current_rows, relations[it->second].rows, model);
                }
            } else {
                const edge_t* best_edge = nullptr;
                double best_cost = std::numeric_limits<double>::max();
                for (const auto& edge : edges) {
                    const double cost = join_cost(relations[edge.first].rows, relations[edge.second].rows, model);
                    if (!best_edge || cost < best_cost ||
                        (std::abs(cost - best_cost) <= 1e-12 && edge.original_index < best_edge->original_index)) {
                        best_edge = &edge;
                        best_cost = cost;
                    }
                }
                if (!best_edge) {
                    return false;
                }

                if (relations[best_edge->first].rows < relations[best_edge->second].rows) {
                    order = {best_edge->second, best_edge->first};
                } else {
                    order = {best_edge->first, best_edge->second};
                }
                selected_edges = {best_edge};
                std::unordered_set<size_t> joined{order[0], order[1]};
                current_rows = join_rows(relations[order[0]].rows, relations[order[1]].rows, model);

                while (order.size() < relations.size()) {
                    size_t best_relation = SIZE_MAX;
                    const edge_t* edge = nullptr;
                    double incremental = std::numeric_limits<double>::max();
                    for (size_t i = 0; i < relations.size(); ++i) {
                        if (joined.count(i)) {
                            continue;
                        }
                        const auto* candidate_edge = connecting_edge(edges, joined, i);
                        if (!candidate_edge) {
                            continue;
                        }
                        const double candidate = join_cost(current_rows, relations[i].rows, model);
                        if (best_relation == SIZE_MAX || candidate < incremental ||
                            (std::abs(candidate - incremental) <= 1e-12 &&
                             relations[i].original_index < relations[best_relation].original_index)) {
                            best_relation = i;
                            edge = candidate_edge;
                            incremental = candidate;
                        }
                    }
                    if (best_relation == SIZE_MAX || !edge) {
                        return false;
                    }
                    order.push_back(best_relation);
                    selected_edges.push_back(edge);
                    joined.insert(best_relation);
                    current_rows = join_rows(current_rows, relations[best_relation].rows, model);
                }
            }

            bool unchanged = true;
            for (size_t i = 0; i < order.size(); ++i) {
                unchanged = unchanged && order[i] == i;
            }
            if (unchanged) {
                return forced_order != nullptr;
            }

            std::vector<size_t> new_offsets(relations.size(), 0);
            size_t running = 0;
            for (auto relation : order) {
                new_offsets[relation] = running;
                running += relations[relation].columns.size();
            }
            std::vector<key_update_t> updates;
            if (!collect_parent_updates(aggregate, join_root, relations, new_offsets, updates)) {
                return false;
            }

            std::vector<size_t> current_order{order[0]};
            lp::node_ptr new_root = relations[order[0]].node;
            for (size_t i = 1; i < order.size(); ++i) {
                new_root = make_join(aggregate->resource(),
                                     new_root,
                                     relations[order[i]].node,
                                     *selected_edges[i - 1],
                                     current_order,
                                     relations);
                current_order.push_back(order[i]);
            }

            for (auto& update : updates) {
                auto path = update.key->path();
                path[0] = update.new_index;
                update.key->set_path(std::move(path));
            }
            for (auto& child : aggregate->children()) {
                if (child == join_root) {
                    child = new_root;
                    break;
                }
            }
            return true;
        }

        bool walk(const lp::node_ptr& node,
                  const metadata_map_t& metadata,
                  const table_statistics_map_t& statistics,
                  const join_cost_model_t& model,
                  const std::vector<std::string>* forced_order) {
            if (!node) {
                return false;
            }
            bool changed = try_reorder_aggregate(node, metadata, statistics, model, forced_order);
            for (const auto& child : node->children()) {
                changed = walk(child, metadata, statistics, model, forced_order) || changed;
            }
            return changed;
        }
    } // namespace

    bool force_join_order(const logical_plan::node_ptr& root, const std::vector<std::string>& leading_order) {
        if (!root || leading_order.empty()) {
            return false;
        }
        const table_statistics_map_t no_statistics;
        return walk(root, collect_metadata(root), no_statistics, join_cost_model_t{}, &leading_order);
    }

    bool order_joins(const logical_plan::node_ptr& root,
                     const table_statistics_map_t& statistics,
                     const join_cost_model_t& cost_model) {
        if (!root || statistics.empty()) {
            return false;
        }
        return walk(root, collect_metadata(root), statistics, cost_model, nullptr);
    }

} // namespace components::planner::optimizer
