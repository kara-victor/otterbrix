#include "filter_pushdown.hpp"

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <vector>

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>

namespace components::planner::optimizer {

    namespace {

        using expressions::compare_expression_ptr;
        using expressions::compare_type;
        using expressions::expression_group;
        using expressions::expression_ptr;
        using expressions::key_t;
        using expressions::param_storage;
        using expressions::side_t;
        using logical_plan::node_aggregate_t;
        using logical_plan::node_catalog_resolve_table_t;
        using logical_plan::node_join_t;
        using logical_plan::node_ptr;
        using logical_plan::node_type;

        using table_cols_map = std::unordered_map<components::catalog::oid_t, size_t>;

        void collect_table_md(const node_ptr& root, table_cols_map& out) {
            if (!root) {
                return;
            }
            std::vector<node_ptr> stack{root};
            while (!stack.empty()) {
                auto current = std::move(stack.back());
                stack.pop_back();
                if (current->type() == node_type::catalog_resolve_table_t) {
                    const auto* resolve = static_cast<const node_catalog_resolve_table_t*>(current.get());
                    const auto& md = resolve->resolved_metadata();
                    if (md && md->table_oid != components::catalog::INVALID_OID) {
                        out[md->table_oid] = md->columns.size();
                    }
                }
                for (const auto& child : current->children()) {
                    stack.push_back(child);
                }
            }
        }

        enum class predicate_side
        {
            none,
            left,
            right,
            mixed,
            unsupported
        };

        predicate_side merge_side(predicate_side a, predicate_side b) {
            if (a == predicate_side::unsupported || b == predicate_side::unsupported) {
                return predicate_side::unsupported;
            }
            if (a == predicate_side::mixed || b == predicate_side::mixed) {
                return predicate_side::mixed;
            }
            if (a == predicate_side::none) {
                return b;
            }
            if (b == predicate_side::none || a == b) {
                return a;
            }
            return predicate_side::mixed;
        }

        predicate_side side_from_key(const key_t& key) {
            if (key.side() == side_t::left) {
                return predicate_side::left;
            }
            if (key.side() == side_t::right) {
                return predicate_side::right;
            }
            return predicate_side::unsupported;
        }

        predicate_side referenced_side(const param_storage& param) {
            if (std::holds_alternative<key_t>(param)) {
                return side_from_key(std::get<key_t>(param));
            }
            if (std::holds_alternative<core::parameter_id_t>(param)) {
                return predicate_side::none;
            }
            return predicate_side::unsupported;
        }

        predicate_side referenced_side(const expression_ptr& expr) {
            if (!expr || expr->group() != expression_group::compare) {
                return predicate_side::unsupported;
            }
            const auto& comp = reinterpret_cast<const compare_expression_ptr&>(expr);
            if (comp->type() == compare_type::union_or || comp->type() == compare_type::union_not) {
                return predicate_side::unsupported;
            }
            if (comp->type() == compare_type::union_and) {
                predicate_side side = predicate_side::none;
                for (const auto& child : comp->children()) {
                    side = merge_side(side, referenced_side(child));
                    if (side == predicate_side::mixed || side == predicate_side::unsupported) {
                        return side;
                    }
                }
                return side;
            }
            return merge_side(referenced_side(comp->left()), referenced_side(comp->right()));
        }

        bool remap_key_to_child(key_t& key) {
            if (key.path().empty()) {
                return false;
            }
            key.set_side(side_t::undefined);
            return true;
        }

        std::optional<param_storage> remap_param(std::pmr::memory_resource* resource,
                                                 const param_storage& param,
                                                 predicate_side target_side,
                                                 size_t right_offset) {
            if (std::holds_alternative<core::parameter_id_t>(param)) {
                return param;
            }
            if (!std::holds_alternative<key_t>(param)) {
                return std::nullopt;
            }

            key_t key = std::get<key_t>(param);
            if (target_side == predicate_side::right) {
                if (key.path().empty() || key.path()[0] < right_offset) {
                    return std::nullopt;
                }
                std::pmr::vector<size_t> path{resource};
                path.reserve(key.path().size());
                path.push_back(key.path()[0] - right_offset);
                for (size_t i = 1; i < key.path().size(); ++i) {
                    path.push_back(key.path()[i]);
                }
                key.set_path(std::move(path));
            }
            if (!remap_key_to_child(key)) {
                return std::nullopt;
            }
            return param_storage{std::move(key)};
        }

        expression_ptr clone_compare_for_child(std::pmr::memory_resource* resource,
                                               const expression_ptr& expr,
                                               predicate_side target_side,
                                               size_t right_offset) {
            if (!expr || expr->group() != expression_group::compare) {
                return nullptr;
            }
            const auto& comp = reinterpret_cast<const compare_expression_ptr&>(expr);
            if (comp->type() == compare_type::union_and) {
                auto clone = expressions::make_compare_union_expression(resource, compare_type::union_and);
                for (const auto& child : comp->children()) {
                    auto child_clone = clone_compare_for_child(resource, child, target_side, right_offset);
                    if (!child_clone) {
                        return nullptr;
                    }
                    clone->append_child(child_clone);
                }
                return clone;
            }

            auto left = remap_param(resource, comp->left(), target_side, right_offset);
            auto right = remap_param(resource, comp->right(), target_side, right_offset);
            if (!left || !right) {
                return nullptr;
            }
            return expressions::make_compare_expression(resource, comp->type(), *left, *right);
        }

        std::vector<expression_ptr> split_top_level_and(const expression_ptr& expr) {
            if (!expr || expr->group() != expression_group::compare) {
                return {expr};
            }
            const auto& comp = reinterpret_cast<const compare_expression_ptr&>(expr);
            if (comp->type() != compare_type::union_and) {
                return {expr};
            }
            std::vector<expression_ptr> result;
            result.reserve(comp->children().size());
            for (const auto& child : comp->children()) {
                result.push_back(child);
            }
            return result;
        }

        expression_ptr make_and(std::pmr::memory_resource* resource, const std::vector<expression_ptr>& predicates) {
            if (predicates.empty()) {
                return nullptr;
            }
            if (predicates.size() == 1) {
                return predicates.front();
            }
            auto expr = expressions::make_compare_union_expression(resource, compare_type::union_and);
            for (const auto& predicate : predicates) {
                expr->append_child(predicate);
            }
            return expr;
        }

        node_ptr find_child(node_aggregate_t* aggregate, node_type type) {
            for (const auto& child : aggregate->children()) {
                if (child && child->type() == type) {
                    return child;
                }
            }
            return nullptr;
        }

        void append_or_merge_match(std::pmr::memory_resource* resource,
                                   const node_ptr& aggregate,
                                   const expression_ptr& predicate) {
            if (!aggregate || aggregate->type() != node_type::aggregate_t || !predicate) {
                return;
            }
            auto* agg = static_cast<node_aggregate_t*>(aggregate.get());
            auto match = find_child(agg, node_type::match_t);
            if (!match) {
                match = logical_plan::make_node_match(resource, agg->dbname(), agg->relname(), predicate);
                match->set_table_oid(aggregate->table_oid());
                agg->append_child(match);
                return;
            }
            if (match->expressions().empty()) {
                match->append_expression(predicate);
                return;
            }
            std::vector<expression_ptr> predicates;
            predicates.reserve(2);
            predicates.push_back(match->expressions().front());
            predicates.push_back(predicate);
            match->expressions().clear();
            match->append_expression(make_and(resource, predicates));
        }

        bool child_column_count(const node_ptr& child, const table_cols_map& md, size_t& count) {
            if (!child || child->type() != node_type::aggregate_t) {
                return false;
            }
            const auto table_oid = child->table_oid();
            auto it = md.find(table_oid);
            if (it != md.end() && it->second > 0) {
                count = it->second;
                return true;
            }
            auto* aggregate = static_cast<node_aggregate_t*>(child.get());
            if (aggregate->projected_cols().empty()) {
                return false;
            }
            count = *std::max_element(aggregate->projected_cols().begin(), aggregate->projected_cols().end()) + 1;
            return count > 0;
        }

        bool pushdown_aggregate(const node_ptr& aggregate, const table_cols_map& md) {
            if (!aggregate || aggregate->type() != node_type::aggregate_t) {
                return false;
            }
            auto* agg = static_cast<node_aggregate_t*>(aggregate.get());
            auto match = find_child(agg, node_type::match_t);
            auto join = find_child(agg, node_type::join_t);
            if (!match || match->expressions().empty() || !join) {
                return false;
            }

            const auto* join_node = static_cast<const node_join_t*>(join.get());
            if (join_node->type() != logical_plan::join_type::inner &&
                join_node->type() != logical_plan::join_type::cross) {
                return false;
            }
            if (join->children().size() != 2 || join->children()[0]->type() != node_type::aggregate_t ||
                join->children()[1]->type() != node_type::aggregate_t) {
                return false;
            }

            size_t left_count = 0;
            if (!child_column_count(join->children()[0], md, left_count)) {
                return false;
            }

            std::vector<expression_ptr> residual;
            auto conjuncts = split_top_level_and(match->expressions().front());
            bool changed = false;
            for (const auto& conjunct : conjuncts) {
                const auto side = referenced_side(conjunct);
                if (side != predicate_side::left && side != predicate_side::right) {
                    residual.push_back(conjunct);
                    continue;
                }
                auto clone = clone_compare_for_child(aggregate->resource(), conjunct, side, left_count);
                if (!clone) {
                    residual.push_back(conjunct);
                    continue;
                }
                append_or_merge_match(aggregate->resource(),
                                      side == predicate_side::left ? join->children()[0] : join->children()[1],
                                      clone);
                changed = true;
            }

            if (!changed) {
                return false;
            }

            auto residual_expr = make_and(aggregate->resource(), residual);
            auto& children = agg->children();
            if (residual_expr) {
                match->expressions().clear();
                match->append_expression(residual_expr);
            } else {
                children.erase(std::remove(children.begin(), children.end(), match), children.end());
            }
            return true;
        }

        void walk(const node_ptr& node, const table_cols_map& md) {
            if (!node) {
                return;
            }
            if (node->type() == node_type::aggregate_t) {
                pushdown_aggregate(node, md);
            }
            for (const auto& child : node->children()) {
                walk(child, md);
            }
        }

    } // namespace

    void pushdown_filters(const logical_plan::node_ptr& root) {
        table_cols_map md;
        collect_table_md(root, md);
        walk(root, md);
    }

} // namespace components::planner::optimizer
