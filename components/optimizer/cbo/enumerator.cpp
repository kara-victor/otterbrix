#include "enumerator.hpp"

#include "estimator.hpp"
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/optimizer/rules/push_down.hpp>

namespace components::optimizer::cbo {

    namespace {

        plan_cost_t estimate_cost(const logical_plan::node_ptr& node,
                                  const estimator_t& estimator,
                                  const cost_model_t& cost_model) {
            if (!node) {
                return {};
            }

            plan_cost_t total{};
            for (const auto& child : node->children()) {
                const auto child_cost = estimate_cost(child, estimator, cost_model);
                total.cpu += child_cost.cpu;
                total.io += child_cost.io;
            }

            if (node->type() == logical_plan::node_type::join_t && node->children().size() >= 2) {
                const auto left_rows = estimator.estimate_node_cardinality(node->children()[0]);
                const auto right_rows = estimator.estimate_node_cardinality(node->children()[1]);
                const auto join_cost = cost_model.estimate_join_cost(left_rows, right_rows);
                total.cpu += join_cost.cpu + left_rows;
                total.io += join_cost.io;
                return total;
            }

            total.cpu += estimator.estimate_node_cardinality(node);
            return total;
        }

        logical_plan::node_ptr clone_node(std::pmr::memory_resource* resource, const logical_plan::node_ptr& node) {
            if (!node) {
                return nullptr;
            }

            logical_plan::node_ptr cloned = nullptr;
            switch (node->type()) {
                case logical_plan::node_type::aggregate_t:
                    cloned = logical_plan::make_node_aggregate(resource, node->collection_full_name());
                    break;
                case logical_plan::node_type::join_t:
                    cloned = logical_plan::make_node_join(resource,
                                                          node->collection_full_name(),
                                                          static_cast<logical_plan::node_join_t*>(node.get())->type());
                    break;
                case logical_plan::node_type::match_t:
                    cloned = logical_plan::make_node_match(resource,
                                                           node->collection_full_name(),
                                                           node->expressions().empty() ? nullptr
                                                                                       : node->expressions().front());
                    break;
                default:
                    return node;
            }

            if (node->type() != logical_plan::node_type::match_t) {
                cloned->append_expressions(node->expressions());
            } else {
                for (size_t i = 1; i < node->expressions().size(); ++i) {
                    cloned->append_expression(node->expressions()[i]);
                }
            }

            for (const auto& child : node->children()) {
                cloned->append_child(clone_node(resource, child));
            }
            return cloned;
        }

        bool swap_first_inner_join(const logical_plan::node_ptr& node) {
            if (!node) {
                return false;
            }

            if (node->type() == logical_plan::node_type::join_t && node->children().size() >= 2) {
                const auto* join = static_cast<const logical_plan::node_join_t*>(node.get());
                if (join->type() == logical_plan::join_type::inner) {
                    std::swap(node->children()[0], node->children()[1]);
                    return true;
                }
            }

            for (const auto& child : node->children()) {
                if (swap_first_inner_join(child)) {
                    return true;
                }
            }

            return false;
        }

    } // namespace

    std::pmr::vector<candidate_plan_t> enumerator_t::enumerate(std::pmr::memory_resource* resource,
                                                               logical_plan::node_ptr node) const {
        estimator_t estimator;
        cost_model_t cost_model;

        std::pmr::vector<candidate_plan_t> result(resource);

        auto base_plan = rules::push_down_t{}.apply(resource, std::move(node));
        result.push_back({base_plan,
                          estimate_cost(base_plan, estimator, cost_model),
                          "rule-based plan without conjunction_simplification"});

        auto swapped = clone_node(resource, base_plan);
        if (swap_first_inner_join(swapped)) {
            result.push_back({swapped, estimate_cost(swapped, estimator, cost_model), "inner join left/right swap"});
        }

        return result;
    }

} // namespace components::optimizer::cbo