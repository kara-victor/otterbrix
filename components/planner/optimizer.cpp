#include "optimizer.hpp"

#include "optimizer/rules/column_pruning.hpp"
#include "optimizer/rules/constant_folding.hpp"
#include "optimizer/rules/filter_pushdown.hpp"
#include "optimizer/rules/hash_join.hpp"
#include "optimizer/rules/join_ordering.hpp"

#include <utility>

namespace components::planner {

    optimizer_pipeline_t::optimizer_pipeline_t(optimizer_context_t context)
        : context_(context) {}

    logical_plan::node_ptr optimizer_pipeline_t::run_pre_validate(logical_plan::node_ptr node) const {
        if (!node) {
            return nullptr;
        }

        if (context_.options.enable_constant_folding && context_.parameters) {
            optimizer::fold_constants(context_.resource, node, context_.parameters);
        }

        return node;
    }

    logical_plan::node_ptr optimizer_pipeline_t::run_post_validate(logical_plan::node_ptr node) const {
        if (!node) {
            return nullptr;
        }

        if (context_.options.enable_filter_pushdown) {
            optimizer::pushdown_filters(node);
        }

        const auto* leading = context_.hints && context_.hints->leading_order ? &*context_.hints->leading_order : nullptr;
        const bool leading_applied = leading && optimizer::force_join_order(node, *leading);
        if (!leading_applied && context_.options.enable_cbo_join_ordering &&
            !(context_.hints && context_.hints->disable_cbo) && context_.table_statistics) {
            optimizer::order_joins(node, *context_.table_statistics, context_.join_cost_model);
        }

        // Column pruning is a default post-validate RBO rule. If it cannot
        // prove a projection is safe, it leaves projected_cols empty and scans
        // keep the existing read-all-columns behavior.
        if (context_.options.enable_column_pruning) {
            optimizer::prune_columns(node);
        }

        if (context_.options.enable_hash_join_rewrite) {
            node = optimizer::rewrite_hash_joins(context_.resource, std::move(node));
        }

        return node;
    }

    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters) {
        optimizer_context_t context{resource, parameters, optimizer_options_t{}, nullptr, join_cost_model_t{}, nullptr};
        return optimizer_pipeline_t(context).run_pre_validate(std::move(node));
    }

    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        return post_validate_optimize(resource, std::move(node), nullptr, nullptr);
    }

    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* resource,
                                                  logical_plan::node_ptr node,
                                                  const table_statistics_map_t* table_statistics) {
        return post_validate_optimize(resource, std::move(node), table_statistics, nullptr);
    }

    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* resource,
                                                  logical_plan::node_ptr node,
                                                  const table_statistics_map_t* table_statistics,
                                                  const logical_plan::optimizer_hints_t* hints) {
        optimizer_context_t context{resource, nullptr, optimizer_options_t{}, table_statistics, join_cost_model_t{}, hints};
        return optimizer_pipeline_t(context).run_post_validate(std::move(node));
    }

} // namespace components::planner
