#include "optimizer.hpp"

#include "optimizer/rules/column_pruning.hpp"
#include "optimizer/rules/constant_folding.hpp"
#include "optimizer/rules/hash_join.hpp"

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
        optimizer_context_t context{resource, parameters, optimizer_options_t{}};
        return optimizer_pipeline_t(context).run_pre_validate(std::move(node));
    }

    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        optimizer_context_t context{resource, nullptr, optimizer_options_t{}};
        return optimizer_pipeline_t(context).run_post_validate(std::move(node));
    }

} // namespace components::planner
