#pragma once

#include <memory_resource>

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::planner {

    struct optimizer_options_t {
        bool enable_constant_folding{true};
        bool enable_filter_pushdown{true};
        bool enable_hash_join_rewrite{true};
        bool enable_column_pruning{true};
    };

    struct optimizer_context_t {
        std::pmr::memory_resource* resource{nullptr};
        logical_plan::parameter_node_t* parameters{nullptr};
        optimizer_options_t options{};
    };

    class optimizer_pipeline_t {
    public:
        explicit optimizer_pipeline_t(optimizer_context_t context);

        logical_plan::node_ptr run_pre_validate(logical_plan::node_ptr node) const;
        logical_plan::node_ptr run_post_validate(logical_plan::node_ptr node) const;

    private:
        optimizer_context_t context_;
    };

    // Early optimization pass. Runs BEFORE the schema validator / enrich.
    // Safe rules only — those that don't need resolved column indices or
    // table OIDs.
    //   - constant_folding (on parameter expressions)
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters);

    // Late optimization pass. Runs AFTER validate_schema +
    // stamp_oids_from_resolves, so node->table_oid() is populated and
    // sibling catalog_resolve_table_t nodes carry resolved_metadata().
    // Schema-aware RBO rules go here.
    //   - filter_pushdown (moves safe single-side predicates below inner/cross joins)
    //   - column_pruning (annotates node_aggregate_t with projected_cols;
    //     empty projected_cols remains the read-all-columns fallback)
    //   - hash_join_rewrite
    //
    // Schema info is read from the plan tree itself (sibling resolves);
    // the optimizer is self-contained and needs no external catalog handle.
    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node);

} // namespace components::planner