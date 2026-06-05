#include "optimizer.hpp"

#include "optimizer/pipeline.hpp"
#include "optimizer/rules/constant_folding.hpp"
#include "optimizer/rules/hash_join.hpp"

#include <array>
#include <utility>

namespace components::planner {

    namespace {
        namespace lp = components::logical_plan;
        namespace opt = components::planner::optimizer;

        lp::node_ptr apply_constant_folding(opt::optimizer_context_t& context, lp::node_ptr node) {
            if (context.parameters) {
                opt::fold_constants(context.resource, node, context.parameters);
            }
            return node;
        }

        lp::node_ptr apply_hash_join(opt::optimizer_context_t& context, lp::node_ptr node) {
            return opt::rewrite_hash_joins(context.resource, std::move(node));
        }

        constexpr std::array<opt::optimizer_rule_t, 1> early_rbo_rules{{
            {"constant_folding", opt::optimizer_phase::early_rbo, true, apply_constant_folding},
        }};

        constexpr std::array<opt::optimizer_rule_t, 1> late_rbo_rules{{
            {"hash_join", opt::optimizer_phase::late_rbo, true, apply_hash_join},
            // Column pruning remains intentionally disabled for now. The rule is
            // schema-aware and still available under optimizer/rules, but it needs a
            // separate regression-stabilization step before it joins this pipeline.
        }};
    } // namespace

    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters) {
        opt::optimizer_context_t context{resource, parameters, opt::optimizer_phase::early_rbo};
        return opt::run_optimizer_pipeline(context, std::move(node), early_rbo_rules.data(), early_rbo_rules.size());
    }

    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        opt::optimizer_context_t context{resource, nullptr, opt::optimizer_phase::late_rbo};
        return opt::run_optimizer_pipeline(context, std::move(node), late_rbo_rules.data(), late_rbo_rules.size());
    }

} // namespace components::planner
