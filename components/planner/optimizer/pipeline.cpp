#include "pipeline.hpp"

#include <utility>

namespace components::planner::optimizer {

    logical_plan::node_ptr run_optimizer_pipeline(optimizer_context_t& context,
                                                  logical_plan::node_ptr node,
                                                  const optimizer_rule_t* rules,
                                                  std::size_t rule_count) {
        if (!node) {
            return nullptr;
        }

        for (std::size_t i = 0; i < rule_count; ++i) {
            const auto& rule = rules[i];
            if (!rule.enabled_by_default || rule.phase != context.phase || !rule.apply) {
                continue;
            }
            node = rule.apply(context, std::move(node));
            if (!node) {
                return nullptr;
            }
        }

        return node;
    }

} // namespace components::planner::optimizer
