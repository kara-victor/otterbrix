#pragma once

#include "context.hpp"

#include <cstddef>
#include <string_view>

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    using rule_fn = logical_plan::node_ptr (*)(optimizer_context_t& context, logical_plan::node_ptr node);

    struct optimizer_rule_t {
        std::string_view name;
        optimizer_phase phase;
        bool enabled_by_default;
        rule_fn apply;
    };

    logical_plan::node_ptr run_optimizer_pipeline(optimizer_context_t& context,
                                                  logical_plan::node_ptr node,
                                                  const optimizer_rule_t* rules,
                                                  std::size_t rule_count);

} // namespace components::planner::optimizer
