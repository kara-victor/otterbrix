#pragma once

#include "cost_model.hpp"

#include <components/logical_plan/node.hpp>

#include <string>

namespace components::optimizer::cbo {

    struct candidate_plan_t {
        logical_plan::node_ptr root;
        plan_cost_t cost;
        std::string reason;
    };

} // namespace components::optimizer::cbo