#pragma once

#include <components/logical_plan/node.hpp>

#include <string>
#include <vector>
#include <components/planner/cost_model.hpp>

namespace components::planner::optimizer {

    // Reorders only fully supported left-deep INNER/equi join chains. Any
    // missing statistics or unsafe expression shape leaves the tree unchanged.
    bool force_join_order(const logical_plan::node_ptr& root, const std::vector<std::string>& leading_order);

    bool order_joins(const logical_plan::node_ptr& root,
                     const table_statistics_map_t& statistics,
                     const join_cost_model_t& cost_model = {});

} // namespace components::planner::optimizer
