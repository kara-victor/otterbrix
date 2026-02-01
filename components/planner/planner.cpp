#include <components/optimizer/optimizer.hpp>
#include "planner.hpp"

namespace components::planner {

    auto planner_t::create_plan(std::pmr::memory_resource*, logical_plan::node_ptr node) -> logical_plan::node_ptr {
        components::optimizer::optimizer_t optimizer{};
        return optimizer.optimize(resource, std::move(node));
    }

} // namespace components::planner
