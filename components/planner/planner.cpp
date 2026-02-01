#include <components/optimizer/optimizer.hpp>
#include "planner.hpp"

namespace components::planner {

    logical_plan::node_ptr planner_t::create_plan(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        components::optimizer::optimizer_t optimizer{};
        return optimizer.optimize(resource, std::move(node));
    }

} // namespace components::planner
