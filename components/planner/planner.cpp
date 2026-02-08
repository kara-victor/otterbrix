#include <iostream>

#include <components/optimizer/optimizer.hpp>
#include "planner.hpp"

namespace components::planner {

    logical_plan::node_ptr planner_t::create_plan(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        std::cout << "\n================ LOGICAL PLAN BEFORE OPTIMIZATION ================\n";
        std::cout << node->to_string() << std::endl;
        std::cout << "==================================================================\n";

        components::optimizer::optimizer_t optimizer{};
        auto optimized = optimizer.optimize(resource, std::move(node));

        std::cout << "\n================ LOGICAL PLAN AFTER OPTIMIZATION =================\n";
        std::cout << optimized->to_string() << std::endl;
        std::cout << "==================================================================\n";

        return optimized;
    }

} // namespace components::planner
