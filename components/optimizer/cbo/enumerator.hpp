#pragma once

#include "candidate_plan.hpp"

namespace components::optimizer::cbo {

    class enumerator_t {
    public:
        std::pmr::vector<candidate_plan_t> enumerate(std::pmr::memory_resource* resource,
                                                     logical_plan::node_ptr node) const;
    };

} // namespace components::optimizer::cbo