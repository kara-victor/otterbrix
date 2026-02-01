#pragma once

#include <components/logical_plan/node.hpp>

namespace components::optimizer {

	class optimizer_t {
    public:
        auto optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node) -> logical_plan::node_ptr;
	};

} // namespace components::optimizer