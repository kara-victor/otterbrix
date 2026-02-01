#pragma once

#include <components/logical_plan/node.hpp>


namespace components::optimizer {

	class optimizer_t {
    public:
        logical_plan::node_ptr optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node);
	};

} // namespace components::optimizer