#include "optimizer.hpp"

#include "rules/conjunction_simplification.hpp"

namespace components::optimizer {

    logical_plan::node_ptr optimizer_t::optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        node = rules::conjunction_simplification_t{}.apply(resource, std::move(node));
        return node;
    }

} // namespace components::optimizer