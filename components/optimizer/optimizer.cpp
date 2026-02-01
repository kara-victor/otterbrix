#include "optimizer.hpp"

namespace components::optimizer {

    auto optimizer_t::optimize(std::pmr::memory_resource*, logical_plan::node_ptr node) -> logical_plan::node_ptr {
        return node;
    }

} // namespace components::optimizer