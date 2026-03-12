#include "optimizer.hpp"

#include "cbo/enumerator.hpp"

#include <limits>

namespace components::optimizer {

    logical_plan::node_ptr optimizer_t::optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        auto candidates = cbo::enumerator_t{}.enumerate(resource, std::move(node));
        if (candidates.empty()) {
            return nullptr;
        }

        auto best = candidates.begin();
        auto best_cost = std::numeric_limits<double>::infinity();
        for (auto it = candidates.begin(); it != candidates.end(); ++it) {
            if (it->cost.total() < best_cost) {
                best = it;
                best_cost = it->cost.total();
            }
        }

        return best->root;
    }

} // namespace components::optimizer