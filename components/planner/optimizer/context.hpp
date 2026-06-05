#pragma once

#include <cstdint>
#include <memory_resource>

#include <components/logical_plan/param_storage.hpp>

namespace components::planner::optimizer {

    enum class optimizer_phase : uint8_t
    {
        early_rbo,
        late_rbo,
        cbo
    };

    struct optimizer_context_t {
        std::pmr::memory_resource* resource = nullptr;
        logical_plan::parameter_node_t* parameters = nullptr;
        optimizer_phase phase = optimizer_phase::early_rbo;
    };

} // namespace components::planner::optimizer
