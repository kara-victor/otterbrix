#pragma once

#include <cstdint>
#include <limits>
#include <optional>

namespace components::planner {

    struct table_statistics_t {
        std::optional<std::uint64_t> row_count;
    };

    struct scan_cost_model_t {
        double index_startup_cost{10.0};
        double row_fetch_cost{2.0};
        double equality_selectivity{0.01};
        double range_selectivity{0.25};
    };

    enum class scan_access_path_t : std::uint8_t
    {
        full_scan,
        index_scan
    };

    struct scan_selection_result_t {
        scan_access_path_t access_path{scan_access_path_t::full_scan};
        double full_scan_cost{std::numeric_limits<double>::infinity()};
        double index_scan_cost{std::numeric_limits<double>::infinity()};
    };

} // namespace components::planner
