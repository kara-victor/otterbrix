#pragma once

#include "stats_provider.hpp"

namespace components::optimizer::cbo {

    struct plan_cost_t {
        double cpu{0.0};
        double io{0.0};

        double total() const { return cpu + io; }
    };

    class cost_model_t {
    public:
        plan_cost_t estimate_full_scan_cost(const table_stats_t& table_stats) const;
        plan_cost_t estimate_index_scan_cost(const table_stats_t& table_stats,
                                             const predicate_stats_t& predicate_stats) const;
        plan_cost_t estimate_join_cost(double left_rows, double right_rows) const;

    private:
        static constexpr double index_scan_row_factor = 0.2;
        static constexpr double join_factor = 1.0;
    };

} // namespace components::optimizer::cbo
