#include "cost_model.hpp"

#include <algorithm>
#include <cmath>

namespace components::optimizer::cbo {

    plan_cost_t cost_model_t::estimate_full_scan_cost(const table_stats_t& table_stats) const {
        const auto rows = static_cast<double>(table_stats.row_count);
        return {.cpu = rows, .io = rows};
    }

    plan_cost_t cost_model_t::estimate_index_scan_cost(const table_stats_t& table_stats,
                                                       const predicate_stats_t& predicate_stats) const {
        const auto rows = static_cast<double>(table_stats.row_count);
        const auto safe_rows = std::max(1.0, rows);
        const auto qualifying_rows = rows * predicate_stats.selectivity;
        return {.cpu = std::log2(safe_rows) + qualifying_rows * index_scan_row_factor, .io = qualifying_rows};
    }

    plan_cost_t cost_model_t::estimate_join_cost(double left_rows, double right_rows) const {
        const auto cpu_cost = left_rows * right_rows * join_factor;
        return {.cpu = cpu_cost, .io = 0.0};
    }

} // namespace components::optimizer::cbo