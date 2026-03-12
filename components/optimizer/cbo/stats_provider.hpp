#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/expressions/expression.hpp>

namespace components::optimizer::cbo {

    struct table_stats_t {
        uint64_t row_count{0};
    };

    struct predicate_stats_t {
        double selectivity{1.0}; 
    };

    class stats_provider_t {
    public:
        table_stats_t get_table_stats(const collection_full_name_t& collection) const;
        predicate_stats_t estimate_selectivity(const expressions::expression_ptr& expression) const;
    };

} // namespace components::optimizer::cbo