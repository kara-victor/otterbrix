#include "stats_provider.hpp"

#include <components/expressions/compare_expression.hpp>

namespace components::optimizer::cbo {

    namespace {
        uint64_t default_row_count = 1000;
        double eq_selectivity = 0.1;
        double range_selectivity = 0.3;
        double unknown_selectivity = 0.5;
    } // namespace

    table_stats_t stats_provider_t::get_table_stats(const collection_full_name_t&) const { return {default_row_count}; }

    predicate_stats_t stats_provider_t::estimate_selectivity(const expressions::expression_ptr& expression) const {
        if (!expression) {
            return {unknown_selectivity};
        }

        if (expression->group() != expressions::expression_group::compare) {
            return {unknown_selectivity};
        }

        const auto* compare = static_cast<const expressions::compare_expression_t*>(expression.get());
        switch (compare->type()) {
            case expressions::compare_type::eq:
                return {eq_selectivity};
            case expressions::compare_type::gt:
            case expressions::compare_type::lt:
            case expressions::compare_type::gte:
            case expressions::compare_type::lte:
                return {range_selectivity};
            default:
                return {unknown_selectivity};
        }
    }

} // namespace components::optimizer::cbo