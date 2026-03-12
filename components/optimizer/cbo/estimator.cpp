#include "estimator.hpp"

#include <algorithm>

namespace components::optimizer::cbo {

    estimator_t::estimator_t(const stats_provider_t* stats_provider)
        : stats_provider_(stats_provider) {}

    double estimator_t::estimate_node_cardinality(const logical_plan::node_ptr& node) const {
        if (!node) {
            return 0.0;
        }

        if (node->children().empty()) {
            const auto table_stats = provider().get_table_stats(node->collection_full_name());
            return static_cast<double>(table_stats.row_count) * estimate_selectivity(node);
        }

        if (node->type() == logical_plan::node_type::join_t && node->children().size() >= 2) {
            const auto left = estimate_node_cardinality(node->children()[0]);
            const auto right = estimate_node_cardinality(node->children()[1]);
            return left * right * estimate_selectivity(node);
        }

        double cardinality = estimate_node_cardinality(node->children().front());
        for (size_t i = 1; i < node->children().size(); ++i) {
            cardinality = std::max(cardinality, estimate_node_cardinality(node->children()[i]));
        }

        return cardinality * estimate_selectivity(node);
    }

    const stats_provider_t& estimator_t::provider() const {
        return stats_provider_ ? *stats_provider_ : default_stats_provider_;
    }

    double estimator_t::estimate_selectivity(const logical_plan::node_ptr& node) const {
        double total_selectivity = 1.0;
        for (const auto& expression : node->expressions()) {
            total_selectivity *= provider().estimate_selectivity(expression).selectivity;
        }
        return total_selectivity;
    }

} // namespace components::optimizer::cbo