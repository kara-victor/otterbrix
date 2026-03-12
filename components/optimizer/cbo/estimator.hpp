#pragma once

#include "stats_provider.hpp"

#include <components/logical_plan/node.hpp>

namespace components::optimizer::cbo {

    class estimator_t {
    public:
        explicit estimator_t(const stats_provider_t* stats_provider = nullptr);

        double estimate_node_cardinality(const logical_plan::node_ptr& node) const;

    private:
        const stats_provider_t* stats_provider_;
        stats_provider_t default_stats_provider_;

        const stats_provider_t& provider() const;
        double estimate_selectivity(const logical_plan::node_ptr& node) const;
    };

} // namespace components::optimizer::cbo