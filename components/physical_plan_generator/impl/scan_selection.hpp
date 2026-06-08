#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/planner/cost_model.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    struct scan_candidate_t {
        components::planner::scan_selection_result_t selection;
        bool key_on_left{true};
        components::expressions::compare_type compare_type{components::expressions::compare_type::eq};
        components::logical_plan::index_type index_type{components::logical_plan::index_type::no_valid};
    };

    scan_candidate_t select_scan_access_path(const context_storage_t& context,
                                             components::catalog::oid_t table_oid,
                                             const components::expressions::compare_expression_t& predicate,
                                             components::planner::scan_cost_model_t cost_model = {});

} // namespace services::planner::impl
