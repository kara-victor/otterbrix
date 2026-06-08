#include "scan_selection.hpp"

#include "index_selection_helpers.hpp"

#include <components/logical_plan/param_storage.hpp>

namespace services::planner::impl {

    namespace {

        namespace expr = components::expressions;
        namespace lp = components::logical_plan;
        namespace cbo = components::planner;

        bool is_range_compare(expr::compare_type type) {
            return type == expr::compare_type::lt || type == expr::compare_type::lte ||
                   type == expr::compare_type::gt || type == expr::compare_type::gte;
        }

        lp::index_type compatible_index_type(const context_storage_t& context,
                                             const expr::key_t& key,
                                             expr::compare_type compare) {
            if (compare == expr::compare_type::eq) {
                if (context.has_index_on(key, lp::index_type::hashed)) {
                    return lp::index_type::hashed;
                }
                if (context.has_index_on(key, lp::index_type::single)) {
                    return lp::index_type::single;
                }
                return lp::index_type::no_valid;
            }
            if (is_range_compare(compare) && context.has_index_on(key, lp::index_type::single)) {
                return lp::index_type::single;
            }
            return lp::index_type::no_valid;
        }

    } // namespace

    scan_candidate_t select_scan_access_path(const context_storage_t& context,
                                             components::catalog::oid_t table_oid,
                                             const components::expressions::compare_expression_t& predicate,
                                             components::planner::scan_cost_model_t cost_model) {
        scan_candidate_t result;
        if (!context.parameters || expr::is_union_compare_condition(predicate.type())) {
            return result;
        }

        const expr::key_t* key = nullptr;
        if (std::holds_alternative<expr::key_t>(predicate.left()) &&
            std::holds_alternative<core::parameter_id_t>(predicate.right())) {
            key = &std::get<expr::key_t>(predicate.left());
            result.key_on_left = true;
            result.compare_type = predicate.type();
        } else if (std::holds_alternative<core::parameter_id_t>(predicate.left()) &&
                   std::holds_alternative<expr::key_t>(predicate.right())) {
            key = &std::get<expr::key_t>(predicate.right());
            result.key_on_left = false;
            result.compare_type = mirror_compare(predicate.type());
        } else {
            return result;
        }

        result.index_type = compatible_index_type(context, *key, result.compare_type);
        if (result.index_type == lp::index_type::no_valid) {
            return result;
        }

        const auto* stats = context.table_statistics_for(table_oid);
        if (!context.enable_cbo_scan_selection || !stats || !stats->row_count) {
            result.selection.access_path = cbo::scan_access_path_t::index_scan;
            return result;
        }

        const double rows = static_cast<double>(*stats->row_count);
        const double selectivity = result.compare_type == expr::compare_type::eq ? cost_model.equality_selectivity
                                                                                 : cost_model.range_selectivity;
        result.selection.full_scan_cost = rows;
        result.selection.index_scan_cost =
            cost_model.index_startup_cost + rows * selectivity * cost_model.row_fetch_cost;
        if (result.selection.index_scan_cost < result.selection.full_scan_cost) {
            result.selection.access_path = cbo::scan_access_path_t::index_scan;
        }
        return result;
    }

} // namespace services::planner::impl
