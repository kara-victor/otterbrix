#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Pushes safe WHERE predicates below inner/cross joins so each side can
    // filter during its own scan. Runs after validate_schema, when key side()
    // and path() are already resolved.
    //
    // First iteration is intentionally conservative:
    //   * only aggregate_t with a match_t sibling above join_t;
    //   * only inner/cross joins;
    //   * only top-level AND conjuncts or a single compare predicate;
    //   * only predicates that reference exactly one join side;
    //   * OR/NOT/function/unsupported nested expressions stay as residuals.
    void pushdown_filters(const logical_plan::node_ptr& root);

} // namespace components::planner::optimizer
