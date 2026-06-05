#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::planner {

    // Early optimizer entry point. Runs BEFORE schema validation / enrich, so it
    // must only execute rules that are independent of resolved column indices,
    // resolved paths, table OIDs, and catalog metadata. The concrete rules are
    // registered in the early RBO pipeline in optimizer.cpp; currently this phase
    // contains constant_folding for parameter expressions.
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters);

    // Late optimizer entry point. Runs AFTER validate_schema and
    // stamp_oids_from_resolves, so schema-aware rules may rely on stamped
    // key.side()/key.path(), node->table_oid(), and resolved metadata embedded in
    // the plan tree. The concrete rules are registered in the late RBO pipeline in
    // optimizer.cpp; currently this phase contains the hash-join rewrite.
    //
    // Column pruning is intentionally not part of the active pipeline yet. The
    // implementation remains available under optimizer/rules and should be enabled
    // in a separate stabilization step.
    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node);

} // namespace components::planner
