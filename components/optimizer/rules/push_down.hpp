#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>

#include <memory_resource>

namespace components::optimizer::rules {
    using expression_ptr = components::expressions::expression_ptr;
    using node_ptr = components::logical_plan::node_ptr;

    class push_down_t {
    public:
        node_ptr apply(std::pmr::memory_resource* resource, node_ptr root);

    private:
        enum class join_side : uint8_t
        {
            unknown,
            left,
            right,
            both
        };

        void rewrite_plan(std::pmr::memory_resource* resource, const node_ptr& node);

        std::pmr::vector<expression_ptr> split_and_conjuncts(std::pmr::memory_resource* resource,
                                                             const expression_ptr& expression);

        expression_ptr build_and(std::pmr::memory_resource* resource,
                                 const std::pmr::vector<expression_ptr>& conjuncts);

        join_side classify_by_join_side(const expression_ptr& expression);
    };

} // namespace components::optimizer::rules