#pragma once

#include <components/logical_plan/node.hpp>
#include <components/expressions/compare_expression.hpp>
#include <memory_resource>

namespace components::optimizer::rules {
    using expression_ptr = components::expressions::expression_ptr;
    using node_ptr = components::logical_plan::node_ptr;

    class conjunction_simplification_t {
    public:
        node_ptr apply(std::pmr::memory_resource* resource,
                                                 components::logical_plan::node_ptr root);

    private:

        void simplify_plan(std::pmr::memory_resource* resource, const node_ptr& node);
        expression_ptr simplify(std::pmr::memory_resource* resource, const expression_ptr& expr);
        expression_ptr simplify_and(std::pmr::memory_resource* resource, const expression_ptr& expr);
        expression_ptr simplify_or(std::pmr::memory_resource* resource, const expression_ptr& expr);
        expression_ptr simplify_not(std::pmr::memory_resource* resource, const expression_ptr& expr);

        bool is_compare(const expression_ptr& expr);
        bool is_true(const expression_ptr& expr);
        bool is_false(const expression_ptr& expr);
        bool is_and(const expression_ptr& expr);
        bool is_or(const expression_ptr& expr);
        bool is_not(const expression_ptr& expr);

        expression_ptr make_true(std::pmr::memory_resource* resource);
        expression_ptr make_false(std::pmr::memory_resource* resource);

        void append_flattened(components::expressions::compare_type op,
                              const expression_ptr& child,
                              components::expressions::compare_expression_t& out);
    };
}