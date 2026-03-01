#include "push_down.hpp"

namespace components::optimizer::rules {

    using compare_expression_ptr = components::expressions::compare_expression_ptr;
    using compare_type = components::expressions::compare_type;
    using side_t = components::expressions::side_t;

    namespace {
        compare_expression_ptr as_compare(const expression_ptr& expression) {
            return reinterpret_cast<const compare_expression_ptr&>(expression);
        }

        bool is_compare(const expression_ptr& expression) {
            return expression && expression->group() == components::expressions::expression_group::compare;
        }

        bool is_and_expression(const expression_ptr& expression) {
            return is_compare(expression) && as_compare(expression)->type() == compare_type::union_and;
        }

        bool has_left_reference(const compare_expression_ptr& expression) {
            return expression->primary_key().side() == side_t::left ||
                   expression->secondary_key().side() == side_t::left;
        }

        bool has_right_reference(const compare_expression_ptr& expression) {
            return expression->primary_key().side() == side_t::right ||
                   expression->secondary_key().side() == side_t::right;
        }
    } // namespace

    void push_down_t::rewrite_plan(std::pmr::memory_resource* resource, const node_ptr& node) {
        if (!node) {
            return;
        }

        for (node_ptr& child : node->children()) {
            rewrite_plan(resource, child);
        }

        if (node->type() != components::logical_plan::node_type::join_t) {
            return;
        }

        std::pmr::vector<expression_ptr> rewritten(resource);
        for (const expression_ptr& expression : node->expressions()) {
            std::pmr::vector<expression_ptr> conjuncts = split_and_conjuncts(resource, expression);

            for (const expression_ptr& conjunct : conjuncts) {
                switch (classify_by_join_side(conjunct)) {
                    case join_side::left:
                    case join_side::right:
                    case join_side::both:
                    case join_side::unknown:
                        rewritten.emplace_back(conjunct);
                        break;
                }
            }
        }

        node->expressions().clear();
        if (expression_ptr merged = build_and(resource, rewritten); merged) {
            node->append_expression(merged);
        }
    }

    std::pmr::vector<expression_ptr> push_down_t::split_and_conjuncts(std::pmr::memory_resource* resource,
                                                                      const expression_ptr& expression) {
        std::pmr::vector<expression_ptr> result(resource);
        if (!expression) {
            return result;
        }

        if (!is_and_expression(expression)) {
            result.emplace_back(expression);
            return result;
        }

        for (const expression_ptr& child : as_compare(expression)->children()) {
            std::pmr::vector<expression_ptr> nested = split_and_conjuncts(resource, child);
            result.insert(result.end(), nested.begin(), nested.end());
        }
        return result;
    }

    expression_ptr push_down_t::build_and(std::pmr::memory_resource* resource,
                                          const std::pmr::vector<expression_ptr>& conjuncts) {
        if (conjuncts.empty()) {
            return nullptr;
        }

        if (conjuncts.size() == 1) {
            return conjuncts.front();
        }

        compare_expression_ptr and_expression =
            components::expressions::make_compare_union_expression(resource, compare_type::union_and);
        for (const expression_ptr& conjunct : conjuncts) {
            and_expression->append_child(conjunct);
        }
        return and_expression;
    }

    push_down_t::join_side push_down_t::classify_by_join_side(const expression_ptr& expression) {
        if (!is_compare(expression)) {
            return join_side::unknown;
        }

        compare_expression_ptr compare = as_compare(expression);

        if (compare->is_union()) {
            join_side result = join_side::unknown;
            for (const expression_ptr& child : compare->children()) {
                const join_side child_side = classify_by_join_side(child);
                if (result == join_side::unknown) {
                    result = child_side;
                    continue;
                }
                if (result != child_side) {
                    return join_side::both;
                }
            }
            return result;
        }

        const bool left = has_left_reference(compare);
        const bool right = has_right_reference(compare);

        if (left && right) {
            return join_side::both;
        }
        if (left) {
            return join_side::left;
        }
        if (right) {
            return join_side::right;
        }
        return join_side::unknown;
    }

    node_ptr push_down_t::apply(std::pmr::memory_resource* resource, node_ptr root) {
        if (!resource) {
            resource = std::pmr::get_default_resource();
        }

        rewrite_plan(resource, root);
        return root;
    }

} // namespace components::optimizer::rules