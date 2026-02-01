#include "conjunction_simplification.hpp"

#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>

namespace components::optimizer::rules {

    using components::expressions::compare_expression_ptr;
    using components::expressions::expression_ptr;
    using components::logical_plan::node_ptr;
    using compare_type = components::expressions::compare_type;

    compare_expression_ptr as_compare(const expression_ptr& expr) {
        return reinterpret_cast<const compare_expression_ptr&>(expr);
    }

    bool conjunction_simplification_t::is_compare(const expression_ptr& expr) {
        return expr && expr->group() == components::expressions::expression_group::compare;
    }

    bool conjunction_simplification_t::is_true(const expression_ptr& expr) {
        return is_compare(expr) && as_compare(expr)->type() == compare_type::all_true;
    }

    bool conjunction_simplification_t::is_false(const expression_ptr& expr) {
        return is_compare(expr) && as_compare(expr)->type() == compare_type::all_false;
    }

    bool conjunction_simplification_t::is_and(const expression_ptr& expr) {
        return is_compare(expr) && as_compare(expr)->type() == compare_type::union_and;
    }

    bool conjunction_simplification_t::is_or(const expression_ptr& expr) {
        return is_compare(expr) && as_compare(expr)->type() == compare_type::union_or;
    }

    bool conjunction_simplification_t::is_not(const expression_ptr& expr) {
        return is_compare(expr) && as_compare(expr)->type() == compare_type::union_not;
    }

    expression_ptr conjunction_simplification_t::make_true(std::pmr::memory_resource* resource) {
        return components::expressions::make_compare_expression(resource, compare_type::all_true);
    }

    expression_ptr conjunction_simplification_t::make_false(std::pmr::memory_resource* resource) {
        return components::expressions::make_compare_expression(resource, compare_type::all_false);
    }

    void conjunction_simplification_t::append_flattened(compare_type op,
                                                        const expression_ptr& child,
                                                        components::expressions::compare_expression_t& out) {
        if (!child) {
            return;
        }
        if (is_compare(child) && as_compare(child)->type() == op) {
            compare_expression_ptr nested = as_compare(child);
            for (const expression_ptr& gc : nested->children()) {
                out.append_child(gc);
            }
            return;
        }
        out.append_child(child);
    }

    expression_ptr conjunction_simplification_t::simplify_and(std::pmr::memory_resource* resource,
                                                              const expression_ptr& expr) {
        compare_expression_ptr e = as_compare(expr);
        compare_expression_ptr out =
            components::expressions::make_compare_union_expression(resource, compare_type::union_and);

        for (const expression_ptr& ch : e->children()) {
            expression_ptr s = simplify(resource, ch);

            if (is_false(s)) {
                return make_false(resource);
            }
            if (is_true(s)) {
                continue;
            }
            append_flattened(compare_type::union_and, s, *out);
        }

        const std::pmr::vector<expression_ptr>& kids = out->children();
        if (kids.empty()) {
            return make_true(resource);
        }
        if (kids.size() == 1) {
            return kids.front();
        }
        return out;
    }

    expression_ptr conjunction_simplification_t::simplify_or(std::pmr::memory_resource* resource,
                                                             const expression_ptr& expr) {
        compare_expression_ptr e = as_compare(expr);
        compare_expression_ptr out =
            components::expressions::make_compare_union_expression(resource, compare_type::union_or);

        for (const expression_ptr& ch : e->children()) {
            expression_ptr s = simplify(resource, ch);

            if (is_true(s)) {
                return make_true(resource);
            }
            if (is_false(s)) {
                continue;
            }
            append_flattened(compare_type::union_or, s, *out);
        }

        const std::pmr::vector<expression_ptr>& kids = out->children();
        if (kids.empty()) {
            return make_false(resource);
        }
        if (kids.size() == 1) {
            return kids.front();
        }
        return out;
    }

    expression_ptr conjunction_simplification_t::simplify_not(std::pmr::memory_resource* resource,
                                                              const expression_ptr& expr) {
        compare_expression_ptr e = as_compare(expr);

        if (e->children().empty()) {
            return make_true(resource);
        }

        expression_ptr inner = simplify(resource, e->children().front());

        if (is_true(inner)) {
            return make_false(resource);
        }
        if (is_false(inner)) {
            return make_true(resource);
        }

        if (is_not(inner)) {
            compare_expression_ptr inner_not = as_compare(inner);
            if (!inner_not->children().empty()) {
                return inner_not->children().front();
            }
        }

        compare_expression_ptr out =
            components::expressions::make_compare_union_expression(resource, compare_type::union_not);
        out->append_child(inner);
        return out;
    }

    expression_ptr conjunction_simplification_t::simplify(std::pmr::memory_resource* resource,
                                                          const expression_ptr& expr) {
        if (!expr) {
            return expr;
        }
        if (!is_compare(expr)) {
            return expr;
        }

        compare_expression_ptr e = as_compare(expr);

        if (e->type() == compare_type::all_true || e->type() == compare_type::all_false) {
            return expr;
        }

        if (e->type() == compare_type::union_and) {
            return simplify_and(resource, expr);
        }
        if (e->type() == compare_type::union_or) {
            return simplify_or(resource, expr);
        }
        if (e->type() == compare_type::union_not) {
            return simplify_not(resource, expr);
        }

        return expr;
    }

    void conjunction_simplification_t::simplify_plan(std::pmr::memory_resource* resource, const node_ptr& node) {
        if (!node) {
            return;
        }

        std::pmr::vector<node_ptr>& ch = node->children();
        for (std::size_t i = 0; i < ch.size();) {
            node_ptr& child = ch[i];
            simplify_plan(resource, child);

            if (child && child->type() == components::logical_plan::node_type::match_t) {
                const std::pmr::vector<expression_ptr>& exprs_const = child->expressions();
                if (!exprs_const.empty() && is_true(exprs_const.front())) {
                    ch.erase(ch.begin() + static_cast<std::ptrdiff_t>(i));
                    continue;
                }
            }

            ++i;
        }

        if (node->type() == components::logical_plan::node_type::match_t) {
            std::pmr::vector<expression_ptr>& exprs = node->expressions();
            if (!exprs.empty()) {
                exprs[0] = simplify(resource, exprs[0]);
            }
        }

        if (node->type() == components::logical_plan::node_type::join_t) {
            std::pmr::vector<expression_ptr>& exprs = node->expressions();
            for (expression_ptr& e : exprs) {
                e = simplify(resource, e);
            }
        }
    }

    node_ptr conjunction_simplification_t::apply(std::pmr::memory_resource* resource, node_ptr root) {
        if (!resource) {
            resource = std::pmr::get_default_resource();
        }
        simplify_plan(resource, root);
        return root;
    }

} // namespace components::optimizer::rules
