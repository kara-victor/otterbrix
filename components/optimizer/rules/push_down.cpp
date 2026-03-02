#include "push_down.hpp"

namespace components::optimizer::rules {

    using compare_expression_ptr = components::expressions::compare_expression_ptr;
    using compare_type = components::expressions::compare_type;
    using side_t = components::expressions::side_t;
    using node_type = components::logical_plan::node_type;

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

        bool can_push_to_children(components::logical_plan::join_type type) {
            return type == components::logical_plan::join_type::inner;
        }
    } // namespace

    void push_down_t::rewrite_plan(std::pmr::memory_resource* resource, node_ptr& node) {
        if (!node) {
            return;
        }

        for (node_ptr& child : node->children()) {
            rewrite_plan(resource, child);
        }

        auto append_to_child = [&](node_ptr& child, const std::pmr::vector<expression_ptr>& pushed) {
            if (pushed.empty()) {
                return;
            }

            std::pmr::vector<expression_ptr> child_conjuncts(resource);

            if (child && child->type() == node_type::match_t) {
                for (const expression_ptr& existing : child->expressions()) {
                    std::pmr::vector<expression_ptr> pieces = split_and_conjuncts(resource, existing);
                    child_conjuncts.insert(child_conjuncts.end(), pieces.begin(), pieces.end());
                }
            }

            child_conjuncts.insert(child_conjuncts.end(), pushed.begin(), pushed.end());
            expression_ptr merged = build_and(resource, child_conjuncts);

            if (!merged) {
                return;
            }

            if (child && child->type() == node_type::match_t) {
                child->expressions().clear();
                child->append_expression(merged);
            } else {
                const auto collection = child ? child->collection_full_name() : node->collection_full_name();
                auto match = components::logical_plan::make_node_match(resource, collection, merged);
                if (child) {
                    match->append_child(child);
                }
                child = match;
            }
        };

        if (node->type() == node_type::join_t && node->children().size() >= 2) {
            auto* join_node = reinterpret_cast<components::logical_plan::node_join_t*>(node.get());
            const auto join_kind = join_node->type();

            std::pmr::vector<expression_ptr> stay_on_join(resource);
            std::pmr::vector<expression_ptr> left_push(resource);
            std::pmr::vector<expression_ptr> right_push(resource);

            for (const expression_ptr& expression : node->expressions()) {
                std::pmr::vector<expression_ptr> conjuncts = split_and_conjuncts(resource, expression);

                for (const expression_ptr& conjunct : conjuncts) {
                    switch (classify_by_join_side(conjunct)) {
                        case join_side::left:
                            if (can_push_to_children(join_kind)) {
                                left_push.emplace_back(conjunct);
                            } else {
                                stay_on_join.emplace_back(conjunct);
                            }
                            break;
                        case join_side::right:
                            if (can_push_to_children(join_kind)) {
                                right_push.emplace_back(conjunct);
                            } else {
                                stay_on_join.emplace_back(conjunct);
                            }
                            break;
                        case join_side::both:
                        case join_side::unknown:
                            stay_on_join.emplace_back(conjunct);
                            break;
                    }
                }
            }

            node_ptr& left_child = node->children()[0];
            node_ptr& right_child = node->children()[1];
            append_to_child(left_child, left_push);
            append_to_child(right_child, right_push);

            node->expressions().clear();
            if (expression_ptr merged = build_and(resource, stay_on_join); merged) {
                node->append_expression(merged);
            }
            return;
        }

        if (node->type() == node_type::match_t && !node->children().empty()) {
            node_ptr& child = node->children().front();
            if (!child || child->type() != node_type::join_t || node->expressions().empty()) {
                return;
            }

            auto* join_node = reinterpret_cast<components::logical_plan::node_join_t*>(child.get());
            if (!can_push_to_children(join_node->type()) || child->children().size() < 2) {
                return;
            }

            std::pmr::vector<expression_ptr> stay_on_match(resource);
            std::pmr::vector<expression_ptr> left_push(resource);
            std::pmr::vector<expression_ptr> right_push(resource);

            for (const expression_ptr& expression : node->expressions()) {
                std::pmr::vector<expression_ptr> conjuncts = split_and_conjuncts(resource, expression);
                for (const expression_ptr& conjunct : conjuncts) {
                    switch (classify_by_join_side(conjunct)) {
                        case join_side::left:
                            left_push.emplace_back(conjunct);
                            break;
                        case join_side::right:
                            right_push.emplace_back(conjunct);
                            break;
                        case join_side::both:
                        case join_side::unknown:
                            stay_on_match.emplace_back(conjunct);
                            break;
                    }
                }
            }

            node_ptr& left_child = child->children()[0];
            node_ptr& right_child = child->children()[1];
            append_to_child(left_child, left_push);
            append_to_child(right_child, right_push);

            node->expressions().clear();
            if (expression_ptr merged = build_and(resource, stay_on_match); merged) {
                node->append_expression(merged);
            } else {
                node = child;
            }
            return;
        }

        if (node->type() == node_type::aggregate_t) {
            node_ptr join_child;
            std::size_t join_index = 0;
            for (std::size_t i = 0; i < node->children().size(); ++i) {
                if (node->children()[i] && node->children()[i]->type() == node_type::join_t) {
                    join_child = node->children()[i];
                    join_index = i;
                    break;
                }
            }

            if (!join_child || join_child->children().size() < 2) {
                return;
            }

            auto* join_node = reinterpret_cast<components::logical_plan::node_join_t*>(join_child.get());
            if (!can_push_to_children(join_node->type())) {
                return;
            }

            for (std::size_t i = 0; i < node->children().size();) {
                if (i == join_index || !node->children()[i] || node->children()[i]->type() != node_type::match_t) {
                    ++i;
                    continue;
                }

                node_ptr& match_node = node->children()[i];
                std::pmr::vector<expression_ptr> stay_on_match(resource);
                std::pmr::vector<expression_ptr> left_push(resource);
                std::pmr::vector<expression_ptr> right_push(resource);

                for (const expression_ptr& expression : match_node->expressions()) {
                    std::pmr::vector<expression_ptr> conjuncts = split_and_conjuncts(resource, expression);
                    for (const expression_ptr& conjunct : conjuncts) {
                        switch (classify_by_join_side(conjunct)) {
                            case join_side::left:
                                left_push.emplace_back(conjunct);
                                break;
                            case join_side::right:
                                right_push.emplace_back(conjunct);
                                break;
                            case join_side::both:
                            case join_side::unknown:
                                stay_on_match.emplace_back(conjunct);
                                break;
                        }
                    }
                }

                node_ptr& left_child = join_child->children()[0];
                node_ptr& right_child = join_child->children()[1];
                append_to_child(left_child, left_push);
                append_to_child(right_child, right_push);

                match_node->expressions().clear();
                if (expression_ptr merged = build_and(resource, stay_on_match); merged) {
                    match_node->append_expression(merged);
                    ++i;
                } else {
                    node->children().erase(node->children().begin() + static_cast<std::ptrdiff_t>(i));
                    if (i < join_index) {
                        --join_index;
                    }
                }
            }
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