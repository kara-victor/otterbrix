#pragma once

#include <components/logical_plan/optimizer_hints.hpp>

namespace components::sql::transform {

    logical_plan::optimizer_hints_t parse_optimizer_hints(const char* raw_sql);

} // namespace components::sql::transform
