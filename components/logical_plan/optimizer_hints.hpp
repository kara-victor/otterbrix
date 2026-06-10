#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace components::logical_plan {

    enum class scan_hint_t : std::uint8_t
    {
        full_scan,
        index_scan
    };

    struct optimizer_hints_t {
        bool disable_cbo{false};
        std::optional<std::vector<std::string>> leading_order;
        std::unordered_map<std::string, scan_hint_t> scan_preferences;

        bool empty() const noexcept {
            return !disable_cbo && !leading_order && scan_preferences.empty();
        }
    };

} // namespace components::logical_plan
