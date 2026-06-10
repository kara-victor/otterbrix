#include "hint_parser.hpp"

#include <algorithm>
#include <cctype>
#include <string_view>
#include <unordered_set>

namespace components::sql::transform {

    namespace {
        bool is_identifier_char(char c) {
            return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
        }

        void skip_space(std::string_view text, size_t& pos) {
            while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos]))) {
                ++pos;
            }
        }

        std::string upper(std::string_view value) {
            std::string result(value);
            std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) {
                return static_cast<char>(std::toupper(c));
            });
            return result;
        }

        std::optional<std::vector<std::string>> parse_arguments(std::string_view body, size_t& pos) {
            skip_space(body, pos);
            if (pos >= body.size() || body[pos] != '(') {
                return std::nullopt;
            }
            ++pos;
            std::vector<std::string> result;
            while (pos < body.size()) {
                skip_space(body, pos);
                while (pos < body.size() && body[pos] == ',') {
                    ++pos;
                    skip_space(body, pos);
                }
                if (pos < body.size() && body[pos] == ')') {
                    ++pos;
                    return result.empty() ? std::nullopt : std::optional{std::move(result)};
                }
                const size_t begin = pos;
                while (pos < body.size() && is_identifier_char(body[pos])) {
                    ++pos;
                }
                if (begin == pos) {
                    return std::nullopt;
                }
                result.emplace_back(body.substr(begin, pos - begin));
                skip_space(body, pos);
                if (pos < body.size() && body[pos] != ',' && body[pos] != ')' &&
                    !is_identifier_char(body[pos])) {
                    return std::nullopt;
                }
            }
            return std::nullopt;
        }
    } // namespace

    logical_plan::optimizer_hints_t parse_optimizer_hints(const char* raw_sql) {
        logical_plan::optimizer_hints_t result;
        if (!raw_sql) {
            return result;
        }
        const std::string_view sql(raw_sql);
        size_t pos = 0;
        skip_space(sql, pos);
        if (pos + 6 > sql.size() || upper(sql.substr(pos, 6)) != "SELECT") {
            return result;
        }
        pos += 6;
        skip_space(sql, pos);
        if (pos + 3 > sql.size() || sql.substr(pos, 3) != "/*+") {
            return result;
        }
        const auto end = sql.find("*/", pos + 3);
        if (end == std::string_view::npos) {
            return result;
        }

        const auto body = sql.substr(pos + 3, end - pos - 3);
        std::unordered_set<std::string> conflicted;
        pos = 0;
        while (pos < body.size()) {
            skip_space(body, pos);
            while (pos < body.size() && body[pos] == ',') {
                ++pos;
                skip_space(body, pos);
            }
            const size_t begin = pos;
            while (pos < body.size() && is_identifier_char(body[pos])) {
                ++pos;
            }
            if (begin == pos) {
                ++pos;
                continue;
            }
            const auto name = upper(body.substr(begin, pos - begin));
            if (name == "NO_CBO") {
                size_t lookahead = pos;
                skip_space(body, lookahead);
                if (lookahead >= body.size() || body[lookahead] != '(') {
                    result.disable_cbo = true;
                }
                continue;
            }
            auto args = parse_arguments(body, pos);
            if (!args) {
                continue;
            }
            if (name == "LEADING") {
                std::unordered_set<std::string> unique;
                bool valid = true;
                for (const auto& alias : *args) {
                    valid = unique.insert(alias).second && valid;
                }
                if (valid) {
                    result.leading_order = std::move(*args);
                }
            } else if ((name == "FULL_SCAN" || name == "INDEX_SCAN") && args->size() == 1) {
                const auto& alias = args->front();
                if (conflicted.count(alias)) {
                    continue;
                }
                const auto preference =
                    name == "FULL_SCAN" ? logical_plan::scan_hint_t::full_scan : logical_plan::scan_hint_t::index_scan;
                auto [it, inserted] = result.scan_preferences.emplace(alias, preference);
                if (!inserted && it->second != preference) {
                    result.scan_preferences.erase(it);
                    conflicted.insert(alias);
                }
            }
        }
        return result;
    }

} // namespace components::sql::transform
