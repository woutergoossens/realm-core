#pragma once

#include "realm/util/from_chars.hpp"
#include "realm/util/string_view.hpp"

namespace realm::_impl {

struct MessageParseException : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

// These functions will parse the space/new-line delimited headers found at the beginning of
// messages and changesets.
inline util::StringView parse_header_element(util::StringView sv, char)
{
    return sv;
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<std::remove_reference_t<T>>>>
inline util::StringView parse_header_value(util::StringView sv, T&& cur_arg)
{
    auto parse_res = realm::util::from_chars(sv.begin(), sv.end(), cur_arg, 10);
    if (parse_res.ec != std::errc{}) {
        throw MessageParseException(
            util::format("error parsing integer in header line: %1", std::make_error_code(parse_res.ec).message()));
    }

    return sv.substr(parse_res.ptr - sv.begin());
}

inline util::StringView parse_header_value(util::StringView sv, util::StringView& cur_arg)
{
    auto delim_at = std::find(sv.begin(), sv.end(), ' ');
    if (delim_at == sv.end()) {
        throw MessageParseException("reached end of header line prematurely");
    }

    auto sub_str_len = std::distance(sv.begin(), delim_at);
    cur_arg = util::StringView(sv.begin(), sub_str_len);

    return sv.substr(sub_str_len);
}

template <typename T, typename... Args>
inline util::StringView parse_header_element(util::StringView sv, char end_delim, T&& cur_arg, Args&&... next_args)
{
    if (sv.empty()) {
        throw MessageParseException("cannot parse an empty header line");
    }
    sv = parse_header_value(sv, std::forward<T&&>(cur_arg));

    if (sv.front() == ' ') {
        return parse_header_element(sv.substr(1), end_delim, next_args...);
    }
    if (sv.front() == end_delim) {
        return sv.substr(1);
    }
    throw MessageParseException("found invalid character in header line");
}

template <typename... Args>
inline util::StringView parse_header_line(util::StringView sv, char end_delim, Args&&... args)
{
    return parse_header_element(sv, end_delim, args...);
}

} // namespace realm::_impl
