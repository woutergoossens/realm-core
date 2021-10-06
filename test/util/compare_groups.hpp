#ifndef REALM_TEST_UTIL_COMPARE_GROUPS_HPP
#define REALM_TEST_UTIL_COMPARE_GROUPS_HPP

#include <functional>

#include <realm/util/logger.hpp>
#include <realm/group.hpp>
#include <realm/table.hpp>

namespace realm::test_util {

bool compare_tables(const Table& table_1, const Table& table_2, util::Logger&);

bool compare_tables(const Table& table_1, const Table& table_2);

bool compare_groups(const Group& group_1, const Group& group_2);

bool compare_groups(const Group& group_1, const Group& group_2, util::Logger&);

bool compare_groups(const Group& group_1, const Group& group_2,
                    std::function<bool(StringData table_name)> filter_func, util::Logger&);
} // namespace realm::test_util

#endif // REALM_TEST_UTIL_COMPARE_GROUPS_HPP
