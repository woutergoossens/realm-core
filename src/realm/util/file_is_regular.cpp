#include <system_error>
#include <stdexcept>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#include <sys/stat.h>
#endif

#include <realm/utilities.hpp>
#include <realm/util/file.hpp>
#include <realm/util/file_is_regular.hpp>

using namespace realm;

bool util::file_is_regular(const std::string& path)
{
#ifndef _WIN32
    struct stat statbuf;
    if (::stat(path.c_str(), &statbuf) == 0)
        return S_ISREG(statbuf.st_mode);
    int err = errno; // Eliminate any risk of clobbering
    switch (err) {
        case EACCES:
        case ENOENT:
        case ENOTDIR:
            return false;
    }
    throw std::system_error(err, std::system_category(), "stat() failed");
#elif REALM_HAVE_STD_FILESYSTEM
    std::wstring w_path = string_to_wstring(path);
    return std::filesystem::is_regular_file(w_path);
#else
    static_cast<void>(path);
    throw util::runtime_error("Not yet supported");
#endif
}
