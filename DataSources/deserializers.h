#ifndef DESERIALIZERS_H
#define DESERIALIZERS_H

#include <cstring>
#include <charconv>
#include "fast_float.h"

template<typename T>
void deserialize(const char *src, const char *end, void *dest, int attSize, size_t len);

// Specializations for int, double, char, and string
template<>
inline void deserialize<int>(const char *src, const char *end, void *dest, int attSize, size_t len) {
    int value;
    std::from_chars(src, end, value);
    std::memcpy(dest, &value, sizeof(int));
}

template<>
inline void deserialize<double>(const char *src, const char *end, void *dest, int attSize, size_t len) {
    double value;
    fast_float::from_chars(src, end, value);
    //std::from_chars(src, end, value);
    std::memcpy(dest, &value, sizeof(double));
}

template<>
inline void deserialize<char>(const char *src, const char *end, void *dest, int attSize, size_t len) {
    std::memcpy(dest, src, sizeof(char));
}

template<>
inline void deserialize<const char *>(const char *src, const char *end, void *dest, int attSize, size_t len) {
    std::memset(dest, 0, attSize);
    std::memcpy(dest, src, len);
}

#endif // DESERIALIZERS_H