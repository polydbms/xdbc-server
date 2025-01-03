#ifndef DESERIALIZERS_PARQUET_H
#define DESERIALIZERS_PARQUET_H

#include <cstring>
#include <parquet/stream_reader.h>
#include <string>

// Generic template declaration
template<typename T>
inline void deserialize(parquet::StreamReader &stream, void *dest, int attSize);

// Specialization for int
template<>
inline void deserialize<int>(parquet::StreamReader &stream, void *dest, int attSize) {
    int value;
    stream >> value;
    *reinterpret_cast<int *>(dest) = value;
}

// Specialization for double
template<>
inline void deserialize<double>(parquet::StreamReader &stream, void *dest, int attSize) {
    double value;
    stream >> value;
    *reinterpret_cast<double *>(dest) = value;
}

// Specialization for char
template<>
inline void deserialize<char>(parquet::StreamReader &stream, void *dest, int attSize) {
    std::string value;
    stream >> value;
    *reinterpret_cast<char *>(dest) = value[0];
}

// Specialization for std::string
template<>
inline void deserialize<std::string>(parquet::StreamReader &stream, void *dest, int attSize) {
    std::string value;
    stream >> value;

    // Resize or pad the string to fixed size
    if (value.size() > static_cast<size_t>(attSize)) {
        value.resize(attSize);
    } else if (value.size() < static_cast<size_t>(attSize)) {
        value.append(attSize - value.size(), '\0');
    }

    std::memcpy(dest, value.c_str(), attSize);
}

#endif // DESERIALIZERS_PARQUET_H
