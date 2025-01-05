#ifndef DESERIALIZERS_ARROW_H
#define DESERIALIZERS_ARROW_H

#include <arrow/api.h>
#include <arrow/builder.h>

// Arrow deserializers for each type
template<typename T>
void deserialize_arrow(const char *src, const char *end, std::shared_ptr<arrow::ArrayBuilder> builder, int attSize,
                       size_t len);

// Specialization for int
template<>
inline void
deserialize_arrow<int>(const char *src, const char *end, std::shared_ptr<arrow::ArrayBuilder> builder, int attSize,
                       size_t len) {
    int value;
    std::from_chars(src, end, value);
    auto int_builder = std::static_pointer_cast<arrow::Int32Builder>(builder);
    auto status = int_builder->Append(value);
}

// Specialization for double
template<>
inline void
deserialize_arrow<double>(const char *src, const char *end, std::shared_ptr<arrow::ArrayBuilder> builder, int attSize,
                          size_t len) {
    double value;
    fast_float::from_chars(src, end, value);
    auto double_builder = std::static_pointer_cast<arrow::DoubleBuilder>(builder);
    auto status = double_builder->Append(value);
}

// Specialization for char
template<>
inline void
deserialize_arrow<char>(const char *src, const char *end, std::shared_ptr<arrow::ArrayBuilder> builder, int attSize,
                        size_t len) {
    std::string value(src, end - src);
    auto fixed_builder = std::static_pointer_cast<arrow::FixedSizeBinaryBuilder>(builder);
    auto status = fixed_builder->Append(value);
}

// Specialization for const char *
template<>
inline void
deserialize_arrow<const char *>(const char *src, const char *end, std::shared_ptr<arrow::ArrayBuilder> builder,
                                int attSize, size_t len) {
    std::string value(src, end - src);
    auto fixed_builder = std::static_pointer_cast<arrow::FixedSizeBinaryBuilder>(builder);
    auto status = fixed_builder->Append(value);
}

#endif // DESERIALIZERS_ARROW_H
