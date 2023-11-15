#include "Compressor.h"
#include <lz4.h>
#include <zstd.h>
#include <snappy.h>
#include <lzo/lzo1x.h>
#include <lz4.h>
#include <zlib.h>
#include "spdlog/spdlog.h"
#include <fastpfor/codecfactory.h>
#include <fastpfor/deltautil.h>
#include <fpzip.h>

Compressor::Compressor(RuntimeEnv &xdbcEnv) :
        xdbcEnv(&xdbcEnv),
        bp(*xdbcEnv.bpPtr) {

}

void Compressor::compress(int thr, const std::string &compName) {

    int emptyCtr = 0;
    int bufferId;
    int netQ = 0;

    while (emptyCtr < xdbcEnv->deser_parallelism) {
        auto start_wait = std::chrono::high_resolution_clock::now();

        bufferId = xdbcEnv->compBufferPtr[thr]->pop();

        auto duration_wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start_wait).count();
        xdbcEnv->compression_wait_time.fetch_add(duration_wait_microseconds, std::memory_order_relaxed);

        if (bufferId == -1)
            emptyCtr++;
        else {

            auto start = std::chrono::high_resolution_clock::now();

            //TODO: replace function with a hashmap or similar
            //0 nocomp, 1 zstd, 2 snappy, 3 lzo, 4 lz4, 5 zlib, 6 cols
            size_t compId = Compressor::getCompId(xdbcEnv->compression_algorithm);

            //spdlog::get("XDBC.SERVER")->warn("Send thread {0} entering compression", thr);

            std::array<size_t, MAX_ATTRIBUTES> compressed_sizes = Compressor::compress_buffer(
                    xdbcEnv->compression_algorithm, bp[bufferId].data(), bp[bufferId].data() + sizeof(Header),
                    xdbcEnv->buffer_size * xdbcEnv->tuple_size,
                    xdbcEnv->buffer_size, xdbcEnv->schema);

            auto duration_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - start).count();
            xdbcEnv->compression_time.fetch_add(duration_microseconds, std::memory_order_relaxed);


            size_t totalSize = 0;
            //TODO: check if schema larger than MAX_ATTRIBUTES

            if (xdbcEnv->compression_algorithm == "cols" &&
                compressed_sizes[0] == xdbcEnv->buffer_size * xdbcEnv->tuple_size)
                totalSize = compressed_sizes[0];
            else {
                for (int i = 0; i < xdbcEnv->schema.size(); i++) {
                    totalSize += compressed_sizes[i];
                }
            }

            if (totalSize > xdbcEnv->buffer_size * xdbcEnv->tuple_size) {
                spdlog::get("XDBC.SERVER")->warn("Compress thread {0} compression more than buffer", thr);
                compId = 0;
            }
            if (totalSize == xdbcEnv->buffer_size * xdbcEnv->tuple_size) {
                compId = 0;
            }

            if (totalSize <= 0)
                spdlog::get("XDBC.SERVER")->error("Compress thread {0} compression: {1}, totalSize: {2}",
                                                  thr, compId, totalSize);


            //TODO: create more sophisticated header with checksum etc

            Header head;
            head.compressionType = compId;
            head.totalSize = totalSize;
            head.intermediateFormat = static_cast<size_t>(xdbcEnv->iformat);
            //head.crc = compute_crc(bp[bufferId].data(), totalSize);
            head.attributeComp;
            //head.attributeSize = compressed_sizes;

            std::copy(compressed_sizes.begin(), compressed_sizes.end(), head.attributeSize);
            std::memcpy(bp[bufferId].data(), &head, sizeof(Header));
            xdbcEnv->sendBufferPtr[netQ]->push(bufferId);
            netQ++;
            if (netQ == xdbcEnv->network_parallelism)
                netQ = 0;
        }
    }

    for (int i = 0; i < xdbcEnv->network_parallelism; i++)
        xdbcEnv->sendBufferPtr[i]->push(-1);


}

size_t Compressor::getCompId(const std::string &name) {

    if (name == "nocomp")
        return 0;
    if (name == "zstd")
        return 1;
    if (name == "snappy")
        return 2;
    if (name == "lzo")
        return 3;
    if (name == "lz4")
        return 4;
    if (name == "zlib")
        return 5;
    if (name == "cols")
        return 6;

    return 0;
}
//TODO: examine in-place compression to avoid copies
//TODO: recycle compression resources like context, temp buffer etc

size_t Compressor::compress_zstd(void *data, void *dst, size_t size) {

    // Calculate the maximum compressed size
    size_t maxCompressedSize = ZSTD_compressBound(size);

    // Create a temporary buffer for the compressed data
    std::vector<char> compressedBuffer(maxCompressedSize);

    // Compress the data
    size_t compressedSize = ZSTD_compress(
            compressedBuffer.data(), maxCompressedSize, data, size, /* compression level */ 1);

    // Check if compression was successful
    if (ZSTD_isError(compressedSize)) {
        // Compression error occurred
        spdlog::get("XDBC.SERVER")->warn("Zstd Compression error: {0}", std::string(ZSTD_getErrorName(compressedSize)));
        //TODO: handle error
        return size;
    }

    // Copy the compressed data back to the original buffer
    std::memcpy(dst, compressedBuffer.data(), compressedSize);

    compressedBuffer.clear();
    compressedBuffer.shrink_to_fit();

    return compressedSize;
}


size_t Compressor::compress_snappy(void *data, void *dst, size_t size) {
    // Calculate the maximum compressed size
    size_t maxCompressedSize = snappy::MaxCompressedLength(size);

    // Create a temporary buffer for the compressed data
    std::vector<char> compressedBuffer(maxCompressedSize);

    // Compress the data
    size_t compressedSize;
    snappy::RawCompress(static_cast<const char *>(data), size, compressedBuffer.data(), &compressedSize);

    // Check if compression was successful
    if (compressedSize > maxCompressedSize) {
        // Compression error occurred
        spdlog::get("XDBC.SERVER")->warn("Snappy compression  compressed size exceeds maximum size: {0}",
                                         compressedSize);

        return size;
    }

    // Copy the compressed data back to the original buffer
    std::memcpy(dst, compressedBuffer.data(), compressedSize);

    return compressedSize;
}

size_t Compressor::compress_lzo(void *src, void *dst, size_t size) {
    // Calculate the maximum compressed size
    size_t maxCompressedSize = size + (size / 16) + 64 + 3;

    // Create a temporary buffer for the compressed data
    std::vector<unsigned char> compressedBuffer(maxCompressedSize);

    // Compress the data
    lzo_uint compressedSize;
    lzo_voidp wrkmem = (lzo_voidp) malloc(LZO1X_1_MEM_COMPRESS);
    lzo1x_1_compress(static_cast<const unsigned char *>(src), size, compressedBuffer.data(), &compressedSize, wrkmem);

    // Check if compression was successful
    if (compressedSize > maxCompressedSize) {
        // Compression error occurred
        spdlog::get("XDBC.SERVER")->warn("lzo compression error: compressed size exceeds maximum size: {0}/{1}",
                                         maxCompressedSize, compressedSize);

        return size;
    }

    // Copy the compressed data back to the original buffer
    std::memcpy(dst, compressedBuffer.data(), compressedSize);

    return compressedSize;
}

size_t Compressor::compress_lz4(void *src, void *dst, size_t size) {
    // Calculate the maximum compressed size
    int maxCompressedSize = LZ4_compressBound(size);

    // Create a temporary buffer for the compressed data
    std::vector<char> compressedBuffer(maxCompressedSize);

    // Compress the data
    int compressedSize = LZ4_compress_default(static_cast<const char *>(src), compressedBuffer.data(), size,
                                              maxCompressedSize);

    // Check if compression was successful
    if (compressedSize < 0) {
        // Compression error occurred
        spdlog::get("XDBC.SERVER")->warn("lz4 compression error: {0} ", compressedSize);
        //throw std::runtime_error("Compression error: failed to compress data");
        return size;
    }

    // Copy the compressed data back to the original buffer
    std::memcpy(dst, compressedBuffer.data(), compressedSize);

    return compressedSize;
}

size_t Compressor::compress_zlib(void *src, void *dst, size_t size) {

    uLongf compressed_size = compressBound(size);

    std::vector<Bytef> compressed_data(compressed_size);

    int compression_level = 9;

    int result = compress2(compressed_data.data(), &compressed_size,
                           static_cast<const Bytef *>(src), size, compression_level);

    if (compressed_size > size)
        return size;

    if (result == Z_OK) {
        compressed_data.resize(compressed_size);
        std::memcpy(dst, compressed_data.data(), compressed_size);
    } else {
        spdlog::get("XDBC.SERVER")->warn("ZLIB: not OK, error {0} ", zError(result));
        return size;
    }

    return compressed_size;
}


std::array<size_t, MAX_ATTRIBUTES>
Compressor::compress_buffer(const std::string &method, void *src, void *dst, size_t size, size_t buff_size,
                            const std::vector<SchemaAttribute> &schema) {


    //1 zstd
    //2 snappy
    //3 lzo
    //4 lz4
    //5 zlib
    //6 cols

    std::array<size_t, MAX_ATTRIBUTES> ret{};
    for (size_t i = 0; i < MAX_ATTRIBUTES; i++)
        ret[i] = 0;

    if (method == "zstd")
        ret[0] = compress_zstd(src, dst, size);
    else if (method == "snappy")
        ret[0] = compress_snappy(src, dst, size);
    else if (method == "lzo")
        ret[0] = compress_lzo(src, dst, size);
    else if (method == "lz4")
        ret[0] = compress_lz4(src, dst, size);
    else if (method == "zlib")
        ret[0] = compress_zlib(src, dst, size);
    else if (method == "cols") {
        ret = compress_cols(src, dst, size, buff_size, schema);
        //no method picked, not compressing
    } else {
        std::memcpy(dst, src, size);
        ret[0] = size;
    }
    return ret;
}

/* compress floating-point data */
static size_t compress(FPZ *fpz, const void *data) {
    size_t size;
    /* write header */
    if (!fpzip_write_header(fpz)) {
        fprintf(stderr, "cannot write header: %s\n", fpzip_errstr[fpzip_errno]);
        return 0;
    }
    /* perform actual compression */
    size = fpzip_write(fpz, data);
    if (!size) {
        spdlog::get("XDBC.SERVER")->error("Compressor: fpzip failed, error: {0}", fpzip_errstr[fpzip_errno]);
        return 0;
    }
    return size;
}

size_t compressIntColumn(uint32_t *in, void *out, size_t buff_size) {

    //memcpy(out, in, buffer_size * 48);
    //return buffer_size;
    //spdlog::get("XDBC.SERVER")->error("Compressor: Entered compressIntCol, input size: {0}", size);

    std::vector<int> compressedData(buff_size);

    using namespace FastPForLib;
    CODECFactory factory;

    IntegerCODEC &codec = *factory.getFromName("simdfastpfor256");

    //std::vector<uint32_t> compressed_output(buffer_size + 1024);

    size_t compressedsize = buff_size + 1024;
    codec.encodeArray(in, buff_size, reinterpret_cast<uint32_t *>(out),
                      compressedsize);
    //compressed_output.resize(compressedsize);
    //compressed_output.shrink_to_fit();

    //memcpy(out, compressedData.data(), compressedsize * 4);

    //spdlog::get("XDBC.SERVER")->error("compressedsize: {0}, in first value: {1}", compressedsize, uint_data[0]);
    //TODO: remove code for decompression
    //auto testData = reinterpret_cast<uint32_t *>(out);
    /*std::vector<uint32_t> mydataback(buffer_size);
    size_t recoveredsize = mydataback.size();

    codec.decodeArray(testData, compressedsize,
                      mydataback.data(), recoveredsize);

    spdlog::get("XDBC.SERVER")->error("Test in: {0}, out:{1}", in[0], mydataback[0]);

    if (mydataback[0] != testData[0]) {
        spdlog::get("XDBC.SERVER")->error("Unequal values: {0}!={1}", mydataback[0], testData[0]);
    }*/
    //spdlog::get("XDBC.SERVER")->error("Compressor: compressed input: {0}, output: {1}", buffer_size, compressedsize);
    //spdlog::get("XDBC.SERVER")->error("Decompressed data: {0}", mydataback[0]);

    return compressedsize;
}

size_t compressDoubleColumn(const double *in, void *out, size_t size) {


    int status;

    size_t inbytes = size * sizeof(double);
    size_t bufbytes = 1024 + inbytes;
    size_t outbytes = 0;
    //void *buffer = malloc(bufbytes);

    /* compress to memory */
    FPZ *fpz = fpzip_write_to_buffer(out, bufbytes);
    fpz->type = FPZIP_TYPE_DOUBLE;
    fpz->prec = 0;
    fpz->nx = size;
    fpz->ny = 1;
    fpz->nz = 1;
    fpz->nf = 1;
    outbytes = compress(fpz, in);
    status = (0 < outbytes && outbytes <= bufbytes);
    fpzip_write_close(fpz);


    return outbytes;
}

std::array<size_t, MAX_ATTRIBUTES> Compressor::compress_cols(void *src, void *dst, size_t size, size_t buff_size,
                                                             const std::vector<SchemaAttribute> &schema) {

    std::array<size_t, MAX_ATTRIBUTES> compressedColumns{};

    //TODO: get schema size automatically
    std::vector<std::byte> outputBuf(size);
    auto compressedPtr = outputBuf.data();

    int attributeNum = 0;
    size_t totalSize = 0;

    int bytesWritten = 0;
    for (const auto &attribute: schema) {

        //spdlog::get("XDBC.SERVER")->warn("handling attribute: {0} with attributeNum: {1}", std::get<0>(attribute), attributeNum);
        size_t compressedDataSize = 0;
        if (attribute.tpe == "INT") {
            uint32_t *decompressedPtr = reinterpret_cast<uint32_t *>(reinterpret_cast<std::byte *>(src) +
                                                                     buff_size * bytesWritten);
            size_t compressedDataSizeElements = compressIntColumn(decompressedPtr, compressedPtr, buff_size);
            compressedDataSize = compressedDataSizeElements * attribute.size;
            bytesWritten += 4;

            //compressedDataSize += 4 * buffer_size;
            //spdlog::get("XDBC.SERVER")->warn("compressedDataSize: {0}, added {1} ", compressedDataSize, 4*buffer_size);
        } else if (attribute.tpe == "DOUBLE") {
            //TODO: refactor compress fpzip in other function
            double *decompressedPtr = reinterpret_cast<double *>(reinterpret_cast<std::byte *>(src) +
                                                                 buff_size * bytesWritten);

            //compressedDataSize = compress_zstd(decompressedPtr, compressedPtr, buff_size * 8);
            compressedDataSize = compressDoubleColumn(decompressedPtr, compressedPtr, buff_size);
            bytesWritten += 8;

            //auto startPtr = reinterpret_cast<double *>(data) + buff_size * attributeNum;


            //compressedDataSize += 8 * buffer_size;
        }
        compressedColumns[attributeNum] = compressedDataSize;
        totalSize += compressedDataSize;
        attributeNum++;
        compressedPtr += compressedDataSize;
        //spdlog::get("XDBC.SERVER")->warn("compressedDataSize: {0}", compressedDataSize);
    }
    if (totalSize >= size) {
        spdlog::get("XDBC.SERVER")->warn("Compressor: buffer overflow {0}/{1}", totalSize, size);
        //set first entry to total size, handle to send uncompressed
        compressedColumns[0] = size;
    } else
        std::memcpy(dst, outputBuf.data(), totalSize);

    return compressedColumns;
}





