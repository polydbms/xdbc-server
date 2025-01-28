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

    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "comp", "start"});

    int inBufferId;
    int outBufferId;
    long compressedBuffers = 0;

    while (true) {

        inBufferId = xdbcEnv->compBufferPtr->pop();

        if (inBufferId == -1)
            break;

        xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "comp", "pop"});

        if (!Compressor::getCompId(xdbcEnv->compression_algorithm)) {
            //nothing to do, forward buffer
            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "comp", "push"});

            xdbcEnv->sendBufferPtr->push(inBufferId);
        } else {

            outBufferId = xdbcEnv->freeBufferPtr->pop();
            //TODO: replace function with a hashmap or similar
            //0 nocomp, 1 zstd, 2 snappy, 3 lzo, 4 lz4, 5 zlib, 6 cols
            size_t compId = Compressor::getCompId(xdbcEnv->compression_algorithm);

            //spdlog::get("XDBC.SERVER")->warn("Send thread {0} entering compression", thr);
            auto headIn = reinterpret_cast<Header *>(bp[inBufferId].data());

            auto decompressedPtr = bp[inBufferId].data() + sizeof(Header);
            std::array<size_t, MAX_ATTRIBUTES> compressed_sizes = Compressor::compress_buffer(
                    xdbcEnv->compression_algorithm, decompressedPtr, bp[outBufferId].data() + sizeof(Header),
                    headIn->totalSize,
                    xdbcEnv->tuples_per_buffer, xdbcEnv->schema);

            size_t totalSize = 0;
            //TODO: check if schema larger than MAX_ATTRIBUTES

            if (compId < 6)
                totalSize = compressed_sizes[0];
            else {
                for (int i = 0; i < xdbcEnv->schema.size(); i++) {
                    totalSize += compressed_sizes[i];
                }
            }

            if (totalSize >= xdbcEnv->buffer_size * 1024 || totalSize <= 0) {
                spdlog::get("XDBC.SERVER")->error("Compress thread {} with comp {} invalid size {}/{}",
                                                  thr, compId, totalSize, xdbcEnv->buffer_size * 1024);
                compId = 0;
            }

            //TODO: create more sophisticated header with checksum etc

            Header head{};
            head.totalTuples = headIn->totalTuples;
            head.compressionType = compId;
            head.totalSize = totalSize;
            head.uncompressedSize = headIn->totalSize;
            head.intermediateFormat = headIn->intermediateFormat;
            //head.crc = compute_crc(bp[bufferId].data(), totalSize);
            //head.attributeComp;
            //head.attributeSize = compressed_sizes;

            std::copy(compressed_sizes.begin(), compressed_sizes.end(), head.attributeSize);

            xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "comp", "push"});

            if (compId == 0) {
                //comp unsuccessful
                std::memcpy(bp[inBufferId].data(), &head, sizeof(Header));
                //forward in buffer with new header
                xdbcEnv->sendBufferPtr->push(inBufferId);
                //release out buffer
                xdbcEnv->freeBufferPtr->push(outBufferId);
            } else {
                /*spdlog::get("XDBC.SERVER")->warn("Entering compid {}, totalSize {}, tuples {}, freeQ size {}", compId,
                                                 head.totalSize, head.totalTuples, xdbcEnv->freeBufferPtr->size());*/
                std::memcpy(bp[outBufferId].data(), &head, sizeof(Header));
                xdbcEnv->sendBufferPtr->push(outBufferId);
                xdbcEnv->freeBufferPtr->push(inBufferId);
                //spdlog::get("XDBC.SERVER")->warn("Exiting, free size {}", xdbcEnv->freeBufferPtr->size());

            }
            compressedBuffers++;
        }
    }
    xdbcEnv->pts->push(ProfilingTimestamps{std::chrono::high_resolution_clock::now(), thr, "comp", "end"});

    //notify that we finished
    xdbcEnv->finishedCompThreads.fetch_add(1);
    if (xdbcEnv->finishedCompThreads == xdbcEnv->compression_parallelism) {
        for (int i = 0; i < xdbcEnv->network_parallelism; i++)
            xdbcEnv->sendBufferPtr->push(-1);
    }

}

size_t Compressor::getCompId(const std::string &name) {

    if (name == "nocomp")
        return 0;
    else if (name == "zstd")
        return 1;
    else if (name == "snappy")
        return 2;
    else if (name == "lzo")
        return 3;
    else if (name == "lz4")
        return 4;
    else if (name == "zlib")
        return 5;
    else if (name == "cols")
        return 6;

    return 0;
}
//TODO: recycle compression resources like context, temp buffer etc
//TODO: check that we dont exceed size by calculating the max compressed size funcs of the libs

size_t Compressor::compress_zstd(void *data, void *dst, size_t size) {
    size_t compressedSize = ZSTD_compress(dst, size, data, size, 1);

    if (ZSTD_isError(compressedSize)) {
        spdlog::get("XDBC.SERVER")->warn("Zstd Compression error: {0}", std::string(ZSTD_getErrorName(compressedSize)));
        return size;
    }

    return compressedSize;
}


size_t Compressor::compress_snappy(void *data, void *dst, size_t size) {
    //size_t maxCompressedSize = snappy::MaxCompressedLength(size);
    size_t compressedSize;
    snappy::RawCompress(static_cast<const char *>(data), size, static_cast<char *>(dst), &compressedSize);

    if (compressedSize >= size) {
        spdlog::get("XDBC.SERVER")->info("Snappy compression not effective. Returning original size.");
        return size;
    }

    return compressedSize;
}


size_t Compressor::compress_lzo(void *src, void *dst, size_t size) {
    //size_t maxCompressedSize = size + (size / 16) + 64 + 3;

    //allocate the work memory required by LZO
    lzo_voidp wrkmem = (lzo_voidp) malloc(LZO1X_1_MEM_COMPRESS);
    if (!wrkmem) {
        spdlog::get("XDBC.SERVER")->warn("lzo compression error: failed to allocate work memory.");
        return size;
    }

    lzo_uint compressedSize;
    int result = lzo1x_1_compress(static_cast<const unsigned char *>(src), size,
                                  static_cast<unsigned char *>(dst), &compressedSize, wrkmem);

    //free the work memory
    free(wrkmem);

    if (result != LZO_E_OK) {
        spdlog::get("XDBC.SERVER")->warn("lzo compression error: result code {0}.", result);
        return size;
    }

    if (compressedSize > size) {
        spdlog::get("XDBC.SERVER")->warn("lzo compression error: compressed size exceeds maximum size: {0}/{1}",
                                         size, compressedSize);
        return size;
    }

    return compressedSize;
}

size_t Compressor::compress_lz4(void *src, void *dst, size_t size) {
    //int maxCompressedSize = LZ4_compressBound(size);
    int compressedSize = LZ4_compress_default(static_cast<const char *>(src), static_cast<char *>(dst), size,
                                              size);

    if (compressedSize <= 0) {
        spdlog::get("XDBC.SERVER")->warn("lz4 compression error: {0}", compressedSize);
        return size;
    }

    if (compressedSize >= static_cast<int>(size)) {
        spdlog::get("XDBC.SERVER")->info("lz4 compression not effective. Returning original size.");
        return size;
    }

    return compressedSize;
}


size_t Compressor::compress_zlib(void *src, void *dst, size_t size) {

    uLongf maxCompressedSize = compressBound(size);

    uLongf compressedSize = maxCompressedSize;
    //maximum compression level
    int compression_level = 9;

    int result = compress2(static_cast<Bytef *>(dst), &compressedSize,
                           static_cast<const Bytef *>(src), size, compression_level);

    if (result != Z_OK) {
        spdlog::get("XDBC.SERVER")->warn("ZLIB compression error: {0}", zError(result));
        return size;
    }

    if (compressedSize >= size) {
        spdlog::get("XDBC.SERVER")->info("ZLIB compression not effective, returning original size.");
        return size;
    }

    return compressedSize;
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

    auto compMeth = getCompId(method);
    switch (compMeth) {
        case 0: {
            ret[0] = size;
            break;
        }
        case 1: {
            ret[0] = compress_zstd(src, dst, size);
            break;
        }
        case 2: {
            ret[0] = compress_snappy(src, dst, size);
            break;
        }
        case 3: {
            ret[0] = compress_lzo(src, dst, size);
            break;
        }
        case 4: {
            ret[0] = compress_lz4(src, dst, size);
            break;
        }
        case 5: {
            ret[0] = compress_zlib(src, dst, size);
            break;
        }
        case 6: {
            ret = compress_cols(src, dst, size, buff_size, schema);
            break;
        }
        default: {
            std::memcpy(dst, src, size);
            ret[0] = size;
            break;
        }
    }


    return ret;
}

//compress floating-point data
static size_t compress(FPZ *fpz, const void *data) {
    size_t size;
    /* write header */
    if (!fpzip_write_header(fpz)) {
        fprintf(stderr, "cannot write header: %s\n", fpzip_errstr[fpzip_errno]);
        return 0;
    }
    //perform actual compression
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
        //TODO: add more attributes (CHAR, STRING)
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





