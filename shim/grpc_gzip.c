/* gzip compress/decompress for gRPC message payloads, via zlib. */

#include <stdint.h>
#include <string.h>
#include <zlib.h>

int grpc_gzip_bound(int src_len) {
    if (src_len < 0) {
        return -1;
    }
    uLong bound = compressBound((uLong)src_len);
    /* gzip header/trailer is a few extra bytes beyond zlib's bound. */
    if (bound > 0x7fffffff - 32) {
        return -1;
    }
    return (int)bound + 32;
}

int grpc_gzip_compress(
    const uint8_t *src,
    int src_len,
    uint8_t *dst,
    int dst_cap
) {
    z_stream strm;
    int rc;

    if (src_len < 0 || dst_cap < 0 || (src_len > 0 && src == 0) || dst == 0) {
        return -1;
    }
    memset(&strm, 0, sizeof(strm));
    /* windowBits = 15 + 16: gzip wrapper (RFC 1952). */
    if (deflateInit2(
            &strm,
            Z_DEFAULT_COMPRESSION,
            Z_DEFLATED,
            16 + MAX_WBITS,
            8,
            Z_DEFAULT_STRATEGY
        ) != Z_OK) {
        return -1;
    }
    strm.next_in = (Bytef *)src;
    strm.avail_in = (uInt)src_len;
    strm.next_out = dst;
    strm.avail_out = (uInt)dst_cap;
    rc = deflate(&strm, Z_FINISH);
    if (rc != Z_STREAM_END) {
        deflateEnd(&strm);
        return -1;
    }
    rc = (int)strm.total_out;
    deflateEnd(&strm);
    return rc;
}

int grpc_gzip_decompress(
    const uint8_t *src,
    int src_len,
    uint8_t *dst,
    int dst_cap
) {
    z_stream strm;
    int rc;

    if (src_len < 0 || dst_cap < 0 || (src_len > 0 && src == 0) || dst == 0) {
        return -1;
    }
    memset(&strm, 0, sizeof(strm));
    if (inflateInit2(&strm, 16 + MAX_WBITS) != Z_OK) {
        return -1;
    }
    strm.next_in = (Bytef *)src;
    strm.avail_in = (uInt)src_len;
    strm.next_out = dst;
    strm.avail_out = (uInt)dst_cap;
    rc = inflate(&strm, Z_FINISH);
    if (rc == Z_BUF_ERROR && strm.avail_in > 0) {
        inflateEnd(&strm);
        return -2;
    }
    if (rc != Z_STREAM_END) {
        inflateEnd(&strm);
        return -1;
    }
    rc = (int)strm.total_out;
    inflateEnd(&strm);
    return rc;
}
