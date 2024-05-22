/*
Low level interface to Meta's zstd library for use in the `zstd` Python library
*/

#include "Python.h"

#include <zstd.h>
#include <zdict.h>

#include <stdio.h>

// Blocks output buffer wrappers
#include "pycore_blocks_output_buffer.h"

#if ZSTD_VERSION_NUMBER < 10400
    #error "CPython requires zstd > 1.4.0"
#endif

// TODO: look at ZSTD_MULTITHREAD
// TODO: version information as module attributes

typedef struct {
    PyTypeObject *zstd_compressor_type;
    PyTypeObject *zstd_decompressor_type;
} _zstd_state;

static inline _zstd_state*
get_zstd_state(PyObject *module)
{
    void *state = PyModule_GetState(module);
    assert(state != NULL);
    return (_zstd_state *)state;
}

typedef struct {
    PyObject_HEAD
    ZSTD_CCtx stream;
    int compressionLevel;
    PyThread_type_lock lock;
} ZSTDCompressor;

typedef struct {
    PyObject_HEAD
    ZSTD_DCtx stream;
    PyThread_type_lock lock;
} ZSTDDecompressor;

static int
grow_buffer(PyObject **buf, Py_ssize_t max_length)
{
    Py_ssize_t size = PyBytes_GET_SIZE(*buf);
    Py_ssize_t newsize = size + (size >> 3) + 6;

    if (max_length > 0 && newsize > max_length) {
        newsize = max_length;
    }

    return _PyBytes_Resize(buf, newsize);
}

/* ZSTDCompressor class*/

static PyObject *
compress(ZSTDCompressor *c, char *data, size_t len) {
    PyObject *result;
    Py_ssize_t data_size = 0;

    size_t const out_buf_size = ZSTD_CStreamOutSize();
    result = PyBytes_FromStringAndSize(NULL, out_buf_size);
    if (result == NULL) {
        return NULL;
    }

    char* out_buf =  (char *)PyBytes_AS_STRING(result);

    ZSTD_inBuffer inBuf = { data, len, 0};
    ZSTD_outBuffer outBuf = { out_buf, out_buf_size, 0 };

    int finished;
    do {
        Py_BEGIN_ALLOW_THREADS
        size_t remaining = ZSTD_compressStream2(c->stream, &outBuf, &inBuf, ZSTD_e_continue);
        // TODO: check remaining isn't an error
        // All input bytes written, we're done here.
        if (inBuf.pos == inBuf.size) {
            break;
        }
        //if (outBuf.size - outBuf.pos < )
        Py_END_ALLOW_THREADS
    } while (!finished);


error:

    return NULL;
}