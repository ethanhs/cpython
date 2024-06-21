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

#pragma region "Utilities"
#define ACQUIRE_LOCK(obj) do { \
    if (!PyThread_acquire_lock((obj)->lock, 0)) { \
        Py_BEGIN_ALLOW_THREADS \
        PyThread_acquire_lock((obj)->lock, 1); \
        Py_END_ALLOW_THREADS \
    } } while (0)
#define RELEASE_LOCK(obj) PyThread_release_lock((obj)->lock)

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

#pragma endregion

#pragma region "Module state"

typedef struct {
    PyTypeObject *zstd_compressor_type;
    PyTypeObject *zstd_decompressor_type;
    PyTypeObject *zstd_dict_type;
    PyObject *error;
} _zstd_state;

static inline _zstd_state*
get_zstd_state(PyObject *module)
{
    void *state = PyModule_GetState(module);
    assert(state != NULL);
    return (_zstd_state *)state;
}
#pragma endregion

#pragma region "Parameters"
/* -------------------------
     Parameters from zstd
   ------------------------- */
typedef struct {
    const int parameter;
    const char parameter_name[32];
} ParameterInfo;

static const ParameterInfo cp_list[] =
{
    {ZSTD_c_compressionLevel, "compressionLevel"},
    {ZSTD_c_windowLog,        "windowLog"},
    {ZSTD_c_hashLog,          "hashLog"},
    {ZSTD_c_chainLog,         "chainLog"},
    {ZSTD_c_searchLog,        "searchLog"},
    {ZSTD_c_minMatch,         "minMatch"},
    {ZSTD_c_targetLength,     "targetLength"},
    {ZSTD_c_strategy,         "strategy"},

#if ZSTD_VERSION_NUMBER >= 10506
    {ZSTD_c_targetCBlockSize, "targetCBlockSize"},
#endif

    {ZSTD_c_enableLongDistanceMatching, "enableLongDistanceMatching"},
    {ZSTD_c_ldmHashLog,       "ldmHashLog"},
    {ZSTD_c_ldmMinMatch,      "ldmMinMatch"},
    {ZSTD_c_ldmBucketSizeLog, "ldmBucketSizeLog"},
    {ZSTD_c_ldmHashRateLog,   "ldmHashRateLog"},

    {ZSTD_c_contentSizeFlag,  "contentSizeFlag"},
    {ZSTD_c_checksumFlag,     "checksumFlag"},
    {ZSTD_c_dictIDFlag,       "dictIDFlag"},

    {ZSTD_c_nbWorkers,        "nbWorkers"},
    {ZSTD_c_jobSize,          "jobSize"},
    {ZSTD_c_overlapLog,       "overlapLog"}
};

static const ParameterInfo dp_list[] =
{
    {ZSTD_d_windowLogMax, "windowLogMax"}
};

/* Format an user friendly error message. */
void
set_parameter_error(const _zstd_state* const state, int is_compress,
                    int key_v, int value_v)
{
    ParameterInfo const *list;
    int list_size;
    char const *name;
    char *type;
    ZSTD_bounds bounds;
    int i;
    char pos_msg[128];

    if (is_compress) {
        list = cp_list;
        list_size = Py_ARRAY_LENGTH(cp_list);
        type = "compression";
    } else {
        list = dp_list;
        list_size = Py_ARRAY_LENGTH(dp_list);
        type = "decompression";
    }

    /* Find parameter's name */
    name = NULL;
    for (i = 0; i < list_size; i++) {
        if (key_v == (list+i)->parameter) {
            name = (list+i)->parameter_name;
            break;
        }
    }

    /* Unknown parameter */
    if (name == NULL) {
        PyOS_snprintf(pos_msg, sizeof(pos_msg),
                      "unknown parameter (key %d)", key_v);
        name = pos_msg;
    }

    /* Get parameter bounds */
    if (is_compress) {
        bounds = ZSTD_cParam_getBounds(key_v);
    } else {
        bounds = ZSTD_dParam_getBounds(key_v);
    }
    if (ZSTD_isError(bounds.error)) {
        PyErr_Format(state->error,
                     "Zstd %s parameter \"%s\" is invalid. (zstd v%s)",
                     type, name, ZSTD_versionString());
        return;
    }

    /* Error message */
    PyErr_Format(state->error,
                 "Error when setting zstd %s parameter \"%s\", it "
                 "should %d <= value <= %d, provided value is %d. "
                 "(zstd v%s, %d-bit build)",
                 type, name,
                 bounds.lowerBound, bounds.upperBound, value_v,
                 ZSTD_versionString(), 8*(int)sizeof(Py_ssize_t));
}
#pragma endregion

#pragma region "Error handling"

typedef enum {
    ERR_DECOMPRESS,
    ERR_COMPRESS,
    ERR_SET_PLEDGED_INPUT_SIZE,

    ERR_LOAD_D_DICT,
    ERR_LOAD_C_DICT,

    ERR_GET_C_BOUNDS,
    ERR_GET_D_BOUNDS,
    ERR_SET_C_LEVEL,

    ERR_TRAIN_DICT,
    ERR_FINALIZE_DICT
} error_type;

typedef enum {
    DICT_TYPE_DIGESTED = 0,
    DICT_TYPE_UNDIGESTED = 1,
    DICT_TYPE_PREFIX = 2
} dictionary_type;

/* Format error message and set ZstdError. */
void
set_zstd_error(const _zstd_state* const state,
               const error_type type, const size_t zstd_ret)
{
    char buf[128];
    char *msg;
    assert(ZSTD_isError(zstd_ret));

    switch (type)
    {
    case ERR_DECOMPRESS:
        msg = "Unable to decompress zstd data: %s";
        break;
    case ERR_COMPRESS:
        msg = "Unable to compress zstd data: %s";
        break;
    case ERR_SET_PLEDGED_INPUT_SIZE:
        msg = "Unable to set pledged uncompressed content size: %s";
        break;

    case ERR_LOAD_D_DICT:
        msg = "Unable to load zstd dictionary or prefix for decompression: %s";
        break;
    case ERR_LOAD_C_DICT:
        msg = "Unable to load zstd dictionary or prefix for compression: %s";
        break;

    case ERR_GET_C_BOUNDS:
        msg = "Unable to get zstd compression parameter bounds: %s";
        break;
    case ERR_GET_D_BOUNDS:
        msg = "Unable to get zstd decompression parameter bounds: %s";
        break;
    case ERR_SET_C_LEVEL:
        msg = "Unable to set zstd compression level: %s";
        break;

    case ERR_TRAIN_DICT:
        msg = "Unable to train zstd dictionary: %s";
        break;
    case ERR_FINALIZE_DICT:
        msg = "Unable to finalize zstd dictionary: %s";
        break;

    default:
        Py_UNREACHABLE();
    }
    PyOS_snprintf(buf, sizeof(buf), msg, ZSTD_getErrorName(zstd_ret));
    PyErr_SetString(state->error, buf);
}
#pragma endregion

#pragma region "Objects"
typedef struct {
    PyObject_HEAD
    /* Thread lock for compressing */
    PyThread_type_lock lock;

    /* Compression context */
    ZSTD_CCtx *cctx;

    /* ZstdDict object in use */
    PyObject *dict;

    /* Last mode, initialized to ZSTD_e_end */
    int last_mode;

    /* (nbWorker >= 1) ? 1 : 0 */
    int use_multithread;

    /* Compression level */
    int compression_level;

    _zstd_state *module_state;
} ZSTDCompressor;

typedef struct {
    PyObject_HEAD

    /* Thread lock for compressing */
    PyThread_type_lock lock;

    /* Decompression context */
    ZSTD_DCtx *dctx;

    /* ZstdDict object in use */
    PyObject *dict;

    /* Unconsumed input data */
    char *input_buffer;
    size_t input_buffer_size;
    size_t in_begin, in_end;

    /* Unused data */
    PyObject *unused_data;

    /* 0 if decompressor has (or may has) unconsumed input data, 0 or 1. */
    char needs_input;

    /* For EndlessZstdDecompressor, 0 or 1.
       1 when both input and output streams are at a frame edge, means a
       frame is completely decoded and fully flushed, or the decompressor
       just be initialized. */
    char at_frame_edge;

    /* For ZstdDecompressor, 0 or 1.
       1 means the end of the first frame has been reached. */
    char eof;

    /* Used for fast reset above three variables */
    char _unused_char_for_align;

    _zstd_state *module_state;
} ZSTDDecompressor;

#pragma endregion

/* ZSTDCompressor class*/
#pragma region "ZSTDCompressor"

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
        size_t remaining = ZSTD_compressStream2(c->cctx, &outBuf, &inBuf, ZSTD_e_continue);
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

PyObject *
compress_impl(ZSTDCompressor *self, Py_buffer *data,
              const ZSTD_EndDirective end_directive, const int rich_mem)
{
    ZSTD_inBuffer in;
    ZSTD_outBuffer out;
    _BlocksOutputBuffer buffer = {.list = NULL};
    _zstd_state *state = PyType_GetModuleState(Py_TYPE(self));
    size_t zstd_ret;
    PyObject *ret;

    /* Prepare input & output buffers */
    if (data != NULL) {
        in.src = data->buf;
        in.size = data->len;
        in.pos = 0;
    } else {
        in.src = &in;
        in.size = 0;
        in.pos = 0;
    }

    if (rich_mem) {
        /* Calculate output buffer's size */
        size_t output_buffer_size = ZSTD_compressBound(in.size);
        if (output_buffer_size > (size_t) PY_SSIZE_T_MAX) {
            PyErr_NoMemory();
            goto error;
        }

        if (OutputBuffer_InitWithSize(&buffer, &out, -1,
                                      (Py_ssize_t) output_buffer_size) < 0) {
            goto error;
        }
    } else {
        if (OutputBuffer_InitAndGrow(&buffer, &out, -1) < 0) {
            goto error;
        }
    }

    /* zstd stream compress */
    while (1) {
        Py_BEGIN_ALLOW_THREADS
        zstd_ret = ZSTD_compressStream2(self->cctx, &out, &in, end_directive);
        Py_END_ALLOW_THREADS

        /* Check error */
        if (ZSTD_isError(zstd_ret)) {
            STATE_FROM_OBJ(self);
            set_zstd_error(state, ERR_COMPRESS, zstd_ret);
            goto error;
        }

        /* Finished */
        if (zstd_ret == 0) {
            break;
        }

        /* Output buffer should be exhausted, grow the buffer. */
        assert(out.pos == out.size);
        if (out.pos == out.size) {
            if (OutputBuffer_Grow(&buffer, &out) < 0) {
                goto error;
            }
        }
    }

    /* Return a bytes object */
    ret = OutputBuffer_Finish(&buffer, &out);
    if (ret != NULL) {
        return ret;
    }

error:
    OutputBuffer_OnError(&buffer);
    return NULL;
}

static PyObject *
compress_mt_continue_impl(ZSTDCompressor *self, Py_buffer *data)
{
    ZSTD_inBuffer in;
    ZSTD_outBuffer out;
    _BlocksOutputBuffer buffer = {.list = NULL};
    _zstd_state *state = PyType_GetModuleState(Py_TYPE(self));
    size_t zstd_ret;
    PyObject *ret;

    /* Prepare input & output buffers */
    in.src = data->buf;
    in.size = data->len;
    in.pos = 0;

    if (OutputBuffer_InitAndGrow(&buffer, &out, -1) < 0) {
        goto error;
    }

    /* zstd stream compress */
    while (1) {
        Py_BEGIN_ALLOW_THREADS
        do {
            zstd_ret = ZSTD_compressStream2(self->cctx, &out, &in, ZSTD_e_continue);
        } while (out.pos != out.size && in.pos != in.size && !ZSTD_isError(zstd_ret));
        Py_END_ALLOW_THREADS

        /* Check error */
        if (ZSTD_isError(zstd_ret)) {
            STATE_FROM_OBJ(self);
            set_zstd_error(state, ERR_COMPRESS, zstd_ret);
            goto error;
        }

        /* Like compress_impl(), output as much as possible. */
        if (out.pos == out.size) {
            if (OutputBuffer_Grow(&buffer, &out) < 0) {
                goto error;
            }
        } else if (in.pos == in.size) {
            /* Finished */
            assert(mt_continue_should_break(&in, &out));
            break;
        }
    }

    /* Return a bytes object */
    ret = OutputBuffer_Finish(&buffer, &out);
    if (ret != NULL) {
        return ret;
    }

error:
    OutputBuffer_OnError(&buffer);
    return NULL;
}


/*[clinic input]
_zstd.ZSTDCompressor.compress

    data: Py_buffer
    mode: int
        Can be these 3 values .CONTINUE, .FLUSH_BLOCK, .FLUSH_FRAME
    /

Provide data to the compressor object.

Return a chunk of compressed data if possible, or b'' otherwise.
[clinic start generated code]*/

static PyObject *
_zstd_ZSTDCompressor_compress_impl(ZSTDCompressor *self, Py_buffer *data, int mode)
/*[clinic end generated code: output=31f615136963e00f input=64019eac7f2cc8d0]*/
{
    PyObject *ret;
    /* Check mode value */
    if (mode != ZSTD_e_continue &&
        mode != ZSTD_e_flush &&
        mode != ZSTD_e_end)
    {
        PyErr_SetString(PyExc_ValueError,
                        "mode argument wrong value, it should be one of "
                        "ZSTDCompressor.CONTINUE, ZSTDCompressor.FLUSH_BLOCK, "
                        "ZSTDCompressor.FLUSH_FRAME.");
        PyBuffer_Release(&data);
        return NULL;
    }

    /* Thread-safe code */
    ACQUIRE_LOCK(self);

    /* Compress */
    if (self->use_multithread && mode == ZSTD_e_continue) {
        ret = compress_mt_continue_impl(self, &data);
    } else {
        ret = compress_impl(self, &data, mode, 0);
    }

    if (ret) {
        self->last_mode = mode;
    } else {
        self->last_mode = ZSTD_e_end;

        /* Resetting cctx's session never fail */
        ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);
    }
    RELEASE_LOCK(self);

    PyBuffer_Release(&data);
    return ret;
}

/*[clinic input]
_zstd.ZSTDCompressor.flush

    mode: int
        Can be these 2 values .FLUSH_FRAME, .FLUSH_BLOCK
    /

Flush any remaining data in internal buffer.
Since zstd data consists of one or more independent frames, the compressor
object can still be used after this method is called.
[clinic start generated code]*/

static PyObject *
_zstd_ZSTDCompressor_flush_impl(ZSTDCompressor *self, int mode)
/*[clinic end generated code: output=fec21f3e22504f50 input=6b369303f67ad0a8]*/
{
    PyObject *ret;

    /* Check mode value */
    if (mode != ZSTD_e_end && mode != ZSTD_e_flush) {
        PyErr_SetString(PyExc_ValueError,
                        "mode argument wrong value, it should be "
                        "ZSTDCompressor.FLUSH_FRAME or "
                        "ZSTDCompressor.FLUSH_BLOCK.");
        return NULL;
    }

    /* Thread-safe code */
    ACQUIRE_LOCK(self);
    ret = compress_impl(self, NULL, mode, 0);

    if (ret) {
        self->last_mode = mode;
    } else {
        self->last_mode = ZSTD_e_end;

        /* Resetting cctx's session never fail */
        ZSTD_CCtx_reset(self->cctx, ZSTD_reset_session_only);
    }
    RELEASE_LOCK(self);

    return ret;
}

/*[-clinic input]
@classmethod
_lzma.LZMACompressor.__new__



[-clinic start generated code]*/
static PyObject *
Compressor_new(PyTypeObject *type)
{
    ZSTDCompressor *self;
    self = (ZSTDCompressor*)type->tp_alloc(type, 0);
    if (self == NULL) {
        goto error;
    }

    /* Keep this first. Set module state to self. */
    SET_STATE_TO_OBJ(type, self);

    assert(self->dict == NULL);
    assert(self->use_multithread == 0);
    assert(self->compression_level == 0);

    /* Compression context */
    self->cctx = ZSTD_createCCtx();
    if (self->cctx == NULL) {
        _zstd_state *state = PyType_GetModuleState(type);
        PyErr_SetString(state->error,
                        "Unable to create ZSTD_CCtx instance.");
        goto error;
    }

    /* Last mode */
    self->last_mode = ZSTD_e_end;

    /* Thread lock */
    self->lock = PyThread_allocate_lock();
    if (self->lock == NULL) {
        PyErr_NoMemory();
        goto error;
    }
    return (PyObject*)self;

error:
    Py_XDECREF(self);
    return NULL;
}

/*[-clinic input]
_zstd.ZSTDCompressor.__init__
    [
        level: int
            Compression level to use.
    ]
    [
        options: object(type='PyUnicodeObject *', subclass_of='&PyUnicode_Type')
            Advanced compression parameters.
    ]
    [
        zstd_dict: object(type='ZSTDDict *', subclass_of='&zstd_dict_type')
            A ZSTDDict object, pre-trained zstd dictionary.
    ]

Create a streaming compressor object for compressing data incrementally.

For one-shot compression, use the compress() function instead.
[-clinic start generated code]*/
static int
Compressor_init(ZSTDCompressor *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"level", "options", "zstd_dict", NULL};
    int level;
    PyObject *options = Py_None;
    PyObject *zstd_dict = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
                                     "|iOO:ZstdCompressor.__init__", kwlist,
                                     &level, &options, &zstd_dict)) {
        return -1;
    }

    // TODO: handle level/options

    /* Set compressLevel/option to compression context */
    if (level_or_option != Py_None) {
        if (set_c_parameters(self, level_or_option) < 0) {
            return -1;
        }
    }

    /* Load dictionary to compression context */
    if (zstd_dict != Py_None) {
        if (load_c_dict(self, zstd_dict) < 0) {
            return -1;
        }

        /* Py_INCREF the dict */
        Py_INCREF(zstd_dict);
        self->dict = zstd_dict;
    }

    return 0;
}

static void
Compressor_dealloc(ZSTDCompressor *self)
{
    /* Free compression context */
    ZSTD_freeCCtx(self->cctx);

    /* Py_XDECREF the dict after free the compression context */
    Py_XDECREF(self->dict);

    /* Thread lock */
    if (self->lock) {
        PyThread_free_lock(self->lock);
    }

    PyTypeObject *tp = Py_TYPE(self);
    tp->tp_free((PyObject*)self);
    Py_DECREF(tp);
}

static PyMethodDef Compressor_methods[] = {
    _ZSTD_ZSTDCOMPRESSOR_COMPRESS_METHODDEF
    _ZSTD_ZSTDCOMPRESSOR_FLUSH_METHODDEF
    {NULL}
};

static int
Compressor_traverse(ZSTDCompressor *self, visitproc visit, void *arg)
{
    Py_VISIT(Py_TYPE(self));
    return 0;
}

PyDoc_STRVAR(Compressor_doc,
"A streaming compressor. Thread-safe at method level.\n\n"
"ZstdCompressor.__init__(self, level=None, option=None, zstd_dict=None)\n"
"----\n"
"Initialize a ZstdCompressor object.\n\n"
"Parameters\n"
"level:           the compression level.\n"
"options:          advanced compression parameters.\n"
"zstd_dict:       A ZstdDict object, pre-trained zstd dictionary.");

static PyType_Slot zstd_compressor_type_slots[] = {
    {Py_tp_dealloc, Compressor_dealloc},
    {Py_tp_methods, Compressor_methods},
    {Py_tp_init, Compressor_init},
    {Py_tp_new, PyType_GenericNew},
    {Py_tp_doc, (char *)Compressor_doc},
    {Py_tp_traverse, Compressor_traverse},
    {0, 0}
};

static PyType_Spec zstd_compressor_type_spec = {
    .name = "_zstd.ZSTDCompressor",
    .basicsize = sizeof(ZSTDCompressor),
    // Calling PyType_GetModuleState() on a subclass is not safe.
    // zstd_compressor_type_spec does not have Py_TPFLAGS_BASETYPE flag
    // which prevents to create a subclass.
    // So calling PyType_GetModuleState() in this file is always safe.
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = zstd_compressor_type_slots,
};

#pragma endregion

/* ZSTDDecompressor class. */
#pragma region "ZSTDDecompressor"
PyObject *
decompress_impl(ZSTDDecompressor *self, ZSTD_inBuffer *in,
                const Py_ssize_t max_length,
                const Py_ssize_t initial_size,
                const int single_frame)
{
    size_t zstd_ret;
    ZSTD_outBuffer out;
    _BlocksOutputBuffer buffer = {.list = NULL};
    PyObject *ret;

    /* The first AFE check for setting .at_frame_edge flag */
    if (!single_frame) {
        if (self->at_frame_edge && in->pos == in->size) {
            STATE_FROM_OBJ(self);
            ret = self->empty_bytes;
            Py_INCREF(ret);
            return ret;
        }
    }

    /* Initialize the output buffer */
    if (initial_size >= 0) {
        if (OutputBuffer_InitWithSize(&buffer, &out, max_length, initial_size) < 0) {
            goto error;
        }
    } else {
        if (OutputBuffer_InitAndGrow(&buffer, &out, max_length) < 0) {
            goto error;
        }
    }
    assert(out.pos == 0);

    while (1) {
        /* Decompress */
        Py_BEGIN_ALLOW_THREADS
        zstd_ret = ZSTD_decompressStream(self->dctx, &out, in);
        Py_END_ALLOW_THREADS

        /* Check error */
        if (ZSTD_isError(zstd_ret)) {
            STATE_FROM_OBJ(self);
            set_zstd_error(MODULE_STATE, ERR_DECOMPRESS, zstd_ret);
            goto error;
        }

        /* Set .eof/.af_frame_edge flag */
        if (single_frame) {
            /* ZstdDecompressor class stops when a frame is decompressed */
            if (zstd_ret == 0) {
                self->eof = 1;
                break;
            }
        } else if (!single_frame) {
            /* EndlessZstdDecompressor class supports multiple frames */
            self->at_frame_edge = (zstd_ret == 0) ? 1 : 0;

            /* The second AFE check for setting .at_frame_edge flag */
            if (self->at_frame_edge && in->pos == in->size) {
                break;
            }
        }

        /* Need to check out before in. Maybe zstd's internal buffer still has
           a few bytes can be output, grow the buffer and continue. */
        if (out.pos == out.size) {
            /* Output buffer exhausted */

            /* Output buffer reached max_length */
            if (OutputBuffer_ReachedMaxLength(&buffer, &out)) {
                break;
            }

            /* Grow output buffer */
            if (OutputBuffer_Grow(&buffer, &out) < 0) {
                goto error;
            }
            assert(out.pos == 0);

        } else if (in->pos == in->size) {
            /* Finished */
            break;
        }
    }

    /* Return a bytes object */
    ret = OutputBuffer_Finish(&buffer, &out);
    if (ret != NULL) {
        return ret;
    }

error:
    OutputBuffer_OnError(&buffer);
    return NULL;
}

/*[clinic input]
_zstd.ZSTDDecompressor.decompress

    data: Py_buffer
    max_length: Py_ssize_t=-1

Decompress *data*, returning uncompressed data as bytes.

If *max_length* is nonnegative, returns at most *max_length* bytes of
decompressed data. If this limit is reached and further output can be
produced, *self.needs_input* will be set to ``False``. In this case, the next
call to *decompress()* may provide *data* as b'' to obtain more of the output.

If all of the input data was decompressed and returned (either because this
was less than *max_length* bytes, or because *max_length* was negative),
*self.needs_input* will be set to True.

Attempting to decompress data after the end of stream is reached raises an
EOFError.  Any data found after the end of the stream is ignored and saved in
the unused_data attribute.
[clinic start generated code]*/

static PyObject *
_zstd_ZSTDDecompressor_decompress_impl(ZSTDDecompressor *self, Py_buffer *data,
                                       Py_ssize_t max_length)
/*[clinic end generated code: output=ef4e20ec7122241d input=60c1f135820e309d]*/
{
    PyObject *result = NULL;

    ACQUIRE_LOCK(self);
    if (self->eof)
        PyErr_SetString(PyExc_EOFError, "Already at end of stream");
    else
        result = decompress(self, data->buf, data->len, max_length);
    RELEASE_LOCK(self);
    return result;
}

static int
Decompressor_init_raw(_zstd_state *state, zstd_stream *lzs, PyObject *filterspecs)
{
    zstd_filter filters[ZSTD_FILTERS_MAX + 1];
    zstd_ret lzret;

    if (parse_filter_chain_spec(state, filters, filterspecs) == -1) {
        return -1;
    }
    lzret = zstd_raw_decoder(lzs, filters);
    free_filter_chain(filters);
    if (catch_zstd_error(state, lzret)) {
        return -1;
    }
    else {
        return 0;
    }
}

/*[clinic input]
_zstd.ZSTDDecompressor.__init__

    format: int(c_default="FORMAT_AUTO") = FORMAT_AUTO
        Specifies the container format of the input stream.  If this is
        FORMAT_AUTO (the default), the decompressor will automatically detect
        whether the input is FORMAT_XZ or FORMAT_ALONE.  Streams created with
        FORMAT_RAW cannot be autodetected.

    memlimit: object = None
        Limit the amount of memory used by the decompressor.  This will cause
        decompression to fail if the input cannot be decompressed within the
        given limit.

    filters: object = None
        A custom filter chain.  This argument is required for FORMAT_RAW, and
        not accepted with any other format.  When provided, this should be a
        sequence of dicts, each indicating the ID and options for a single
        filter.

Create a decompressor object for decompressing data incrementally.

For one-shot decompression, use the decompress() function instead.
[clinic start generated code]*/

static int
_zstd_ZSTDDecompressor___init___impl(ZSTDDecompressor *self, int format,
                                     PyObject *memlimit, PyObject *filters)
/*[clinic end generated code: output=3e1821f8aa36564c input=81fe684a6c2f8a27]*/
{
    const uint32_t decoder_flags = ZSTD_TELL_ANY_CHECK | ZSTD_TELL_NO_CHECK;
    uint64_t memlimit_ = UINT64_MAX;
    zstd_ret lzret;
    _zstd_state *state = PyType_GetModuleState(Py_TYPE(self));
    assert(state != NULL);

    if (memlimit != Py_None) {
        if (format == FORMAT_RAW) {
            PyErr_SetString(PyExc_ValueError,
                            "Cannot specify memory limit with FORMAT_RAW");
            return -1;
        }
        memlimit_ = PyLong_AsUnsignedLongLong(memlimit);
        if (PyErr_Occurred()) {
            return -1;
        }
    }

    if (format == FORMAT_RAW && filters == Py_None) {
        PyErr_SetString(PyExc_ValueError,
                        "Must specify filters for FORMAT_RAW");
        return -1;
    } else if (format != FORMAT_RAW && filters != Py_None) {
        PyErr_SetString(PyExc_ValueError,
                        "Cannot specify filters except with FORMAT_RAW");
        return -1;
    }

    self->alloc.opaque = NULL;
    self->alloc.alloc = PyLzma_Malloc;
    self->alloc.free = PyLzma_Free;
    self->lzs.allocator = &self->alloc;
    self->lzs.next_in = NULL;

    PyThread_type_lock lock = PyThread_allocate_lock();
    if (lock == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Unable to allocate lock");
        return -1;
    }
    if (self->lock != NULL) {
        PyThread_free_lock(self->lock);
    }
    self->lock = lock;

    self->check = ZSTD_CHECK_UNKNOWN;
    self->needs_input = 1;
    self->input_buffer = NULL;
    self->input_buffer_size = 0;
    Py_XSETREF(self->unused_data, PyBytes_FromStringAndSize(NULL, 0));
    if (self->unused_data == NULL) {
        goto error;
    }

    switch (format) {
        case FORMAT_AUTO:
            lzret = zstd_auto_decoder(&self->lzs, memlimit_, decoder_flags);
            if (catch_zstd_error(state, lzret)) {
                break;
            }
            return 0;

        case FORMAT_XZ:
            lzret = zstd_stream_decoder(&self->lzs, memlimit_, decoder_flags);
            if (catch_zstd_error(state, lzret)) {
                break;
            }
            return 0;

        case FORMAT_ALONE:
            self->check = ZSTD_CHECK_NONE;
            lzret = zstd_alone_decoder(&self->lzs, memlimit_);
            if (catch_zstd_error(state, lzret)) {
                break;
            }
            return 0;

        case FORMAT_RAW:
            self->check = ZSTD_CHECK_NONE;
            if (Decompressor_init_raw(state, &self->lzs, filters) == -1) {
                break;
            }
            return 0;

        default:
            PyErr_Format(PyExc_ValueError,
                         "Invalid container format: %d", format);
            break;
    }

error:
    Py_CLEAR(self->unused_data);
    PyThread_free_lock(self->lock);
    self->lock = NULL;
    return -1;
}

static void
Decompressor_dealloc(ZSTDDecompressor *self)
{
    if(self->input_buffer != NULL)
        PyMem_Free(self->input_buffer);

    zstd_end(&self->lzs);
    Py_CLEAR(self->unused_data);
    if (self->lock != NULL) {
        PyThread_free_lock(self->lock);
    }
    PyTypeObject *tp = Py_TYPE(self);
    tp->tp_free((PyObject *)self);
    Py_DECREF(tp);
}

static int
Decompressor_traverse(ZSTDDecompressor *self, visitproc visit, void *arg)
{
    Py_VISIT(Py_TYPE(self));
    return 0;
}

static PyMethodDef Decompressor_methods[] = {
    _ZSTD_ZSTDDECOMPRESSOR_DECOMPRESS_METHODDEF
    {NULL}
};

PyDoc_STRVAR(Decompressor_check_doc,
"ID of the integrity check used by the input stream.");

PyDoc_STRVAR(Decompressor_eof_doc,
"True if the end-of-stream marker has been reached.");

PyDoc_STRVAR(Decompressor_needs_input_doc,
"True if more input is needed before more decompressed data can be produced.");

PyDoc_STRVAR(Decompressor_unused_data_doc,
"Data found after the end of the compressed stream.");

static PyMemberDef Decompressor_members[] = {
    {"check", T_INT, offsetof(ZSTDDecompressor, check), READONLY,
     Decompressor_check_doc},
    {"eof", T_BOOL, offsetof(ZSTDDecompressor, eof), READONLY,
     Decompressor_eof_doc},
    {"needs_input", T_BOOL, offsetof(ZSTDDecompressor, needs_input), READONLY,
     Decompressor_needs_input_doc},
    {"unused_data", T_OBJECT_EX, offsetof(ZSTDDecompressor, unused_data), READONLY,
     Decompressor_unused_data_doc},
    {NULL}
};

static PyType_Slot zstd_decompressor_type_slots[] = {
    {Py_tp_dealloc, Decompressor_dealloc},
    {Py_tp_methods, Decompressor_methods},
    {Py_tp_init, _zstd_ZSTDDecompressor___init__},
    {Py_tp_new, PyType_GenericNew},
    {Py_tp_doc, (char *)_zstd_ZSTDDecompressor___init____doc__},
    {Py_tp_traverse, Decompressor_traverse},
    {Py_tp_members, Decompressor_members},
    {0, 0}
};

static PyType_Spec zstd_decompressor_type_spec = {
    .name = "_zstd.ZSTDDecompressor",
    .basicsize = sizeof(ZSTDDecompressor),
    // Calling PyType_GetModuleState() on a subclass is not safe.
    // zstd_decompressor_type_spec does not have Py_TPFLAGS_BASETYPE flag
    // which prevents to create a subclass.
    // So calling PyType_GetModuleState() in this file is always safe.
    .flags = Py_TPFLAGS_DEFAULT,
    .slots = zstd_decompressor_type_slots,
};
#pragma endregion

#pragma region "module"

/* Module-level functions. */

PyDoc_STRVAR(_get_cparam_bounds_doc,
"Internal function, get CParameter/DParameter bounds.");

/*[clinic input]
_zstd._get_cparam_bounds
    parameter: int
    /

Get CParameter bounds.
[clinic start generated code]*/
static PyObject *
_get_cparam_bounds(PyObject *module,  parameter)
{
    int parameter;

    ZSTD_bounds bound;

    if (!PyArg_ParseTuple(args, "i:_get_cparam_bounds", &parameter)) {
        return NULL;
    }

    bound = ZSTD_cParam_getBounds(parameter);
    if (ZSTD_isError(bound.error)) {
        STATE_FROM_MODULE(module);
        set_zstd_error(MODULE_STATE, ERR_GET_C_BOUNDS, bound.error);
        return NULL;
    }

    return Py_BuildValue("ii", bound.lowerBound, bound.upperBound);
}

/*[clinic input]
_zstd._get_cparam_bounds
    parameter: int
    /

Get DParameter bounds.
[clinic start generated code]*/
static PyObject *
_get_dparam_bounds(PyObject *module, PyObject *args)
{
    int parameter;

    ZSTD_bounds bound;

    if (!PyArg_ParseTuple(args, "i:_get_cparam_bounds", &parameter)) {
        return NULL;
    }

    bound = ZSTD_dParam_getBounds(parameter);
    if (ZSTD_isError(bound.error)) {
        STATE_FROM_MODULE(module);
        set_zstd_error(MODULE_STATE, ERR_GET_D_BOUNDS, bound.error);
        return NULL;
    }


    return Py_BuildValue("ii", bound.lowerBound, bound.upperBound);
}



static int
zstd_exec(PyObject *module)
{
    _zstd_state *state = get_zstd_state(module);
    state->error = PyErr_NewExceptionWithDoc("_zstd.ZSTDError", "Call to the underlying zstd library failed.", NULL, NULL);
    if (state->error == NULL) {
        return -1;
    }

    if (PyModule_AddType(module, (PyTypeObject *)state->error) < 0) {
        return -1;
    }

    state->zstd_compressor_type = (PyTypeObject *)PyType_FromModuleAndSpec(module,
                                                            &zstd_compressor_type_spec, NULL);
    if (state->zstd_compressor_type == NULL) {
        return -1;
    }

    if (PyModule_AddType(module, state->zstd_compressor_type) < 0) {
        return -1;
    }

    state->zstd_decompressor_type = (PyTypeObject *)PyType_FromModuleAndSpec(module,
                                                         &zstd_decompressor_type_spec, NULL);
    if (state->zstd_decompressor_type == NULL) {
        return -1;
    }

    if (PyModule_AddType(module, state->zstd_decompressor_type) < 0) {
        return -1;
    }
    return 0;
}

static PyMethodDef zstd_methods[] = {
    _ZSTD_GET_CPARAM_BOUNDS_METHODDEF,
    _ZSTD_GET_DPARAM_BOUNDS_METHODDEF,
    _ZSTD_GET_FRAME_SIZE_METHODDEF,
    _ZSTD_TRAIN_DICT_METHODDEF,
    _ZSTD_FINALIZE_DICT_METHODDEF,
    {NULL}
};

static PyModuleDef_Slot zstd_slots[] = {
    {Py_mod_exec, zstd_exec},
    {0, NULL}
};

static int
zstd_traverse(PyObject *module, visitproc visit, void *arg)
{
    _zstd_state *state = get_zstd_state(module);
    Py_VISIT(state->zstd_compressor_type);
    Py_VISIT(state->zstd_decompressor_type);
    Py_VISIT(state->zstd_dict_type);
    Py_VISIT(state->error);
    return 0;
}

static int
zstd_clear(PyObject *module)
{
    _zstd_state *state = get_zstd_state(module);
    Py_CLEAR(state->zstd_compressor_type);
    Py_CLEAR(state->zstd_decompressor_type);
    Py_CLEAR(state->zstd_dict_type);
    Py_CLEAR(state->error);
    return 0;
}

static void
zstd_free(void *module)
{
    zstd_clear((PyObject *)module);
}

static PyModuleDef _zstdmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_zstd",
    .m_size = sizeof(_zstd_state),
    .m_methods = zstd_methods,
    .m_slots = zstd_slots,
    .m_traverse = zstd_traverse,
    .m_clear = zstd_clear,
    .m_free = zstd_free,
};

PyMODINIT_FUNC
PyInit__zstd(void)
{
    return PyModuleDef_Init(&_zstdmodule);
}
#pragma endregion