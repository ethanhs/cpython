/*[clinic input]
preserve
[clinic start generated code]*/

#include "pycore_modsupport.h"    // _PyArg_CheckPositional()

PyDoc_STRVAR(_zstd_ZSTDCompressor_compress__doc__,
"compress($self, data, mode, /)\n"
"--\n"
"\n"
"Provide data to the compressor object.\n"
"\n"
"  mode\n"
"    Can be these 3 values ZSTDCompressor.CONTINUE,\n"
"    ZSTDCompressor.FLUSH_BLOCK, ZSTDCompressor.FLUSH_FRAME\n"
"\n"
"Return a chunk of compressed data if possible, or b\'\' otherwise.");

#define _ZSTD_ZSTDCOMPRESSOR_COMPRESS_METHODDEF    \
    {"compress", _PyCFunction_CAST(_zstd_ZSTDCompressor_compress), METH_FASTCALL, _zstd_ZSTDCompressor_compress__doc__},

static PyObject *
_zstd_ZSTDCompressor_compress_impl(ZSTDCompressor *self, Py_buffer *data,
                                   int mode);

static PyObject *
_zstd_ZSTDCompressor_compress(ZSTDCompressor *self, PyObject *const *args, Py_ssize_t nargs)
{
    PyObject *return_value = NULL;
    Py_buffer data = {NULL, NULL};
    int mode;

    if (!_PyArg_CheckPositional("compress", nargs, 2, 2)) {
        goto exit;
    }
    if (PyObject_GetBuffer(args[0], &data, PyBUF_SIMPLE) != 0) {
        goto exit;
    }
    mode = PyLong_AsInt(args[1]);
    if (mode == -1 && PyErr_Occurred()) {
        goto exit;
    }
    return_value = _zstd_ZSTDCompressor_compress_impl(self, &data, mode);

exit:
    /* Cleanup for data */
    if (data.obj) {
       PyBuffer_Release(&data);
    }

    return return_value;
}

PyDoc_STRVAR(_zstd_ZSTDCompressor_flush__doc__,
"flush($self, mode, /)\n"
"--\n"
"\n"
"Flush any remaining data in internal buffer.\n"
"\n"
"  mode\n"
"    Can be these 2 values ZSTDCompressor.FLUSH_FRAME,\n"
"    ZSTDCompressor.FLUSH_BLOCK\n"
"\n"
"Since zstd data consists of one or more independent frames, the compressor\n"
"object can still be used after this method is called.");

#define _ZSTD_ZSTDCOMPRESSOR_FLUSH_METHODDEF    \
    {"flush", (PyCFunction)_zstd_ZSTDCompressor_flush, METH_O, _zstd_ZSTDCompressor_flush__doc__},

static PyObject *
_zstd_ZSTDCompressor_flush_impl(ZSTDCompressor *self, int mode);

static PyObject *
_zstd_ZSTDCompressor_flush(ZSTDCompressor *self, PyObject *arg)
{
    PyObject *return_value = NULL;
    int mode;

    mode = PyLong_AsInt(arg);
    if (mode == -1 && PyErr_Occurred()) {
        goto exit;
    }
    return_value = _zstd_ZSTDCompressor_flush_impl(self, mode);

exit:
    return return_value;
}

PyDoc_STRVAR(_zstd__get_cparam_bounds__doc__,
"_get_cparam_bounds($module, parameter, /)\n"
"--\n"
"\n"
"Get CParameter bounds.");

#define _ZSTD__GET_CPARAM_BOUNDS_METHODDEF    \
    {"_get_cparam_bounds", (PyCFunction)_zstd__get_cparam_bounds, METH_O, _zstd__get_cparam_bounds__doc__},

static PyObject *
_zstd__get_cparam_bounds_impl(PyObject *module, int parameter);

static PyObject *
_zstd__get_cparam_bounds(PyObject *module, PyObject *arg)
{
    PyObject *return_value = NULL;
    int parameter;

    parameter = PyLong_AsInt(arg);
    if (parameter == -1 && PyErr_Occurred()) {
        goto exit;
    }
    return_value = _zstd__get_cparam_bounds_impl(module, parameter);

exit:
    return return_value;
}

PyDoc_STRVAR(_zstd__get_dparam_bounds__doc__,
"_get_dparam_bounds($module, parameter, /)\n"
"--\n"
"\n"
"Get DParameter bounds.");

#define _ZSTD__GET_DPARAM_BOUNDS_METHODDEF    \
    {"_get_dparam_bounds", (PyCFunction)_zstd__get_dparam_bounds, METH_O, _zstd__get_dparam_bounds__doc__},

static PyObject *
_zstd__get_dparam_bounds_impl(PyObject *module, int parameter);

static PyObject *
_zstd__get_dparam_bounds(PyObject *module, PyObject *arg)
{
    PyObject *return_value = NULL;
    int parameter;

    parameter = PyLong_AsInt(arg);
    if (parameter == -1 && PyErr_Occurred()) {
        goto exit;
    }
    return_value = _zstd__get_dparam_bounds_impl(module, parameter);

exit:
    return return_value;
}
/*[clinic end generated code: output=a9d935c9ce520c61 input=a9049054013a1b77]*/
