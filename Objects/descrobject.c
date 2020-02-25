/* Descriptors -- a new, flexible way to describe attributes */

#include "Python.h"
#include "pycore_object.h"
#include "pycore_pystate.h"
#include "pycore_tupleobject.h"
#include "structmember.h" /* Why is this not included in Python.h? */

_Py_IDENTIFIER(getattr);

/*[clinic input]
class mappingproxy "mappingproxyobject *" "&PyDictProxy_Type"
class property "propertyobject *" "&PyProperty_Type"
[clinic start generated code]*/
/*[clinic end generated code: output=da39a3ee5e6b4b0d input=556352653fd4c02e]*/

static void
descr_dealloc(PyDescrObject *descr)
{
    _PyObject_GC_UNTRACK(descr);
    Py_XDECREF(descr->d_type);
    Py_XDECREF(descr->d_name);
    Py_XDECREF(descr->d_qualname);
    PyObject_GC_Del(descr);
}

static PyObject *
descr_name(PyDescrObject *descr)
{
    if (descr->d_name != NULL && PyUnicode_Check(descr->d_name))
        return descr->d_name;
    return NULL;
}

static PyObject *
descr_repr(PyDescrObject *descr, const char *format)
{
    PyObject *name = NULL;
    if (descr->d_name != NULL && PyUnicode_Check(descr->d_name))
        name = descr->d_name;

    return PyUnicode_FromFormat(format, name, "?", descr->d_type->tp_name);
}

static PyObject *
method_repr(PyMethodDescrObject *descr)
{
    return descr_repr((PyDescrObject *)descr,
                      "<method '%V' of '%s' objects>");
}

static PyObject *
member_repr(PyMemberDescrObject *descr)
{
    return descr_repr((PyDescrObject *)descr,
                      "<member '%V' of '%s' objects>");
}

static PyObject *
getset_repr(PyGetSetDescrObject *descr)
{
    return descr_repr((PyDescrObject *)descr,
                      "<attribute '%V' of '%s' objects>");
}

static PyObject *
wrapperdescr_repr(PyWrapperDescrObject *descr)
{
    return descr_repr((PyDescrObject *)descr,
                      "<slot wrapper '%V' of '%s' objects>");
}

static int
descr_check(PyDescrObject *descr, PyObject *obj, PyObject **pres)
{
    if (obj == NULL) {
        Py_INCREF(descr);
        *pres = (PyObject *)descr;
        return 1;
    }
    if (!PyObject_TypeCheck(obj, descr->d_type)) {
        PyErr_Format(PyExc_TypeError,
                     "descriptor '%V' for '%.100s' objects "
                     "doesn't apply to a '%.100s' object",
                     descr_name((PyDescrObject *)descr), "?",
                     descr->d_type->tp_name,
                     Py_TYPE(obj)->tp_name);
        *pres = NULL;
        return 1;
    }
    return 0;
}

static PyObject *
classmethod_get(PyMethodDescrObject *descr, PyObject *obj, PyObject *type)
{
    /* Ensure a valid type.  Class methods ignore obj. */
    if (type == NULL) {
        if (obj != NULL)
            type = (PyObject *)Py_TYPE(obj);
        else {
            /* Wot - no type?! */
            PyErr_Format(PyExc_TypeError,
                         "descriptor '%V' for type '%.100s' "
                         "needs either an object or a type",
                         descr_name((PyDescrObject *)descr), "?",
                         PyDescr_TYPE(descr)->tp_name);
            return NULL;
        }
    }
    if (!PyType_Check(type)) {
        PyErr_Format(PyExc_TypeError,
                     "descriptor '%V' for type '%.100s' "
                     "needs a type, not a '%.100s' as arg 2",
                     descr_name((PyDescrObject *)descr), "?",
                     PyDescr_TYPE(descr)->tp_name,
                     Py_TYPE(type)->tp_name);
        return NULL;
    }
    if (!PyType_IsSubtype((PyTypeObject *)type, PyDescr_TYPE(descr))) {
        PyErr_Format(PyExc_TypeError,
                     "descriptor '%V' requires a subtype of '%.100s' "
                     "but received '%.100s'",
                     descr_name((PyDescrObject *)descr), "?",
                     PyDescr_TYPE(descr)->tp_name,
                     ((PyTypeObject *)type)->tp_name);
        return NULL;
    }
    return PyCFunction_NewEx(descr->d_method, type, NULL);
}

static PyObject *
method_get(PyMethodDescrObject *descr, PyObject *obj, PyObject *type)
{
    PyObject *res;

    if (descr_check((PyDescrObject *)descr, obj, &res))
        return res;
    return PyCFunction_NewEx(descr->d_method, obj, NULL);
}

static PyObject *
member_get(PyMemberDescrObject *descr, PyObject *obj, PyObject *type)
{
    PyObject *res;

    if (descr_check((PyDescrObject *)descr, obj, &res))
        return res;

    if (descr->d_member->flags & READ_RESTRICTED) {
        if (PySys_Audit("object.__getattr__", "Os",
            obj ? obj : Py_None, descr->d_member->name) < 0) {
            return NULL;
        }
    }

    return PyMember_GetOne((char *)obj, descr->d_member);
}

static PyObject *
getset_get(PyGetSetDescrObject *descr, PyObject *obj, PyObject *type)
{
    PyObject *res;

    if (descr_check((PyDescrObject *)descr, obj, &res))
        return res;
    if (descr->d_getset->get != NULL)
        return descr->d_getset->get(obj, descr->d_getset->closure);
    PyErr_Format(PyExc_AttributeError,
                 "attribute '%V' of '%.100s' objects is not readable",
                 descr_name((PyDescrObject *)descr), "?",
                 PyDescr_TYPE(descr)->tp_name);
    return NULL;
}

static PyObject *
wrapperdescr_get(PyWrapperDescrObject *descr, PyObject *obj, PyObject *type)
{
    PyObject *res;

    if (descr_check((PyDescrObject *)descr, obj, &res))
        return res;
    return PyWrapper_New((PyObject *)descr, obj);
}

static int
descr_setcheck(PyDescrObject *descr, PyObject *obj, PyObject *value,
               int *pres)
{
    assert(obj != NULL);
    if (!PyObject_TypeCheck(obj, descr->d_type)) {
        PyErr_Format(PyExc_TypeError,
                     "descriptor '%V' for '%.100s' objects "
                     "doesn't apply to a '%.100s' object",
                     descr_name(descr), "?",
                     descr->d_type->tp_name,
                     Py_TYPE(obj)->tp_name);
        *pres = -1;
        return 1;
    }
    return 0;
}

static int
member_set(PyMemberDescrObject *descr, PyObject *obj, PyObject *value)
{
    int res;

    if (descr_setcheck((PyDescrObject *)descr, obj, value, &res))
        return res;
    return PyMember_SetOne((char *)obj, descr->d_member, value);
}

static int
getset_set(PyGetSetDescrObject *descr, PyObject *obj, PyObject *value)
{
    int res;

    if (descr_setcheck((PyDescrObject *)descr, obj, value, &res))
        return res;
    if (descr->d_getset->set != NULL)
        return descr->d_getset->set(obj, value,
                                    descr->d_getset->closure);
    PyErr_Format(PyExc_AttributeError,
                 "attribute '%V' of '%.100s' objects is not writable",
                 descr_name((PyDescrObject *)descr), "?",
                 PyDescr_TYPE(descr)->tp_name);
    return -1;
}


/* Vectorcall functions for each of the PyMethodDescr calling conventions.
 *
 * First, common helpers
 */
static inline int
method_check_args(PyObject *func, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames)
{
    assert(!PyErr_Occurred());
    if (nargs < 1) {
        PyObject *funcstr = _PyObject_FunctionStr(func);
        if (funcstr != NULL) {
            PyErr_Format(PyExc_TypeError,
                         "unbound method %U needs an argument", funcstr);
            Py_DECREF(funcstr);
        }
        return -1;
    }
    PyObject *self = args[0];
    PyObject *dummy;
    if (descr_check((PyDescrObject *)func, self, &dummy)) {
        return -1;
    }
    if (kwnames && PyTuple_GET_SIZE(kwnames)) {
        PyObject *funcstr = _PyObject_FunctionStr(func);
        if (funcstr != NULL) {
            PyErr_Format(PyExc_TypeError,
                         "%U takes no keyword arguments", funcstr);
            Py_DECREF(funcstr);
        }
        return -1;
    }
    return 0;
}

typedef void (*funcptr)(void);

static inline funcptr
method_enter_call(PyThreadState *tstate, PyObject *func)
{
    if (_Py_EnterRecursiveCall(tstate, " while calling a Python object")) {
        return NULL;
    }
    return (funcptr)((PyMethodDescrObject *)func)->d_method->ml_meth;
}

/* Now the actual vectorcall functions */
static PyObject *
method_vectorcall_VARARGS(
    PyObject *func, PyObject *const *args, size_t nargsf, PyObject *kwnames)
{
    PyThreadState *tstate = _PyThreadState_GET();
    Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);
    if (method_check_args(func, args, nargs, kwnames)) {
        return NULL;
    }
    PyObject *argstuple = _PyTuple_FromArray(args+1, nargs-1);
    if (argstuple == NULL) {
        return NULL;
    }
    PyCFunction meth = (PyCFunction)method_enter_call(tstate, func);
    if (meth == NULL) {
        Py_DECREF(argstuple);
        return NULL;
    }
    PyObject *result = meth(args[0], argstuple);
    Py_DECREF(argstuple);
    _Py_LeaveRecursiveCall(tstate);
    return result;
}

static PyObject *
method_vectorcall_VARARGS_KEYWORDS(
    PyObject *func, PyObject *const *args, size_t nargsf, PyObject *kwnames)
{
    PyThreadState *tstate = _PyThreadState_GET();
    Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);
    if (method_check_args(func, args, nargs, NULL)) {
        return NULL;
    }
    PyObject *argstuple = _PyTuple_FromArray(args+1, nargs-1);
    if (argstuple == NULL) {
        return NULL;
    }
    PyObject *result = NULL;
    /* Create a temporary dict for keyword arguments */
    PyObject *kwdict = NULL;
    if (kwnames != NULL && PyTuple_GET_SIZE(kwnames) > 0) {
        kwdict = _PyStack_AsDict(args + nargs, kwnames);
        if (kwdict == NULL) {
            goto exit;
        }
    }
    PyCFunctionWithKeywords meth = (PyCFunctionWithKeywords)
                                   method_enter_call(tstate, func);
    if (meth == NULL) {
        goto exit;
    }
    result = meth(args[0], argstuple, kwdict);
    _Py_LeaveRecursiveCall(tstate);
exit:
    Py_DECREF(argstuple);
    Py_XDECREF(kwdict);
    return result;
}

static PyObject *
method_vectorcall_FASTCALL(
    PyObject *func, PyObject *const *args, size_t nargsf, PyObject *kwnames)
{
    PyThreadState *tstate = _PyThreadState_GET();
    Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);
    if (method_check_args(func, args, nargs, kwnames)) {
        return NULL;
    }
    _PyCFunctionFast meth = (_PyCFunctionFast)
                            method_enter_call(tstate, func);
    if (meth == NULL) {
        return NULL;
    }
    PyObject *result = meth(args[0], args+1, nargs-1);
    _Py_LeaveRecursiveCall(tstate);
    return result;
}

static PyObject *
method_vectorcall_FASTCALL_KEYWORDS(
    PyObject *func, PyObject *const *args, size_t nargsf, PyObject *kwnames)
{
    PyThreadState *tstate = _PyThreadState_GET();
    Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);
    if (method_check_args(func, args, nargs, NULL)) {
        return NULL;
    }
    _PyCFunctionFastWithKeywords meth = (_PyCFunctionFastWithKeywords)
                                        method_enter_call(tstate, func);
    if (meth == NULL) {
        return NULL;
    }
    PyObject *result = meth(args[0], args+1, nargs-1, kwnames);
    _Py_LeaveRecursiveCall(tstate);
    return result;
}

static PyObject *
method_vectorcall_NOARGS(
    PyObject *func, PyObject *const *args, size_t nargsf, PyObject *kwnames)
{
    PyThreadState *tstate = _PyThreadState_GET();
    Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);
    if (method_check_args(func, args, nargs, kwnames)) {
        return NULL;
    }
    if (nargs != 1) {
        PyObject *funcstr = _PyObject_FunctionStr(func);
        if (funcstr != NULL) {
            PyErr_Format(PyExc_TypeError,
                "%U takes no arguments (%zd given)", funcstr, nargs-1);
            Py_DECREF(funcstr);
        }
        return NULL;
    }
    PyCFunction meth = (PyCFunction)method_enter_call(tstate, func);
    if (meth == NULL) {
        return NULL;
    }
    PyObject *result = meth(args[0], NULL);
    _Py_LeaveRecursiveCall(tstate);
    return result;
}

static PyObject *
method_vectorcall_O(
    PyObject *func, PyObject *const *args, size_t nargsf, PyObject *kwnames)
{
    PyThreadState *tstate = _PyThreadState_GET();
    Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);
    if (method_check_args(func, args, nargs, kwnames)) {
        return NULL;
    }
    if (nargs != 2) {
        PyObject *funcstr = _PyObject_FunctionStr(func);
        if (funcstr != NULL) {
            PyErr_Format(PyExc_TypeError,
                "%U takes exactly one argument (%zd given)",
                funcstr, nargs-1);
            Py_DECREF(funcstr);
        }
        return NULL;
    }
    PyCFunction meth = (PyCFunction)method_enter_call(tstate, func);
    if (meth == NULL) {
        return NULL;
    }
    PyObject *result = meth(args[0], args[1]);
    _Py_LeaveRecursiveCall(tstate);
    return result;
}


/* Instances of classmethod_descriptor are unlikely to be called directly.
   For one, the analogous class "classmethod" (for Python classes) is not
   callable. Second, users are not likely to access a classmethod_descriptor
   directly, since it means pulling it from the class __dict__.

   This is just an excuse to say that this doesn't need to be optimized:
   we implement this simply by calling __get__ and then calling the result.
*/
static PyObject *
classmethoddescr_call(PyMethodDescrObject *descr, PyObject *args,
                      PyObject *kwds)
{
    Py_ssize_t argc = PyTuple_GET_SIZE(args);
    if (argc < 1) {
        PyErr_Format(PyExc_TypeError,
                     "descriptor '%V' of '%.100s' "
                     "object needs an argument",
                     descr_name((PyDescrObject *)descr), "?",
                     PyDescr_TYPE(descr)->tp_name);
        return NULL;
    }
    PyObject *self = PyTuple_GET_ITEM(args, 0);
    PyObject *bound = classmethod_get(descr, NULL, self);
    if (bound == NULL) {
        return NULL;
    }
    PyObject *res = _PyObject_FastCallDict(bound, _PyTuple_ITEMS(args)+1,
                                           argc-1, kwds);
    Py_DECREF(bound);
    return res;
}

Py_LOCAL_INLINE(PyObject *)
wrapperdescr_raw_call(PyWrapperDescrObject *descr, PyObject *self,
                      PyObject *args, PyObject *kwds)
{
    wrapperfunc wrapper = descr->d_base->wrapper;

    if (descr->d_base->flags & PyWrapperFlag_KEYWORDS) {
        wrapperfunc_kwds wk = (wrapperfunc_kwds)(void(*)(void))wrapper;
        return (*wk)(self, args, descr->d_wrapped, kwds);
    }

    if (kwds != NULL && (!PyDict_Check(kwds) || PyDict_GET_SIZE(kwds) != 0)) {
        PyErr_Format(PyExc_TypeError,
                     "wrapper %s() takes no keyword arguments",
                     descr->d_base->name);
        return NULL;
    }
    return (*wrapper)(self, args, descr->d_wrapped);
}

static PyObject *
wrapperdescr_call(PyWrapperDescrObject *descr, PyObject *args, PyObject *kwds)
{
    Py_ssize_t argc;
    PyObject *self, *result;

    /* Make sure that the first argument is acceptable as 'self' */
    assert(PyTuple_Check(args));
    argc = PyTuple_GET_SIZE(args);
    if (argc < 1) {
        PyErr_Format(PyExc_TypeError,
                     "descriptor '%V' of '%.100s' "
                     "object needs an argument",
                     descr_name((PyDescrObject *)descr), "?",
                     PyDescr_TYPE(descr)->tp_name);
        return NULL;
    }
    self = PyTuple_GET_ITEM(args, 0);
    if (!_PyObject_RealIsSubclass((PyObject *)Py_TYPE(self),
                                  (PyObject *)PyDescr_TYPE(descr))) {
        PyErr_Format(PyExc_TypeError,
                     "descriptor '%V' "
                     "requires a '%.100s' object "
                     "but received a '%.100s'",
                     descr_name((PyDescrObject *)descr), "?",
                     PyDescr_TYPE(descr)->tp_name,
                     Py_TYPE(self)->tp_name);
        return NULL;
    }

    args = PyTuple_GetSlice(args, 1, argc);
    if (args == NULL) {
        return NULL;
    }
    result = wrapperdescr_raw_call(descr, self, args, kwds);
    Py_DECREF(args);
    return result;
}


static PyObject *
method_get_doc(PyMethodDescrObject *descr, void *closure)
{
    return _PyType_GetDocFromInternalDoc(descr->d_method->ml_name, descr->d_method->ml_doc);
}

static PyObject *
method_get_text_signature(PyMethodDescrObject *descr, void *closure)
{
    return _PyType_GetTextSignatureFromInternalDoc(descr->d_method->ml_name, descr->d_method->ml_doc);
}

static PyObject *
calculate_qualname(PyDescrObject *descr)
{
    PyObject *type_qualname, *res;
    _Py_IDENTIFIER(__qualname__);

    if (descr->d_name == NULL || !PyUnicode_Check(descr->d_name)) {
        PyErr_SetString(PyExc_TypeError,
                        "<descriptor>.__name__ is not a unicode object");
        return NULL;
    }

    type_qualname = _PyObject_GetAttrId((PyObject *)descr->d_type,
                                        &PyId___qualname__);
    if (type_qualname == NULL)
        return NULL;

    if (!PyUnicode_Check(type_qualname)) {
        PyErr_SetString(PyExc_TypeError, "<descriptor>.__objclass__."
                        "__qualname__ is not a unicode object");
        Py_XDECREF(type_qualname);
        return NULL;
    }

    res = PyUnicode_FromFormat("%S.%S", type_qualname, descr->d_name);
    Py_DECREF(type_qualname);
    return res;
}

static PyObject *
descr_get_qualname(PyDescrObject *descr, void *Py_UNUSED(ignored))
{
    if (descr->d_qualname == NULL)
        descr->d_qualname = calculate_qualname(descr);
    Py_XINCREF(descr->d_qualname);
    return descr->d_qualname;
}

static PyObject *
descr_reduce(PyDescrObject *descr, PyObject *Py_UNUSED(ignored))
{
    return Py_BuildValue("N(OO)", _PyEval_GetBuiltinId(&PyId_getattr),
                         PyDescr_TYPE(descr), PyDescr_NAME(descr));
}

static PyMethodDef descr_methods[] = {
    {"__reduce__", (PyCFunction)descr_reduce, METH_NOARGS, NULL},
    {NULL, NULL}
};

static PyMemberDef descr_members[] = {
    {"__objclass__", T_OBJECT, offsetof(PyDescrObject, d_type), READONLY},
    {"__name__", T_OBJECT, offsetof(PyDescrObject, d_name), READONLY},
    {0}
};

static PyGetSetDef method_getset[] = {
    {"__doc__", (getter)method_get_doc},
    {"__qualname__", (getter)descr_get_qualname},
    {"__text_signature__", (getter)method_get_text_signature},
    {0}
};

static PyObject *
member_get_doc(PyMemberDescrObject *descr, void *closure)
{
    if (descr->d_member->doc == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(descr->d_member->doc);
}

static PyGetSetDef member_getset[] = {
    {"__doc__", (getter)member_get_doc},
    {"__qualname__", (getter)descr_get_qualname},
    {0}
};

static PyObject *
getset_get_doc(PyGetSetDescrObject *descr, void *closure)
{
    if (descr->d_getset->doc == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(descr->d_getset->doc);
}

static PyGetSetDef getset_getset[] = {
    {"__doc__", (getter)getset_get_doc},
    {"__qualname__", (getter)descr_get_qualname},
    {0}
};

static PyObject *
wrapperdescr_get_doc(PyWrapperDescrObject *descr, void *closure)
{
    return _PyType_GetDocFromInternalDoc(descr->d_base->name, descr->d_base->doc);
}

static PyObject *
wrapperdescr_get_text_signature(PyWrapperDescrObject *descr, void *closure)
{
    return _PyType_GetTextSignatureFromInternalDoc(descr->d_base->name, descr->d_base->doc);
}

static PyGetSetDef wrapperdescr_getset[] = {
    {"__doc__", (getter)wrapperdescr_get_doc},
    {"__qualname__", (getter)descr_get_qualname},
    {"__text_signature__", (getter)wrapperdescr_get_text_signature},
    {0}
};

static int
descr_traverse(PyObject *self, visitproc visit, void *arg)
{
    PyDescrObject *descr = (PyDescrObject *)self;
    Py_VISIT(descr->d_type);
    return 0;
}

PyTypeObject PyMethodDescr_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "method_descriptor",
    sizeof(PyMethodDescrObject),
    0,
    (destructor)descr_dealloc,                  /* tp_dealloc */
    offsetof(PyMethodDescrObject, vectorcall),  /* tp_vectorcall_offset */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_as_async */
    (reprfunc)method_repr,                      /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    PyVectorcall_Call,                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
    _Py_TPFLAGS_HAVE_VECTORCALL |
    Py_TPFLAGS_METHOD_DESCRIPTOR,               /* tp_flags */
    0,                                          /* tp_doc */
    descr_traverse,                             /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    descr_methods,                              /* tp_methods */
    descr_members,                              /* tp_members */
    method_getset,                              /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    (descrgetfunc)method_get,                   /* tp_descr_get */
    0,                                          /* tp_descr_set */
};

/* This is for METH_CLASS in C, not for "f = classmethod(f)" in Python! */
PyTypeObject PyClassMethodDescr_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "classmethod_descriptor",
    sizeof(PyMethodDescrObject),
    0,
    (destructor)descr_dealloc,                  /* tp_dealloc */
    0,                                          /* tp_vectorcall_offset */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_as_async */
    (reprfunc)method_repr,                      /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    (ternaryfunc)classmethoddescr_call,         /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC, /* tp_flags */
    0,                                          /* tp_doc */
    descr_traverse,                             /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    descr_methods,                              /* tp_methods */
    descr_members,                              /* tp_members */
    method_getset,                              /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    (descrgetfunc)classmethod_get,              /* tp_descr_get */
    0,                                          /* tp_descr_set */
};

PyTypeObject PyMemberDescr_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "member_descriptor",
    sizeof(PyMemberDescrObject),
    0,
    (destructor)descr_dealloc,                  /* tp_dealloc */
    0,                                          /* tp_vectorcall_offset */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_as_async */
    (reprfunc)member_repr,                      /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC, /* tp_flags */
    0,                                          /* tp_doc */
    descr_traverse,                             /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    descr_methods,                              /* tp_methods */
    descr_members,                              /* tp_members */
    member_getset,                              /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    (descrgetfunc)member_get,                   /* tp_descr_get */
    (descrsetfunc)member_set,                   /* tp_descr_set */
};

PyTypeObject PyGetSetDescr_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "getset_descriptor",
    sizeof(PyGetSetDescrObject),
    0,
    (destructor)descr_dealloc,                  /* tp_dealloc */
    0,                                          /* tp_vectorcall_offset */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_as_async */
    (reprfunc)getset_repr,                      /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC, /* tp_flags */
    0,                                          /* tp_doc */
    descr_traverse,                             /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    0,                                          /* tp_methods */
    descr_members,                              /* tp_members */
    getset_getset,                              /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    (descrgetfunc)getset_get,                   /* tp_descr_get */
    (descrsetfunc)getset_set,                   /* tp_descr_set */
};

PyTypeObject PyWrapperDescr_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "wrapper_descriptor",
    sizeof(PyWrapperDescrObject),
    0,
    (destructor)descr_dealloc,                  /* tp_dealloc */
    0,                                          /* tp_vectorcall_offset */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_as_async */
    (reprfunc)wrapperdescr_repr,                /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    (ternaryfunc)wrapperdescr_call,             /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
    Py_TPFLAGS_METHOD_DESCRIPTOR,               /* tp_flags */
    0,                                          /* tp_doc */
    descr_traverse,                             /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    descr_methods,                              /* tp_methods */
    descr_members,                              /* tp_members */
    wrapperdescr_getset,                        /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    (descrgetfunc)wrapperdescr_get,             /* tp_descr_get */
    0,                                          /* tp_descr_set */
};

static PyDescrObject *
descr_new(PyTypeObject *descrtype, PyTypeObject *type, const char *name)
{
    PyDescrObject *descr;

    descr = (PyDescrObject *)PyType_GenericAlloc(descrtype, 0);
    if (descr != NULL) {
        Py_XINCREF(type);
        descr->d_type = type;
        descr->d_name = PyUnicode_InternFromString(name);
        if (descr->d_name == NULL) {
            Py_DECREF(descr);
            descr = NULL;
        }
        else {
            descr->d_qualname = NULL;
        }
    }
    return descr;
}

PyObject *
PyDescr_NewMethod(PyTypeObject *type, PyMethodDef *method)
{
    /* Figure out correct vectorcall function to use */
    vectorcallfunc vectorcall;
    switch (method->ml_flags & (METH_VARARGS | METH_FASTCALL | METH_NOARGS | METH_O | METH_KEYWORDS))
    {
        case METH_VARARGS:
            vectorcall = method_vectorcall_VARARGS;
            break;
        case METH_VARARGS | METH_KEYWORDS:
            vectorcall = method_vectorcall_VARARGS_KEYWORDS;
            break;
        case METH_FASTCALL:
            vectorcall = method_vectorcall_FASTCALL;
            break;
        case METH_FASTCALL | METH_KEYWORDS:
            vectorcall = method_vectorcall_FASTCALL_KEYWORDS;
            break;
        case METH_NOARGS:
            vectorcall = method_vectorcall_NOARGS;
            break;
        case METH_O:
            vectorcall = method_vectorcall_O;
            break;
        default:
            PyErr_SetString(PyExc_SystemError, "bad call flags");
            return NULL;
    }

    PyMethodDescrObject *descr;

    descr = (PyMethodDescrObject *)descr_new(&PyMethodDescr_Type,
                                             type, method->ml_name);
    if (descr != NULL) {
        descr->d_method = method;
        descr->vectorcall = vectorcall;
    }
    return (PyObject *)descr;
}

PyObject *
PyDescr_NewClassMethod(PyTypeObject *type, PyMethodDef *method)
{
    PyMethodDescrObject *descr;

    descr = (PyMethodDescrObject *)descr_new(&PyClassMethodDescr_Type,
                                             type, method->ml_name);
    if (descr != NULL)
        descr->d_method = method;
    return (PyObject *)descr;
}

PyObject *
PyDescr_NewMember(PyTypeObject *type, PyMemberDef *member)
{
    PyMemberDescrObject *descr;

    descr = (PyMemberDescrObject *)descr_new(&PyMemberDescr_Type,
                                             type, member->name);
    if (descr != NULL)
        descr->d_member = member;
    return (PyObject *)descr;
}

PyObject *
PyDescr_NewGetSet(PyTypeObject *type, PyGetSetDef *getset)
{
    PyGetSetDescrObject *descr;

    descr = (PyGetSetDescrObject *)descr_new(&PyGetSetDescr_Type,
                                             type, getset->name);
    if (descr != NULL)
        descr->d_getset = getset;
    return (PyObject *)descr;
}

PyObject *
PyDescr_NewWrapper(PyTypeObject *type, struct wrapperbase *base, void *wrapped)
{
    PyWrapperDescrObject *descr;

    descr = (PyWrapperDescrObject *)descr_new(&PyWrapperDescr_Type,
                                             type, base->name);
    if (descr != NULL) {
        descr->d_base = base;
        descr->d_wrapped = wrapped;
    }
    return (PyObject *)descr;
}


/* --- mappingproxy: read-only proxy for mappings --- */

/* This has no reason to be in this file except that adding new files is a
   bit of a pain */

typedef struct {
    PyObject_HEAD
    PyObject *mapping;
} mappingproxyobject;

static Py_ssize_t
mappingproxy_len(mappingproxyobject *pp)
{
    return PyObject_Size(pp->mapping);
}

static PyObject *
mappingproxy_getitem(mappingproxyobject *pp, PyObject *key)
{
    return PyObject_GetItem(pp->mapping, key);
}

static PyMappingMethods mappingproxy_as_mapping = {
    (lenfunc)mappingproxy_len,                  /* mp_length */
    (binaryfunc)mappingproxy_getitem,           /* mp_subscript */
    0,                                          /* mp_ass_subscript */
};

static int
mappingproxy_contains(mappingproxyobject *pp, PyObject *key)
{
    if (PyDict_CheckExact(pp->mapping))
        return PyDict_Contains(pp->mapping, key);
    else
        return PySequence_Contains(pp->mapping, key);
}

static PySequenceMethods mappingproxy_as_sequence = {
    0,                                          /* sq_length */
    0,                                          /* sq_concat */
    0,                                          /* sq_repeat */
    0,                                          /* sq_item */
    0,                                          /* sq_slice */
    0,                                          /* sq_ass_item */
    0,                                          /* sq_ass_slice */
    (objobjproc)mappingproxy_contains,                 /* sq_contains */
    0,                                          /* sq_inplace_concat */
    0,                                          /* sq_inplace_repeat */
};

static PyObject *
mappingproxy_get(mappingproxyobject *pp, PyObject *const *args, Py_ssize_t nargs)
{
    /* newargs: mapping, key, default=None */
    PyObject *newargs[3];
    newargs[0] = pp->mapping;
    newargs[2] = Py_None;

    if (!_PyArg_UnpackStack(args, nargs, "get", 1, 2,
                            &newargs[1], &newargs[2]))
    {
        return NULL;
    }
    _Py_IDENTIFIER(get);
    return _PyObject_VectorcallMethodId(&PyId_get, newargs,
                                        3 | PY_VECTORCALL_ARGUMENTS_OFFSET,
                                        NULL);
}

static PyObject *
mappingproxy_keys(mappingproxyobject *pp, PyObject *Py_UNUSED(ignored))
{
    _Py_IDENTIFIER(keys);
    return _PyObject_CallMethodIdNoArgs(pp->mapping, &PyId_keys);
}

static PyObject *
mappingproxy_values(mappingproxyobject *pp, PyObject *Py_UNUSED(ignored))
{
    _Py_IDENTIFIER(values);
    return _PyObject_CallMethodIdNoArgs(pp->mapping, &PyId_values);
}

static PyObject *
mappingproxy_items(mappingproxyobject *pp, PyObject *Py_UNUSED(ignored))
{
    _Py_IDENTIFIER(items);
    return _PyObject_CallMethodIdNoArgs(pp->mapping, &PyId_items);
}

static PyObject *
mappingproxy_copy(mappingproxyobject *pp, PyObject *Py_UNUSED(ignored))
{
    _Py_IDENTIFIER(copy);
    return _PyObject_CallMethodIdNoArgs(pp->mapping, &PyId_copy);
}

/* WARNING: mappingproxy methods must not give access
            to the underlying mapping */

static PyMethodDef mappingproxy_methods[] = {
    {"get",       (PyCFunction)(void(*)(void))mappingproxy_get, METH_FASTCALL,
     PyDoc_STR("D.get(k[,d]) -> D[k] if k in D, else d."
               "  d defaults to None.")},
    {"keys",      (PyCFunction)mappingproxy_keys,       METH_NOARGS,
     PyDoc_STR("D.keys() -> list of D's keys")},
    {"values",    (PyCFunction)mappingproxy_values,     METH_NOARGS,
     PyDoc_STR("D.values() -> list of D's values")},
    {"items",     (PyCFunction)mappingproxy_items,      METH_NOARGS,
     PyDoc_STR("D.items() -> list of D's (key, value) pairs, as 2-tuples")},
    {"copy",      (PyCFunction)mappingproxy_copy,       METH_NOARGS,
     PyDoc_STR("D.copy() -> a shallow copy of D")},
    {0}
};

static void
mappingproxy_dealloc(mappingproxyobject *pp)
{
    _PyObject_GC_UNTRACK(pp);
    Py_DECREF(pp->mapping);
    PyObject_GC_Del(pp);
}

static PyObject *
mappingproxy_getiter(mappingproxyobject *pp)
{
    return PyObject_GetIter(pp->mapping);
}

static PyObject *
mappingproxy_str(mappingproxyobject *pp)
{
    return PyObject_Str(pp->mapping);
}

static PyObject *
mappingproxy_repr(mappingproxyobject *pp)
{
    return PyUnicode_FromFormat("mappingproxy(%R)", pp->mapping);
}

static int
mappingproxy_traverse(PyObject *self, visitproc visit, void *arg)
{
    mappingproxyobject *pp = (mappingproxyobject *)self;
    Py_VISIT(pp->mapping);
    return 0;
}

static PyObject *
mappingproxy_richcompare(mappingproxyobject *v, PyObject *w, int op)
{
    return PyObject_RichCompare(v->mapping, w, op);
}

static int
mappingproxy_check_mapping(PyObject *mapping)
{
    if (!PyMapping_Check(mapping)
        || PyList_Check(mapping)
        || PyTuple_Check(mapping)) {
        PyErr_Format(PyExc_TypeError,
                    "mappingproxy() argument must be a mapping, not %s",
                    Py_TYPE(mapping)->tp_name);
        return -1;
    }
    return 0;
}

/*[clinic input]
@classmethod
mappingproxy.__new__ as mappingproxy_new

    mapping: object

[clinic start generated code]*/

static PyObject *
mappingproxy_new_impl(PyTypeObject *type, PyObject *mapping)
/*[clinic end generated code: output=65f27f02d5b68fa7 input=d2d620d4f598d4f8]*/
{
    mappingproxyobject *mappingproxy;

    if (mappingproxy_check_mapping(mapping) == -1)
        return NULL;

    mappingproxy = PyObject_GC_New(mappingproxyobject, &PyDictProxy_Type);
    if (mappingproxy == NULL)
        return NULL;
    Py_INCREF(mapping);
    mappingproxy->mapping = mapping;
    _PyObject_GC_TRACK(mappingproxy);
    return (PyObject *)mappingproxy;
}

PyObject *
PyDictProxy_New(PyObject *mapping)
{
    mappingproxyobject *pp;

    if (mappingproxy_check_mapping(mapping) == -1)
        return NULL;

    pp = PyObject_GC_New(mappingproxyobject, &PyDictProxy_Type);
    if (pp != NULL) {
        Py_INCREF(mapping);
        pp->mapping = mapping;
        _PyObject_GC_TRACK(pp);
    }
    return (PyObject *)pp;
}


/* --- Wrapper object for "slot" methods --- */

/* This has no reason to be in this file except that adding new files is a
   bit of a pain */

typedef struct {
    PyObject_HEAD
    PyWrapperDescrObject *descr;
    PyObject *self;
} wrapperobject;

#define Wrapper_Check(v) (Py_TYPE(v) == &_PyMethodWrapper_Type)

static void
wrapper_dealloc(wrapperobject *wp)
{
    PyObject_GC_UnTrack(wp);
    Py_TRASHCAN_BEGIN(wp, wrapper_dealloc)
    Py_XDECREF(wp->descr);
    Py_XDECREF(wp->self);
    PyObject_GC_Del(wp);
    Py_TRASHCAN_END
}

static PyObject *
wrapper_richcompare(PyObject *a, PyObject *b, int op)
{
    wrapperobject *wa, *wb;
    int eq;

    assert(a != NULL && b != NULL);

    /* both arguments should be wrapperobjects */
    if ((op != Py_EQ && op != Py_NE)
        || !Wrapper_Check(a) || !Wrapper_Check(b))
    {
        Py_RETURN_NOTIMPLEMENTED;
    }

    wa = (wrapperobject *)a;
    wb = (wrapperobject *)b;
    eq = (wa->descr == wb->descr && wa->self == wb->self);
    if (eq == (op == Py_EQ)) {
        Py_RETURN_TRUE;
    }
    else {
        Py_RETURN_FALSE;
    }
}

static Py_hash_t
wrapper_hash(wrapperobject *wp)
{
    Py_hash_t x, y;
    x = _Py_HashPointer(wp->self);
    y = _Py_HashPointer(wp->descr);
    x = x ^ y;
    if (x == -1)
        x = -2;
    return x;
}

static PyObject *
wrapper_repr(wrapperobject *wp)
{
    return PyUnicode_FromFormat("<method-wrapper '%s' of %s object at %p>",
                               wp->descr->d_base->name,
                               Py_TYPE(wp->self)->tp_name,
                               wp->self);
}

static PyObject *
wrapper_reduce(wrapperobject *wp, PyObject *Py_UNUSED(ignored))
{
    return Py_BuildValue("N(OO)", _PyEval_GetBuiltinId(&PyId_getattr),
                         wp->self, PyDescr_NAME(wp->descr));
}

static PyMethodDef wrapper_methods[] = {
    {"__reduce__", (PyCFunction)wrapper_reduce, METH_NOARGS, NULL},
    {NULL, NULL}
};

static PyMemberDef wrapper_members[] = {
    {"__self__", T_OBJECT, offsetof(wrapperobject, self), READONLY},
    {0}
};

static PyObject *
wrapper_objclass(wrapperobject *wp, void *Py_UNUSED(ignored))
{
    PyObject *c = (PyObject *)PyDescr_TYPE(wp->descr);

    Py_INCREF(c);
    return c;
}

static PyObject *
wrapper_name(wrapperobject *wp, void *Py_UNUSED(ignored))
{
    const char *s = wp->descr->d_base->name;

    return PyUnicode_FromString(s);
}

static PyObject *
wrapper_doc(wrapperobject *wp, void *Py_UNUSED(ignored))
{
    return _PyType_GetDocFromInternalDoc(wp->descr->d_base->name, wp->descr->d_base->doc);
}

static PyObject *
wrapper_text_signature(wrapperobject *wp, void *Py_UNUSED(ignored))
{
    return _PyType_GetTextSignatureFromInternalDoc(wp->descr->d_base->name, wp->descr->d_base->doc);
}

static PyObject *
wrapper_qualname(wrapperobject *wp, void *Py_UNUSED(ignored))
{
    return descr_get_qualname((PyDescrObject *)wp->descr, NULL);
}

static PyGetSetDef wrapper_getsets[] = {
    {"__objclass__", (getter)wrapper_objclass},
    {"__name__", (getter)wrapper_name},
    {"__qualname__", (getter)wrapper_qualname},
    {"__doc__", (getter)wrapper_doc},
    {"__text_signature__", (getter)wrapper_text_signature},
    {0}
};

static PyObject *
wrapper_call(wrapperobject *wp, PyObject *args, PyObject *kwds)
{
    return wrapperdescr_raw_call(wp->descr, wp->self, args, kwds);
}

static int
wrapper_traverse(PyObject *self, visitproc visit, void *arg)
{
    wrapperobject *wp = (wrapperobject *)self;
    Py_VISIT(wp->descr);
    Py_VISIT(wp->self);
    return 0;
}

PyTypeObject _PyMethodWrapper_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "method-wrapper",                           /* tp_name */
    sizeof(wrapperobject),                      /* tp_basicsize */
    0,                                          /* tp_itemsize */
    /* methods */
    (destructor)wrapper_dealloc,                /* tp_dealloc */
    0,                                          /* tp_vectorcall_offset */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_as_async */
    (reprfunc)wrapper_repr,                     /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    (hashfunc)wrapper_hash,                     /* tp_hash */
    (ternaryfunc)wrapper_call,                  /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC, /* tp_flags */
    0,                                          /* tp_doc */
    wrapper_traverse,                           /* tp_traverse */
    0,                                          /* tp_clear */
    wrapper_richcompare,                        /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    wrapper_methods,                            /* tp_methods */
    wrapper_members,                            /* tp_members */
    wrapper_getsets,                            /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
};

PyObject *
PyWrapper_New(PyObject *d, PyObject *self)
{
    wrapperobject *wp;
    PyWrapperDescrObject *descr;

    assert(PyObject_TypeCheck(d, &PyWrapperDescr_Type));
    descr = (PyWrapperDescrObject *)d;
    assert(_PyObject_RealIsSubclass((PyObject *)Py_TYPE(self),
                                    (PyObject *)PyDescr_TYPE(descr)));

    wp = PyObject_GC_New(wrapperobject, &_PyMethodWrapper_Type);
    if (wp != NULL) {
        Py_INCREF(descr);
        wp->descr = descr;
        Py_INCREF(self);
        wp->self = self;
        _PyObject_GC_TRACK(wp);
    }
    return (PyObject *)wp;
}


/* A built-in 'property' type */

/*
class property(object):

    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        if doc is None and fget is not None and hasattr(fget, "__doc__"):
            doc = fget.__doc__
        self.__get = fget
        self.__set = fset
        self.__del = fdel
        self.__doc__ = doc

    def __get__(self, inst, type=None):
        if inst is None:
            return self
        if self.__get is None:
            raise AttributeError, "unreadable attribute"
        return self.__get(inst)

    def __set__(self, inst, value):
        if self.__set is None:
            raise AttributeError, "can't set attribute"
        return self.__set(inst, value)

    def __delete__(self, inst):
        if self.__del is None:
            raise AttributeError, "can't delete attribute"
        return self.__del(inst)

*/

typedef struct {
    PyObject_HEAD
    PyObject *prop_get;
    PyObject *prop_set;
    PyObject *prop_del;
    PyObject *prop_doc;
    int getter_doc;
} propertyobject;

static PyObject * property_copy(PyObject *, PyObject *, PyObject *,
                                  PyObject *);

static PyMemberDef property_members[] = {
    {"fget", T_OBJECT, offsetof(propertyobject, prop_get), READONLY},
    {"fset", T_OBJECT, offsetof(propertyobject, prop_set), READONLY},
    {"fdel", T_OBJECT, offsetof(propertyobject, prop_del), READONLY},
    {"__doc__",  T_OBJECT, offsetof(propertyobject, prop_doc), 0},
    {0}
};


PyDoc_STRVAR(getter_doc,
             "Descriptor to change the getter on a property.");

static PyObject *
property_getter(PyObject *self, PyObject *getter)
{
    return property_copy(self, getter, NULL, NULL);
}


PyDoc_STRVAR(setter_doc,
             "Descriptor to change the setter on a property.");

static PyObject *
property_setter(PyObject *self, PyObject *setter)
{
    return property_copy(self, NULL, setter, NULL);
}


PyDoc_STRVAR(deleter_doc,
             "Descriptor to change the deleter on a property.");

static PyObject *
property_deleter(PyObject *self, PyObject *deleter)
{
    return property_copy(self, NULL, NULL, deleter);
}


static PyMethodDef property_methods[] = {
    {"getter", property_getter, METH_O, getter_doc},
    {"setter", property_setter, METH_O, setter_doc},
    {"deleter", property_deleter, METH_O, deleter_doc},
    {0}
};


static void
property_dealloc(PyObject *self)
{
    propertyobject *gs = (propertyobject *)self;

    _PyObject_GC_UNTRACK(self);
    Py_XDECREF(gs->prop_get);
    Py_XDECREF(gs->prop_set);
    Py_XDECREF(gs->prop_del);
    Py_XDECREF(gs->prop_doc);
    Py_TYPE(self)->tp_free(self);
}

static PyObject *
property_descr_get(PyObject *self, PyObject *obj, PyObject *type)
{
    if (obj == NULL || obj == Py_None) {
        Py_INCREF(self);
        return self;
    }

    propertyobject *gs = (propertyobject *)self;
    if (gs->prop_get == NULL) {
        PyErr_SetString(PyExc_AttributeError, "unreadable attribute");
        return NULL;
    }

    return _PyObject_CallOneArg(gs->prop_get, obj);
}

static int
property_descr_set(PyObject *self, PyObject *obj, PyObject *value)
{
    propertyobject *gs = (propertyobject *)self;
    PyObject *func, *res;

    if (value == NULL)
        func = gs->prop_del;
    else
        func = gs->prop_set;
    if (func == NULL) {
        PyErr_SetString(PyExc_AttributeError,
                        value == NULL ?
                        "can't delete attribute" :
                "can't set attribute");
        return -1;
    }
    if (value == NULL)
        res = _PyObject_CallOneArg(func, obj);
    else
        res = PyObject_CallFunctionObjArgs(func, obj, value, NULL);
    if (res == NULL)
        return -1;
    Py_DECREF(res);
    return 0;
}

static PyObject *
property_copy(PyObject *old, PyObject *get, PyObject *set, PyObject *del)
{
    propertyobject *pold = (propertyobject *)old;
    PyObject *new, *type, *doc;

    type = PyObject_Type(old);
    if (type == NULL)
        return NULL;

    if (get == NULL || get == Py_None) {
        Py_XDECREF(get);
        get = pold->prop_get ? pold->prop_get : Py_None;
    }
    if (set == NULL || set == Py_None) {
        Py_XDECREF(set);
        set = pold->prop_set ? pold->prop_set : Py_None;
    }
    if (del == NULL || del == Py_None) {
        Py_XDECREF(del);
        del = pold->prop_del ? pold->prop_del : Py_None;
    }
    if (pold->getter_doc && get != Py_None) {
        /* make _init use __doc__ from getter */
        doc = Py_None;
    }
    else {
        doc = pold->prop_doc ? pold->prop_doc : Py_None;
    }

    new =  PyObject_CallFunctionObjArgs(type, get, set, del, doc, NULL);
    Py_DECREF(type);
    if (new == NULL)
        return NULL;
    return new;
}

/*[clinic input]
property.__init__ as property_init

    fget: object(c_default="NULL") = None
        function to be used for getting an attribute value
    fset: object(c_default="NULL") = None
        function to be used for setting an attribute value
    fdel: object(c_default="NULL") = None
        function to be used for del'ing an attribute
    doc: object(c_default="NULL") = None
        docstring

Property attribute.

Typical use is to define a managed attribute x:

class C(object):
    def getx(self): return self._x
    def setx(self, value): self._x = value
    def delx(self): del self._x
    x = property(getx, setx, delx, "I'm the 'x' property.")

Decorators make defining new properties or modifying existing ones easy:

class C(object):
    @property
    def x(self):
        "I am the 'x' property."
        return self._x
    @x.setter
    def x(self, value):
        self._x = value
    @x.deleter
    def x(self):
        del self._x
[clinic start generated code]*/

static int
property_init_impl(propertyobject *self, PyObject *fget, PyObject *fset,
                   PyObject *fdel, PyObject *doc)
/*[clinic end generated code: output=01a960742b692b57 input=dfb5dbbffc6932d5]*/
{
    if (fget == Py_None)
        fget = NULL;
    if (fset == Py_None)
        fset = NULL;
    if (fdel == Py_None)
        fdel = NULL;

    Py_XINCREF(fget);
    Py_XINCREF(fset);
    Py_XINCREF(fdel);
    Py_XINCREF(doc);

    Py_XSETREF(self->prop_get, fget);
    Py_XSETREF(self->prop_set, fset);
    Py_XSETREF(self->prop_del, fdel);
    Py_XSETREF(self->prop_doc, doc);
    self->getter_doc = 0;

    /* if no docstring given and the getter has one, use that one */
    if ((doc == NULL || doc == Py_None) && fget != NULL) {
        _Py_IDENTIFIER(__doc__);
        PyObject *get_doc;
        int rc = _PyObject_LookupAttrId(fget, &PyId___doc__, &get_doc);
        if (rc <= 0) {
            return rc;
        }
        if (Py_TYPE(self) == &PyProperty_Type) {
            Py_XSETREF(self->prop_doc, get_doc);
        }
        else {
            /* If this is a property subclass, put __doc__
               in dict of the subclass instance instead,
               otherwise it gets shadowed by __doc__ in the
               class's dict. */
            int err = _PyObject_SetAttrId((PyObject *)self, &PyId___doc__, get_doc);
            Py_DECREF(get_doc);
            if (err < 0)
                return -1;
        }
        self->getter_doc = 1;
    }

    return 0;
}

static PyObject *
property_get___isabstractmethod__(propertyobject *prop, void *closure)
{
    int res = _PyObject_IsAbstract(prop->prop_get);
    if (res == -1) {
        return NULL;
    }
    else if (res) {
        Py_RETURN_TRUE;
    }

    res = _PyObject_IsAbstract(prop->prop_set);
    if (res == -1) {
        return NULL;
    }
    else if (res) {
        Py_RETURN_TRUE;
    }

    res = _PyObject_IsAbstract(prop->prop_del);
    if (res == -1) {
        return NULL;
    }
    else if (res) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static PyGetSetDef property_getsetlist[] = {
    {"__isabstractmethod__",
     (getter)property_get___isabstractmethod__, NULL,
     NULL,
     NULL},
    {NULL} /* Sentinel */
};

static int
property_traverse(PyObject *self, visitproc visit, void *arg)
{
    propertyobject *pp = (propertyobject *)self;
    Py_VISIT(pp->prop_get);
    Py_VISIT(pp->prop_set);
    Py_VISIT(pp->prop_del);
    Py_VISIT(pp->prop_doc);
    return 0;
}

static int
property_clear(PyObject *self)
{
    propertyobject *pp = (propertyobject *)self;
    Py_CLEAR(pp->prop_doc);
    return 0;
}

#include "clinic/descrobject.c.h"

PyTypeObject PyDictProxy_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "mappingproxy",                             /* tp_name */
    sizeof(mappingproxyobject),                 /* tp_basicsize */
    0,                                          /* tp_itemsize */
    /* methods */
    (destructor)mappingproxy_dealloc,           /* tp_dealloc */
    0,                                          /* tp_vectorcall_offset */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_as_async */
    (reprfunc)mappingproxy_repr,                /* tp_repr */
    0,                                          /* tp_as_number */
    &mappingproxy_as_sequence,                  /* tp_as_sequence */
    &mappingproxy_as_mapping,                   /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    (reprfunc)mappingproxy_str,                 /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC, /* tp_flags */
    0,                                          /* tp_doc */
    mappingproxy_traverse,                      /* tp_traverse */
    0,                                          /* tp_clear */
    (richcmpfunc)mappingproxy_richcompare,      /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    (getiterfunc)mappingproxy_getiter,          /* tp_iter */
    0,                                          /* tp_iternext */
    mappingproxy_methods,                       /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    0,                                          /* tp_init */
    0,                                          /* tp_alloc */
    mappingproxy_new,                           /* tp_new */
};

PyTypeObject PyProperty_Type = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    "property",                                 /* tp_name */
    sizeof(propertyobject),                     /* tp_basicsize */
    0,                                          /* tp_itemsize */
    /* methods */
    property_dealloc,                           /* tp_dealloc */
    0,                                          /* tp_vectorcall_offset */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_as_async */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    PyObject_GenericGetAttr,                    /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC |
        Py_TPFLAGS_BASETYPE,                    /* tp_flags */
    property_init__doc__,                       /* tp_doc */
    property_traverse,                          /* tp_traverse */
    (inquiry)property_clear,                    /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    property_methods,                           /* tp_methods */
    property_members,                           /* tp_members */
    property_getsetlist,                        /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    property_descr_get,                         /* tp_descr_get */
    property_descr_set,                         /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    property_init,                              /* tp_init */
    PyType_GenericAlloc,                        /* tp_alloc */
    PyType_GenericNew,                          /* tp_new */
    PyObject_GC_Del,                            /* tp_free */
};


// Experimental code to implement PEP 585 (list[int] etc.)
// TODO: Does this belong in this file?

typedef struct {
    PyObject_HEAD
    PyObject *origin;
    PyObject *args;
    PyObject *parameters;
} gaobject;

static void
ga_dealloc(PyObject *self)
{
    gaobject *alias = (gaobject *)self;

    _PyObject_GC_UNTRACK(self);
    Py_XDECREF(alias->origin);
    Py_XDECREF(alias->args);
    Py_XDECREF(alias->parameters);
    self->ob_type->tp_free(self);
}

static int
ga_traverse(PyObject *self, visitproc visit, void *arg)
{
    gaobject *alias = (gaobject *)self;
    Py_VISIT(alias->origin);
    Py_VISIT(alias->args);
    Py_VISIT(alias->parameters);
    return 0;
}

static int
ga_repr_item(_PyUnicodeWriter *writer, PyObject *p)
{
    PyObject *qualname = PyObject_GetAttrString(p, "__qualname__");
    PyErr_Clear();
    PyObject *module = PyObject_GetAttrString(p, "__module__");
    PyErr_Clear();
    PyObject *r = NULL;
    int err;
    if (PyObject_HasAttrString(p, "__origin__") &&
        PyObject_HasAttrString(p, "__args__"))
    {
        // It looks like a GenericAlias
        r = PyObject_Repr(p);
    }
    else if (p == Py_Ellipsis) {
        // The Ellipsis object
        r = PyUnicode_FromString("...");
    }
    else if (qualname != NULL && module != NULL) {
        // Looks like a class
        if (PyUnicode_CompareWithASCIIString(module, "builtins") == 0) {
            // builtins don't need a module name
            Py_INCREF(qualname);
            r = qualname;
        }
        else {
            r = PyUnicode_FromFormat("%U.%U", module, qualname);
        }
    }
    else {
        // fallback
        r = PyObject_Repr(p);
    }
    if (r == NULL) {
        // error if any of the above PyObject_Repr/PyUnicode_From* fail
        err = -1;
    } else {
        err = _PyUnicodeWriter_WriteStr(writer, r);
    }
    Py_XDECREF(qualname);
    Py_XDECREF(module);
    Py_XDECREF(r);
    return err;
}

static PyObject *
ga_repr(PyObject *self)
{
    gaobject *alias = (gaobject *)self;
    Py_ssize_t len = PyTuple_Size(alias->args);

    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    
    if (ga_repr_item(&writer, alias->origin) < 0) {
        goto error;
    }
    if (_PyUnicodeWriter_WriteASCIIString(&writer, "[", 1) < 0) {
        goto error;
    }
    for (Py_ssize_t i = 0; i < len; i++) {
        if (i > 0) {
            if (_PyUnicodeWriter_WriteASCIIString(&writer, ", ", 2) < 0) {
                goto error;
            }
        }
        PyObject *p = PyTuple_GET_ITEM(alias->args, i);
        if (ga_repr_item(&writer, p) < 0) {
            goto error;
        }
    }
    if (len == 0) {
        // for something like tuple[()] we should print a "()"
        if (_PyUnicodeWriter_WriteASCIIString(&writer, "()", 2) < 0) {
            goto error;
        }
    }
    if (_PyUnicodeWriter_WriteASCIIString(&writer, "]", 1) < 0) {
        goto error;
    }
    return _PyUnicodeWriter_Finish(&writer);
error:
    _PyUnicodeWriter_Dealloc(&writer);
    return NULL;
}

// isinstance(obj, TypeVar) without importing typing.py.  If someone
// names some other class TypeVar, it will be mistaken for a TypeVar.
// Maybe that's a feature; or maybe we'll have to see if
// sys.modules['typing'] exists and look for its 'TypeVar' attribute
// (which is roughly what dataclasses.py uses to recognize ClassVar).
static int
is_typevar(PyObject *obj)
{
    PyTypeObject *type = Py_TYPE(obj);
    return strcmp(type->tp_name, "TypeVar") == 0;
}

// Index of item in self[:len], or -1 if not found (self is a tuple)
static Py_ssize_t
tuple_index(PyObject *self, Py_ssize_t len, PyObject *item)
{
    for (Py_ssize_t i = 0; i < len; i++) {
        if (PyTuple_GET_ITEM(self, i) == item) {
            return i;
        }
    }
    return -1;
}

// tuple(t for t in args if isinstance(t, TypeVar))
static PyObject *
make_parameters(PyObject *args)
{
    Py_ssize_t len = PyTuple_GET_SIZE(args);
    PyObject *parameters = PyTuple_New(len);
    if (parameters == NULL)
        return NULL;
    Py_ssize_t iparam = 0;
    for (Py_ssize_t iarg = 0; iarg < len; iarg++) {
        PyObject *t = PyTuple_GET_ITEM(args, iarg);
        if (is_typevar(t)) {
            if (tuple_index(parameters, iparam, t) < 0) {
                Py_INCREF(t);
                PyTuple_SET_ITEM(parameters, iparam, t);
                iparam++;
            }
        }
    }
    if (iparam < len) {
        if (_PyTuple_Resize(&parameters, iparam) < 0) {
            Py_XDECREF(parameters);
            return NULL;
        }
    }
    return parameters;
}

static PyObject *
ga_getitem(PyObject *self, PyObject *item)
{
    gaobject *alias = (gaobject *)self;
    // do a lookup for __parameters__ so it gets populated (if not already)
    if (alias->parameters == NULL) {
        _Py_IDENTIFIER(__parameters__);
        PyObject *params = _PyObject_GetAttrId(self, &PyId___parameters__);
        if (params == NULL) {
            return NULL;
        }
        Py_DECREF(params);
    }
    Py_ssize_t nparams = PyTuple_GET_SIZE(alias->parameters);
    if (nparams == 0) {
        return PyErr_Format(PyExc_TypeError,
                            "There are no type variables left in %R",
                            self);
    }
    int is_tuple = PyTuple_Check(item);
    Py_ssize_t nitem = is_tuple ? PyTuple_GET_SIZE(item) : 1;
    if (nitem != nparams) {
        return PyErr_Format(PyExc_TypeError,
                            "Too %s arguments for %R",
                            nitem > nparams ? "many" : "few",
                            self);
    }
    Py_ssize_t nargs = PyTuple_GET_SIZE(alias->args);
    PyObject *newargs = PyTuple_New(nargs);
    if (newargs == NULL)
        return NULL;
    for (Py_ssize_t iarg = 0; iarg < nargs; iarg++) {
        PyObject *arg = PyTuple_GET_ITEM(alias->args, iarg);
        if (is_typevar(arg)) {
            Py_ssize_t iparam = tuple_index(alias->parameters, nparams, arg);
            assert(iparam >= 0);
            if (is_tuple) {
                arg = PyTuple_GET_ITEM(item, iparam);
            }
            else {
                assert(iparam == 0);
                arg = item;
            }
        }
        Py_INCREF(arg);
        PyTuple_SET_ITEM(newargs, iarg, arg);
    }
    PyObject *res = Py_GenericAlias(alias->origin, newargs);
    Py_DECREF(newargs);
    return res;
}

static PyMappingMethods ga_as_mapping = {
    .mp_subscript = ga_getitem,
};

static PyObject *
ga_call(PyObject *self, PyObject *args, PyObject *kwds)
{
    gaobject *alias = (gaobject *)self;
    PyObject *obj = PyObject_Call(alias->origin, args, kwds);
    if (obj != NULL) {
        PyObject_SetAttrString(obj, "__orig_class__", self);
        PyErr_Clear();
    }
    return obj;
}

static const char* const attr_exceptions[] = {
    "__origin__",
    "__args__",
    "__parameters__",
    "__mro_entries__",
    "__reduce_ex__",  // needed so we don't look up object.__reduce_ex__
    "__reduce__",
    NULL,
};

static PyObject *
ga_getattro(PyObject *self, PyObject *name)
{
    gaobject *alias = (gaobject *)self;
    if (PyUnicode_Check(name)) {
        for (const char * const *p = attr_exceptions; ; p++) {
            if (*p == NULL) {
                return PyObject_GetAttr(alias->origin, name);
            }
            if (PyUnicode_CompareWithASCIIString(name, *p) == 0) {
                break;
            }
        }
    }
    return PyObject_GenericGetAttr(self, name);
}

static PyObject *
ga_richcompare(PyObject *a, PyObject *b, int op)
{
    if (Py_TYPE(a) != &Py_GenericAliasType ||
        Py_TYPE(b) != &Py_GenericAliasType ||
        (op != Py_EQ && op != Py_NE))
    {
        Py_RETURN_NOTIMPLEMENTED;
    }

    if (op == Py_NE) {
        PyObject *eq = ga_richcompare(a, b, Py_EQ);
        if (eq == NULL)
            return NULL;
        Py_DECREF(eq);
        if (eq == Py_True) {
            Py_RETURN_FALSE;
        }
        else {
            Py_RETURN_TRUE;
        }
    }

    gaobject *aa = (gaobject *)a;
    gaobject *bb = (gaobject *)b;
    PyObject *eq = PyObject_RichCompare(aa->origin, bb->origin, Py_EQ);
    if (eq == NULL)
        return NULL;
    if (eq == Py_False) {
        return eq;
    }
    Py_DECREF(eq);
    return PyObject_RichCompare(aa->args, bb->args, Py_EQ);
}

static PyObject *
ga_mro_entries(PyObject *self, PyObject *args)
{
    gaobject *alias = (gaobject *)self;
    return PyTuple_Pack(1, alias->origin);
}

static PyObject *
ga_instancecheck(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    return PyErr_Format(PyExc_TypeError,
                        "TypeError: Subscripted generics cannot be used with class and instance checks",
                        self);
}

static PyObject *
ga_subclasscheck(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    return PyErr_Format(PyExc_TypeError,
                        "TypeError: Subscripted generics cannot be used with class and instance checks",
                        self);
}

static PyObject *
ga_reduce(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    gaobject *alias = (gaobject *)self;
    return Py_BuildValue("O(OO)", Py_TYPE(alias),
                         alias->origin, alias->args);
}

static PyMethodDef ga_methods[] = {
    {"__mro_entries__", ga_mro_entries, METH_O},
    {"__instancecheck__", ga_instancecheck, METH_O},
    {"__subclasscheck__", ga_subclasscheck, METH_O},
    {"__reduce__", ga_reduce, METH_NOARGS},
    {0}
};

static PyMemberDef ga_members[] = {
    {"__origin__", T_OBJECT, offsetof(gaobject, origin), READONLY},
    {"__args__", T_OBJECT, offsetof(gaobject, args), READONLY},
    {0}
};

static PyObject *
ga_parameters(PyObject *self, void *unused)
{
    gaobject *alias = (gaobject *)self;
    if (alias->parameters == NULL) {
        alias->parameters = make_parameters(alias->args);
        if (alias->parameters == NULL) {
            Py_DECREF(alias->parameters);
            return NULL;
        }
    }
    Py_INCREF(alias->parameters);
    return alias->parameters;
}

static PyGetSetDef ga_properties[] = {
    {"__parameters__", ga_parameters, (setter)NULL, "Type variables in the GenericAlias.", NULL},
    {0}
};

static PyObject *
ga_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    if (kwds != NULL && PyDict_GET_SIZE(kwds) != 0) {
        return PyErr_Format(PyExc_TypeError, "GenericAlias does not support keyword arguments");
    }
    if (PyTuple_Size(args) != 2) {
        return PyErr_Format(PyExc_TypeError, "GenericAlias expects 2 positional arguments");
    }
    PyObject *origin = PyTuple_GET_ITEM(args, 0);
    PyObject *arguments = PyTuple_GET_ITEM(args, 1);
    return Py_GenericAlias(origin, arguments);
}

// TODO:
// - argument clinic?
// - __doc__?
// - cache?
PyTypeObject Py_GenericAliasType = {
    PyVarObject_HEAD_INIT(&PyType_Type, 0)
    .tp_name = "types.GenericAlias",
    .tp_basicsize = sizeof(gaobject),
    .tp_dealloc = ga_dealloc,
    .tp_repr = ga_repr,
    .tp_as_mapping = &ga_as_mapping,
    .tp_call = ga_call,
    .tp_getattro = ga_getattro,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = ga_traverse,
    .tp_richcompare = ga_richcompare,
    .tp_methods = ga_methods,
    .tp_members = ga_members,
    .tp_alloc = PyType_GenericAlloc,
    .tp_new = ga_new,
    .tp_free = PyObject_GC_Del,
    .tp_getset = ga_properties,
};

PyObject *
Py_GenericAlias(PyObject *origin, PyObject *args)
{
    if (!PyTuple_Check(args)) {
        args = PyTuple_Pack(1, args);
        if (args == NULL) {
            return NULL;
        }
    }
    else {
        Py_INCREF(args);
    }

    gaobject *alias = PyObject_GC_New(gaobject, &Py_GenericAliasType);
    if (alias == NULL) {
        Py_DECREF(args);
        return NULL;
    }

    Py_INCREF(origin);
    alias->origin = origin;
    alias->args = args;
    alias->parameters = NULL;
    _PyObject_GC_TRACK(alias);
    return (PyObject *)alias;
}
