#![coverage(off)]
#![allow(dead_code)]
#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::only_used_in_recursion)]

use std::ffi::CString;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use ordered_float::OrderedFloat;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use yachtsql_common::types::Value;

const PY_EXECUTION_TIMEOUT_MS: u64 = 5000;
const PY_CODE_SIZE_LIMIT: usize = 1024 * 1024;
const PY_RECURSION_LIMIT: i32 = 100;

const ALLOWED_BUILTINS: &[&str] = &[
    "abs",
    "all",
    "any",
    "bin",
    "bool",
    "bytearray",
    "bytes",
    "callable",
    "chr",
    "complex",
    "dict",
    "divmod",
    "enumerate",
    "filter",
    "float",
    "format",
    "frozenset",
    "hasattr",
    "hash",
    "hex",
    "int",
    "isinstance",
    "issubclass",
    "iter",
    "len",
    "list",
    "map",
    "max",
    "min",
    "next",
    "oct",
    "ord",
    "pow",
    "print",
    "range",
    "repr",
    "reversed",
    "round",
    "set",
    "slice",
    "sorted",
    "str",
    "sum",
    "tuple",
    "type",
    "zip",
    "None",
    "True",
    "False",
    "Ellipsis",
    "NotImplemented",
    "Exception",
    "BaseException",
    "ValueError",
    "TypeError",
    "KeyError",
    "IndexError",
    "AttributeError",
    "RuntimeError",
    "StopIteration",
    "ZeroDivisionError",
    "OverflowError",
    "ArithmeticError",
    "LookupError",
];

pub fn evaluate_py_function(
    py_code: &str,
    param_names: &[String],
    args: &[Value],
) -> Result<Value, String> {
    if py_code.len() > PY_CODE_SIZE_LIMIT {
        return Err(format!(
            "Python code size {} exceeds limit of {} bytes",
            py_code.len(),
            PY_CODE_SIZE_LIMIT
        ));
    }

    let code = py_code.to_string();
    let names = param_names.to_vec();
    let arguments = args.to_vec();

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let result = execute_py_internal(&code, &names, &arguments);
        let _ = tx.send(result);
    });

    match rx.recv_timeout(Duration::from_millis(PY_EXECUTION_TIMEOUT_MS)) {
        Ok(result) => {
            let _ = handle.join();
            result
        }
        Err(mpsc::RecvTimeoutError::Timeout) => Err(format!(
            "Python execution timed out after {}ms",
            PY_EXECUTION_TIMEOUT_MS
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("Python execution thread panicked".to_string())
        }
    }
}

fn execute_py_internal(
    py_code: &str,
    param_names: &[String],
    args: &[Value],
) -> Result<Value, String> {
    Python::attach(|py| {
        let sys = py
            .import("sys")
            .map_err(|e| format!("Failed to import sys: {}", e))?;
        sys.call_method1("setrecursionlimit", (PY_RECURSION_LIMIT,))
            .map_err(|e| format!("Failed to set recursion limit: {}", e))?;

        let builtins = py
            .import("builtins")
            .map_err(|e| format!("Failed to import builtins: {}", e))?;
        let restricted_builtins = PyDict::new(py);
        for &name in ALLOWED_BUILTINS {
            if let Ok(obj) = builtins.getattr(name) {
                restricted_builtins
                    .set_item(name, obj)
                    .map_err(|e| format!("Failed to set builtin {}: {}", name, e))?;
            }
        }

        let globals = PyDict::new(py);
        globals
            .set_item("__builtins__", restricted_builtins)
            .map_err(|e| format!("Failed to set __builtins__: {}", e))?;

        for (name, value) in param_names.iter().zip(args.iter()) {
            let py_value = value_to_py(py, value)?;
            globals
                .set_item(name.as_str(), py_value)
                .map_err(|e| format!("Failed to set parameter {}: {}", name, e))?;
        }

        let c_code = CString::new(py_code).map_err(|e| format!("Invalid Python code: {}", e))?;
        py.run(&c_code, Some(&globals), None)
            .map_err(|e| format!("Python execution error: {}", e))?;

        let func_name =
            extract_function_name(py_code).ok_or("Cannot find function name in Python code")?;

        let func = globals
            .get_item(&func_name)
            .map_err(|e| format!("Failed to get function: {}", e))?
            .ok_or_else(|| format!("Function '{}' not found", func_name))?;

        let py_args: Vec<Bound<'_, PyAny>> = param_names
            .iter()
            .filter_map(|name| globals.get_item(name.as_str()).ok().flatten())
            .collect();

        let result = func
            .call1((py_args.as_slice(),))
            .or_else(|_| {
                let tuple = pyo3::types::PyTuple::new(py, &py_args)
                    .map_err(|e| format!("Failed to create tuple: {}", e))?;
                func.call1(tuple)
                    .map_err(|e| format!("Python call error: {}", e))
            })
            .map_err(|e| format!("Python call error: {}", e))?;

        py_to_value(py, &result)
    })
}

fn extract_function_name(py_code: &str) -> Option<String> {
    for line in py_code.lines() {
        let line = line.trim();
        if line.starts_with("def ") {
            let rest = line.strip_prefix("def ")?;
            let name_end = rest.find('(')?;
            return Some(rest[..name_end].trim().to_string());
        }
    }
    None
}

fn value_to_py<'py>(py: Python<'py>, value: &Value) -> Result<Bound<'py, PyAny>, String> {
    match value {
        Value::Null => Ok(py.None().into_bound(py)),
        Value::Bool(b) => Ok(b
            .into_pyobject(py)
            .map_err(|e| e.to_string())?
            .to_owned()
            .into_any()),
        Value::Int64(n) => Ok(n.into_pyobject(py).map_err(|e| e.to_string())?.into_any()),
        Value::Float64(f) => Ok(f.0.into_pyobject(py).map_err(|e| e.to_string())?.into_any()),
        Value::String(s) => Ok(s
            .as_str()
            .into_pyobject(py)
            .map_err(|e| e.to_string())?
            .into_any()),
        Value::Bytes(b) => Ok(b
            .as_slice()
            .into_pyobject(py)
            .map_err(|e| e.to_string())?
            .into_any()),
        Value::Array(arr) => {
            let py_list = PyList::empty(py);
            for elem in arr {
                let py_elem = value_to_py(py, elem)?;
                py_list.append(py_elem).map_err(|e| e.to_string())?;
            }
            Ok(py_list.into_any())
        }
        Value::Numeric(n) => {
            let s = n.to_string();
            let decimal_mod = py.import("decimal").map_err(|e| e.to_string())?;
            let decimal_class = decimal_mod.getattr("Decimal").map_err(|e| e.to_string())?;
            decimal_class.call1((s,)).map_err(|e| e.to_string())
        }
        _ => Ok(format!("{:?}", value)
            .into_pyobject(py)
            .map_err(|e| e.to_string())?
            .into_any()),
    }
}

fn py_to_value(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<Value, String> {
    if obj.is_none() {
        return Ok(Value::Null);
    }

    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Bool(b));
    }

    if let Ok(n) = obj.extract::<i64>() {
        return Ok(Value::Int64(n));
    }

    if let Ok(f) = obj.extract::<f64>() {
        return Ok(Value::Float64(OrderedFloat(f)));
    }

    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::String(s));
    }

    if let Ok(bytes) = obj.extract::<Vec<u8>>() {
        return Ok(Value::Bytes(bytes));
    }

    if let Ok(list) = obj.extract::<Bound<'_, PyList>>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(py_to_value(py, &item)?);
        }
        return Ok(Value::Array(arr));
    }

    let repr = obj.repr().map_err(|e| e.to_string())?;
    let repr_str = repr.to_string();
    Ok(Value::String(repr_str))
}
