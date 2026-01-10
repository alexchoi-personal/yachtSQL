use std::sync::Arc;

use datafusion::arrow::array::{
    Array, AsArray, BooleanArray, Int64Array, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

pub fn register(ctx: &SessionContext) {
    ctx.register_udf(starts_with_udf());
    ctx.register_udf(ends_with_udf());
    ctx.register_udf(contains_udf());
    ctx.register_udf(strpos_udf());
    ctx.register_udf(instr_udf());
    ctx.register_udf(split_udf());
    ctx.register_udf(byte_length_udf());
    ctx.register_udf(char_length_udf());
    ctx.register_udf(format_udf());
    ctx.register_udf(normalize_udf());
    ctx.register_udf(to_code_points_udf());
    ctx.register_udf(code_points_to_string_udf());
    ctx.register_udf(safe_convert_bytes_to_string_udf());
    ctx.register_udf(net_host_udf());
    ctx.register_udf(net_public_suffix_udf());
    ctx.register_udf(net_reg_domain_udf());
}

fn starts_with_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(StartsWithUdf::new())
}

fn ends_with_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(EndsWithUdf::new())
}

fn contains_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(ContainsUdf::new())
}

fn strpos_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(StrposUdf::new())
}

fn instr_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(InstrUdf::new())
}

fn split_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(SplitUdf::new())
}

fn byte_length_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(ByteLengthUdf::new())
}

fn char_length_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(CharLengthUdf::new())
}

fn format_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(FormatUdf::new())
}

fn normalize_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(NormalizeUdf::new())
}

fn to_code_points_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(ToCodePointsUdf::new())
}

fn code_points_to_string_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(CodePointsToStringUdf::new())
}

fn safe_convert_bytes_to_string_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(SafeConvertBytesToStringUdf::new())
}

fn net_host_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(NetHostUdf::new())
}

fn net_public_suffix_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(NetPublicSuffixUdf::new())
}

fn net_reg_domain_udf() -> datafusion::logical_expr::ScalarUDF {
    datafusion::logical_expr::ScalarUDF::new_from_impl(NetRegDomainUdf::new())
}

#[derive(Debug)]
struct StartsWithUdf {
    signature: Signature,
}

impl StartsWithUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StartsWithUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "starts_with"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let haystack = &args[0];
        let needle = &args[1];

        match (haystack, needle) {
            (ColumnarValue::Array(h), ColumnarValue::Array(n)) => {
                let h_arr = h.as_string::<i32>();
                let n_arr = n.as_string::<i32>();
                let results: BooleanArray = h_arr
                    .iter()
                    .zip(n_arr.iter())
                    .map(|(h, n)| match (h, n) {
                        (Some(h), Some(n)) => Some(h.starts_with(n)),
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (ColumnarValue::Array(h), ColumnarValue::Scalar(ScalarValue::Utf8(Some(n)))) => {
                let h_arr = h.as_string::<i32>();
                let results: BooleanArray = h_arr
                    .iter()
                    .map(|h| h.map(|h| h.starts_with(n.as_str())))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(h))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(n))),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                h.starts_with(n.as_str()),
            )))),
            _ => {
                let results: BooleanArray = (0..num_rows).map(|_| None::<bool>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct EndsWithUdf {
    signature: Signature,
}

impl EndsWithUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EndsWithUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "ends_with"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let haystack = &args[0];
        let needle = &args[1];

        match (haystack, needle) {
            (ColumnarValue::Array(h), ColumnarValue::Array(n)) => {
                let h_arr = h.as_string::<i32>();
                let n_arr = n.as_string::<i32>();
                let results: BooleanArray = h_arr
                    .iter()
                    .zip(n_arr.iter())
                    .map(|(h, n)| match (h, n) {
                        (Some(h), Some(n)) => Some(h.ends_with(n)),
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (ColumnarValue::Array(h), ColumnarValue::Scalar(ScalarValue::Utf8(Some(n)))) => {
                let h_arr = h.as_string::<i32>();
                let results: BooleanArray = h_arr
                    .iter()
                    .map(|h| h.map(|h| h.ends_with(n.as_str())))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(h))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(n))),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                h.ends_with(n.as_str()),
            )))),
            _ => {
                let results: BooleanArray = (0..num_rows).map(|_| None::<bool>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct ContainsUdf {
    signature: Signature,
}

impl ContainsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ContainsUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let haystack = &args[0];
        let needle = &args[1];

        match (haystack, needle) {
            (ColumnarValue::Array(h), ColumnarValue::Array(n)) => {
                let h_arr = h.as_string::<i32>();
                let n_arr = n.as_string::<i32>();
                let results: BooleanArray = h_arr
                    .iter()
                    .zip(n_arr.iter())
                    .map(|(h, n)| match (h, n) {
                        (Some(h), Some(n)) => Some(h.contains(n)),
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (ColumnarValue::Array(h), ColumnarValue::Scalar(ScalarValue::Utf8(Some(n)))) => {
                let h_arr = h.as_string::<i32>();
                let results: BooleanArray = h_arr
                    .iter()
                    .map(|h| h.map(|h| h.contains(n.as_str())))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(h))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(n))),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                h.contains(n.as_str()),
            )))),
            _ => {
                let results: BooleanArray = (0..num_rows).map(|_| None::<bool>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct StrposUdf {
    signature: Signature,
}

impl StrposUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for StrposUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "strpos"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let haystack = &args[0];
        let needle = &args[1];

        match (haystack, needle) {
            (ColumnarValue::Array(h), ColumnarValue::Array(n)) => {
                let h_arr = h.as_string::<i32>();
                let n_arr = n.as_string::<i32>();
                let results: Int64Array = h_arr
                    .iter()
                    .zip(n_arr.iter())
                    .map(|(h, n)| match (h, n) {
                        (Some(h), Some(n)) => Some(h.find(n).map(|p| (p + 1) as i64).unwrap_or(0)),
                        _ => None,
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (ColumnarValue::Array(h), ColumnarValue::Scalar(ScalarValue::Utf8(Some(n)))) => {
                let h_arr = h.as_string::<i32>();
                let results: Int64Array = h_arr
                    .iter()
                    .map(|h| h.map(|h| h.find(n.as_str()).map(|p| (p + 1) as i64).unwrap_or(0)))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(h))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(n))),
            ) => {
                let pos = h.find(n.as_str()).map(|p| (p + 1) as i64).unwrap_or(0);
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(pos))))
            }
            _ => {
                let results: Int64Array = (0..num_rows).map(|_| None::<i64>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct InstrUdf {
    signature: Signature,
}

impl InstrUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for InstrUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "instr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        StrposUdf::new().invoke_batch(args, num_rows)
    }
}

#[derive(Debug)]
struct SplitUdf {
    signature: Signature,
}

impl SplitUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SplitUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "split"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(
            datafusion::arrow::datatypes::Field::new("item", DataType::Utf8, true),
        )))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> DFResult<ColumnarValue> {
        let haystack = &args[0];
        let delimiter = &args[1];

        match (haystack, delimiter) {
            (ColumnarValue::Array(h), ColumnarValue::Scalar(ScalarValue::Utf8(Some(d)))) => {
                let h_arr = h.as_string::<i32>();
                let mut builder = ListBuilder::new(StringBuilder::new());

                for val in h_arr.iter() {
                    match val {
                        Some(s) => {
                            let parts: Vec<&str> = s.split(d.as_str()).collect();
                            for part in parts {
                                builder.values().append_value(part);
                            }
                            builder.append(true);
                        }
                        None => {
                            builder.append(false);
                        }
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            (
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(d))),
            ) => {
                let parts: Vec<&str> = s.split(d.as_str()).collect();
                let mut builder = ListBuilder::new(StringBuilder::new());
                for part in parts {
                    builder.values().append_value(part);
                }
                builder.append(true);
                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "SPLIT requires string arguments".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
struct ByteLengthUdf {
    signature: Signature,
}

impl ByteLengthUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ByteLengthUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "byte_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let input = &args[0];

        match input {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_string::<i32>();
                let results: Int64Array =
                    str_arr.iter().map(|s| s.map(|s| s.len() as i64)).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int64(Some(s.len() as i64)),
            )),
            _ => {
                let results: Int64Array = (0..num_rows).map(|_| None::<i64>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct CharLengthUdf {
    signature: Signature,
}

impl CharLengthUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CharLengthUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "char_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let input = &args[0];

        match input {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_string::<i32>();
                let results: Int64Array = str_arr
                    .iter()
                    .map(|s| s.map(|s| s.chars().count() as i64))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int64(Some(s.chars().count() as i64)),
            )),
            _ => {
                let results: Int64Array = (0..num_rows).map(|_| None::<i64>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct FormatUdf {
    signature: Signature,
}

impl FormatUdf {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FormatUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "format"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        if args.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "FORMAT requires at least one argument".to_string(),
            ));
        }

        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(fmt))) => {
                let result = if args.len() == 1 {
                    fmt.clone()
                } else {
                    let mut result = fmt.clone();
                    for (i, arg) in args.iter().skip(1).enumerate() {
                        let placeholder = format!("%{}", i + 1);
                        let value = match arg {
                            ColumnarValue::Scalar(v) => v.to_string(),
                            ColumnarValue::Array(arr) => {
                                if !arr.is_empty() {
                                    format!("{:?}", arr)
                                } else {
                                    String::new()
                                }
                            }
                        };
                        result = result.replace(&placeholder, &value);
                    }
                    result
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            _ => {
                let results: StringArray = (0..num_rows).map(|_| None::<&str>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct NormalizeUdf {
    signature: Signature,
}

impl NormalizeUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NormalizeUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "normalize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let input = &args[0];

        match input {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_string::<i32>();
                let results: StringArray =
                    str_arr.iter().map(|s| s.map(|s| s.to_string())).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.clone()))))
            }
            _ => {
                let results: StringArray = (0..num_rows).map(|_| None::<&str>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct ToCodePointsUdf {
    signature: Signature,
}

impl ToCodePointsUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToCodePointsUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "to_code_points"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::List(Arc::new(
            datafusion::arrow::datatypes::Field::new("item", DataType::Int64, false),
        )))
    }

    fn invoke_batch(&self, args: &[ColumnarValue], _num_rows: usize) -> DFResult<ColumnarValue> {
        let input = &args[0];

        match input {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_string::<i32>();
                let mut builder = ListBuilder::new(datafusion::arrow::array::Int64Builder::new());

                for val in str_arr.iter() {
                    match val {
                        Some(s) => {
                            for c in s.chars() {
                                builder.values().append_value(c as i64);
                            }
                            builder.append(true);
                        }
                        None => {
                            builder.append(false);
                        }
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                let code_points: Vec<i64> = s.chars().map(|c| c as i64).collect();
                let arr = Int64Array::from(code_points);
                let list_arr = datafusion::arrow::array::ListArray::from_iter_primitive::<
                    datafusion::arrow::datatypes::Int64Type,
                    _,
                    _,
                >(vec![Some(arr.iter().collect::<Vec<_>>())]);
                Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::new(list_arr))))
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "TO_CODE_POINTS requires a string argument".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
struct CodePointsToStringUdf {
    signature: Signature,
}

impl CodePointsToStringUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CodePointsToStringUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "code_points_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let input = &args[0];

        match input {
            ColumnarValue::Scalar(ScalarValue::List(list_arr)) => {
                if list_arr.len() > 0 {
                    let values = list_arr.value(0);
                    let int_arr = values.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
                    let s: String = int_arr
                        .iter()
                        .filter_map(|v| v.and_then(|v| char::from_u32(v as u32)))
                        .collect();
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
                }
            }
            _ => {
                let results: StringArray = (0..num_rows).map(|_| None::<&str>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct SafeConvertBytesToStringUdf {
    signature: Signature,
}

impl SafeConvertBytesToStringUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SafeConvertBytesToStringUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "safe_convert_bytes_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let input = &args[0];

        match input {
            ColumnarValue::Array(arr) => {
                let bin_arr = arr.as_binary::<i32>();
                let results: StringArray = bin_arr
                    .iter()
                    .map(|b| b.and_then(|b| std::str::from_utf8(b).ok().map(|s| s.to_string())))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            ColumnarValue::Scalar(ScalarValue::Binary(Some(b))) => {
                let result = std::str::from_utf8(b).ok().map(|s| s.to_string());
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            _ => {
                let results: StringArray = (0..num_rows).map(|_| None::<&str>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct NetHostUdf {
    signature: Signature,
}

impl NetHostUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NetHostUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "net.host"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let input = &args[0];

        fn extract_host(url: &str) -> Option<String> {
            let url = url.trim();
            let without_protocol = url
                .strip_prefix("https://")
                .or_else(|| url.strip_prefix("http://"))
                .or_else(|| url.strip_prefix("//"))
                .unwrap_or(url);

            let host = without_protocol.split('/').next()?;
            let host = host.split('@').next_back()?;
            let host = host.split(':').next()?;

            if host.is_empty() {
                None
            } else {
                Some(host.to_lowercase())
            }
        }

        match input {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_string::<i32>();
                let results: StringArray =
                    str_arr.iter().map(|s| s.and_then(extract_host)).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(extract_host(s))))
            }
            _ => {
                let results: StringArray = (0..num_rows).map(|_| None::<&str>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct NetPublicSuffixUdf {
    signature: Signature,
}

impl NetPublicSuffixUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NetPublicSuffixUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "net.public_suffix"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let input = &args[0];

        fn extract_public_suffix(url: &str) -> Option<String> {
            let url = url.trim().to_lowercase();
            let without_protocol = url
                .strip_prefix("https://")
                .or_else(|| url.strip_prefix("http://"))
                .or_else(|| url.strip_prefix("//"))
                .unwrap_or(&url);

            let host = without_protocol.split('/').next()?;
            let host = host.split('@').next_back()?;
            let host = host.split(':').next()?;

            let parts: Vec<&str> = host.split('.').collect();
            if parts.len() >= 2 {
                Some(parts.last()?.to_string())
            } else {
                None
            }
        }

        match input {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_string::<i32>();
                let results: StringArray = str_arr
                    .iter()
                    .map(|s| s.and_then(extract_public_suffix))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(extract_public_suffix(s)),
            )),
            _ => {
                let results: StringArray = (0..num_rows).map(|_| None::<&str>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[derive(Debug)]
struct NetRegDomainUdf {
    signature: Signature,
}

impl NetRegDomainUdf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NetRegDomainUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "net.reg_domain"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], num_rows: usize) -> DFResult<ColumnarValue> {
        let input = &args[0];

        fn extract_reg_domain(url: &str) -> Option<String> {
            let url = url.trim().to_lowercase();
            let without_protocol = url
                .strip_prefix("https://")
                .or_else(|| url.strip_prefix("http://"))
                .or_else(|| url.strip_prefix("//"))
                .unwrap_or(&url);

            let host = without_protocol.split('/').next()?;
            let host = host.split('@').next_back()?;
            let host = host.split(':').next()?;

            let parts: Vec<&str> = host.split('.').collect();
            if parts.len() >= 2 {
                let domain = parts[parts.len() - 2..].join(".");
                Some(domain)
            } else {
                None
            }
        }

        match input {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_string::<i32>();
                let results: StringArray = str_arr
                    .iter()
                    .map(|s| s.and_then(extract_reg_domain))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(extract_reg_domain(s)),
            )),
            _ => {
                let results: StringArray = (0..num_rows).map(|_| None::<&str>).collect();
                Ok(ColumnarValue::Array(Arc::new(results)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_starts_with() {
        let h = StringArray::from(vec!["hello", "world", "hi"]);
        let n = StringArray::from(vec!["he", "wo", "bye"]);
        let args = vec![
            ColumnarValue::Array(Arc::new(h)),
            ColumnarValue::Array(Arc::new(n)),
        ];
        let result = StartsWithUdf::new().invoke_batch(&args, 3).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let bool_arr = arr.as_boolean();
                assert!(bool_arr.value(0));
                assert!(bool_arr.value(1));
                assert!(!bool_arr.value(2));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_strpos() {
        let h = StringArray::from(vec!["hello", "world"]);
        let n = StringArray::from(vec!["ll", "or"]);
        let args = vec![
            ColumnarValue::Array(Arc::new(h)),
            ColumnarValue::Array(Arc::new(n)),
        ];
        let result = StrposUdf::new().invoke_batch(&args, 2).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
                assert_eq!(int_arr.value(0), 3);
                assert_eq!(int_arr.value(1), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_byte_length() {
        let input = StringArray::from(vec!["hello", "日本語"]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = ByteLengthUdf::new().invoke_batch(&args, 2).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
                assert_eq!(int_arr.value(0), 5);
                assert_eq!(int_arr.value(1), 9);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_char_length() {
        let input = StringArray::from(vec!["hello", "日本語"]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = CharLengthUdf::new().invoke_batch(&args, 2).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
                assert_eq!(int_arr.value(0), 5);
                assert_eq!(int_arr.value(1), 3);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_net_host() {
        let input = StringArray::from(vec![
            "https://www.example.com/path",
            "http://subdomain.example.org:8080/page",
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = NetHostUdf::new().invoke_batch(&args, 2).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let str_arr = arr.as_string::<i32>();
                assert_eq!(str_arr.value(0), "www.example.com");
                assert_eq!(str_arr.value(1), "subdomain.example.org");
            }
            _ => panic!("Expected array"),
        }
    }
}
