#![coverage(off)]

pub mod array;
pub mod binary_ops;
pub mod comparison;
pub mod conversion;
pub mod crypto;
pub mod datetime;
pub mod distance;
pub mod format;
pub mod from_json;
pub mod generate;
pub mod geo;
pub mod helpers;
pub mod interval;
pub mod json;
pub mod lax;
pub mod map;
pub mod math;
pub mod net;
pub mod nulls;
pub mod range;
pub mod string;
pub mod trig;
pub mod vectorized;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::ScalarFunction;

pub fn dispatch(func: &ScalarFunction, args: &[Value]) -> Result<Value> {
    match func {
        ScalarFunction::Abs => math::fn_abs(args),
        ScalarFunction::Floor => math::fn_floor(args),
        ScalarFunction::Ceil => math::fn_ceil(args),
        ScalarFunction::Round => math::fn_round(args),
        ScalarFunction::Sqrt => math::fn_sqrt(args),
        ScalarFunction::Cbrt => math::fn_cbrt(args),
        ScalarFunction::Power => math::fn_power(args),
        ScalarFunction::Pow => math::fn_power(args),
        ScalarFunction::Mod => math::fn_mod(args),
        ScalarFunction::Sign => math::fn_sign(args),
        ScalarFunction::Exp => math::fn_exp(args),
        ScalarFunction::Ln => math::fn_ln(args),
        ScalarFunction::Log => math::fn_log(args),
        ScalarFunction::Log10 => math::fn_log10(args),
        ScalarFunction::Trunc => math::fn_trunc(args),
        ScalarFunction::Div => math::fn_div(args),
        ScalarFunction::SafeDivide => math::fn_safe_divide(args),
        ScalarFunction::IeeeDivide => math::fn_ieee_divide(args),
        ScalarFunction::SafeMultiply => math::fn_safe_multiply(args),
        ScalarFunction::SafeAdd => math::fn_safe_add(args),
        ScalarFunction::SafeSubtract => math::fn_safe_subtract(args),
        ScalarFunction::SafeNegate => math::fn_safe_negate(args),
        ScalarFunction::Rand => generate::fn_rand(args),
        ScalarFunction::RandCanonical => generate::fn_rand_canonical(args),
        ScalarFunction::IsNan => math::fn_is_nan(args),
        ScalarFunction::IsInf => math::fn_is_inf(args),

        ScalarFunction::Sin => trig::fn_sin(args),
        ScalarFunction::Cos => trig::fn_cos(args),
        ScalarFunction::Tan => trig::fn_tan(args),
        ScalarFunction::Asin => trig::fn_asin(args),
        ScalarFunction::Acos => trig::fn_acos(args),
        ScalarFunction::Atan => trig::fn_atan(args),
        ScalarFunction::Atan2 => trig::fn_atan2(args),
        ScalarFunction::Sinh => trig::fn_sinh(args),
        ScalarFunction::Cosh => trig::fn_cosh(args),
        ScalarFunction::Tanh => trig::fn_tanh(args),
        ScalarFunction::Asinh => trig::fn_asinh(args),
        ScalarFunction::Acosh => trig::fn_acosh(args),
        ScalarFunction::Atanh => trig::fn_atanh(args),
        ScalarFunction::Cot => trig::fn_cot(args),
        ScalarFunction::Csc => trig::fn_csc(args),
        ScalarFunction::Sec => trig::fn_sec(args),
        ScalarFunction::Coth => trig::fn_coth(args),
        ScalarFunction::Csch => trig::fn_csch(args),
        ScalarFunction::Sech => trig::fn_sech(args),
        ScalarFunction::Pi => math::fn_pi(args),

        ScalarFunction::Upper => string::fn_upper(args),
        ScalarFunction::Lower => string::fn_lower(args),
        ScalarFunction::Length => string::fn_length(args),
        ScalarFunction::Trim => string::fn_trim(args),
        ScalarFunction::LTrim => string::fn_ltrim(args),
        ScalarFunction::RTrim => string::fn_rtrim(args),
        ScalarFunction::Substr => string::fn_substr(args),
        ScalarFunction::Concat => string::fn_concat(args),
        ScalarFunction::Replace => string::fn_replace(args),
        ScalarFunction::Reverse => string::fn_reverse(args),
        ScalarFunction::Left => string::fn_left(args),
        ScalarFunction::Right => string::fn_right(args),
        ScalarFunction::Repeat => string::fn_repeat(args),
        ScalarFunction::StartsWith => string::fn_starts_with(args),
        ScalarFunction::EndsWith => string::fn_ends_with(args),
        ScalarFunction::Contains => string::fn_contains(args),
        ScalarFunction::Strpos => string::fn_strpos(args),
        ScalarFunction::Instr => string::fn_instr(args),
        ScalarFunction::Split => string::fn_split(args),
        ScalarFunction::Initcap => string::fn_initcap(args),
        ScalarFunction::Lpad => string::fn_lpad(args),
        ScalarFunction::Rpad => string::fn_rpad(args),
        ScalarFunction::Translate => string::fn_translate(args),
        ScalarFunction::Soundex => string::fn_soundex(args),
        ScalarFunction::Normalize => string::fn_normalize(args),
        ScalarFunction::NormalizeAndCasefold => string::fn_normalize_and_casefold(args),
        ScalarFunction::ToCodePoints => string::fn_to_code_points(args),
        ScalarFunction::CodePointsToString => string::fn_code_points_to_string(args),
        ScalarFunction::CodePointsToBytes => string::fn_code_points_to_bytes(args),
        ScalarFunction::ByteLength => string::fn_byte_length(args),
        ScalarFunction::CharLength => string::fn_char_length(args),
        ScalarFunction::Ascii => string::fn_ascii(args),
        ScalarFunction::Chr => string::fn_chr(args),
        ScalarFunction::Unicode => string::fn_unicode(args),
        ScalarFunction::RegexpContains => string::fn_regexp_contains(args),
        ScalarFunction::RegexpExtract => string::fn_regexp_extract(args),
        ScalarFunction::RegexpExtractAll => string::fn_regexp_extract_all(args),
        ScalarFunction::RegexpInstr => string::fn_regexp_instr(args),
        ScalarFunction::RegexpReplace => string::fn_regexp_replace(args),
        ScalarFunction::RegexpSubstr => string::fn_regexp_substr(args),
        ScalarFunction::EditDistance => string::fn_edit_distance(args),
        ScalarFunction::ContainsSubstr => string::fn_contains_substr(args),

        ScalarFunction::Coalesce => nulls::fn_coalesce(args),
        ScalarFunction::IfNull => nulls::fn_ifnull(args),
        ScalarFunction::Ifnull => nulls::fn_ifnull(args),
        ScalarFunction::NullIf => nulls::fn_nullif(args),
        ScalarFunction::If => nulls::fn_if(args),
        ScalarFunction::Zeroifnull => nulls::fn_zeroifnull(args),
        ScalarFunction::Nvl => nulls::fn_nvl(args),
        ScalarFunction::Nvl2 => nulls::fn_nvl2(args),

        ScalarFunction::CurrentDate => datetime::fn_current_date(args),
        ScalarFunction::CurrentTimestamp => datetime::fn_current_timestamp(args),
        ScalarFunction::CurrentTime => datetime::fn_current_time(args),
        ScalarFunction::CurrentDatetime => datetime::fn_current_datetime(args),
        ScalarFunction::Extract => datetime::fn_extract(args),
        ScalarFunction::DateAdd => datetime::fn_date_add(args),
        ScalarFunction::DateSub => datetime::fn_date_sub(args),
        ScalarFunction::DateDiff => datetime::fn_date_diff(args),
        ScalarFunction::DateTrunc => datetime::fn_date_trunc(args),
        ScalarFunction::DateBucket => datetime::fn_date_bucket(args),
        ScalarFunction::DatetimeTrunc => datetime::fn_datetime_trunc(args),
        ScalarFunction::TimestampTrunc => datetime::fn_timestamp_trunc(args),
        ScalarFunction::TimeTrunc => datetime::fn_time_trunc(args),
        ScalarFunction::Date => datetime::fn_date(args),
        ScalarFunction::Time => datetime::fn_time(args),
        ScalarFunction::Datetime => datetime::fn_datetime(args),
        ScalarFunction::Timestamp => datetime::fn_timestamp(args),
        ScalarFunction::TimestampMicros => datetime::fn_timestamp_micros(args),
        ScalarFunction::TimestampMillis => datetime::fn_timestamp_millis(args),
        ScalarFunction::TimestampSeconds => datetime::fn_timestamp_seconds(args),
        ScalarFunction::UnixDate => datetime::fn_unix_date(args),
        ScalarFunction::UnixMicros => datetime::fn_unix_micros(args),
        ScalarFunction::UnixMillis => datetime::fn_unix_millis(args),
        ScalarFunction::UnixSeconds => datetime::fn_unix_seconds(args),
        ScalarFunction::DateFromUnixDate => datetime::fn_date_from_unix_date(args),
        ScalarFunction::LastDay => datetime::fn_last_day(args),
        ScalarFunction::DatetimeBucket => datetime::fn_datetime_bucket(args),
        ScalarFunction::TimestampBucket => datetime::fn_timestamp_bucket(args),

        ScalarFunction::FormatDate => datetime::fn_format_date(args),
        ScalarFunction::FormatTimestamp => datetime::fn_format_timestamp(args),
        ScalarFunction::FormatDatetime => datetime::fn_format_datetime(args),
        ScalarFunction::FormatTime => datetime::fn_format_time(args),
        ScalarFunction::Format => format::fn_format(args),
        ScalarFunction::ParseDate => datetime::fn_parse_date(args),
        ScalarFunction::ParseTimestamp => datetime::fn_parse_timestamp(args),
        ScalarFunction::ParseDatetime => datetime::fn_parse_datetime(args),
        ScalarFunction::ParseTime => datetime::fn_parse_time(args),

        ScalarFunction::MakeInterval => interval::fn_make_interval(args),
        ScalarFunction::JustifyDays => interval::fn_justify_days(args),
        ScalarFunction::JustifyHours => interval::fn_justify_hours(args),
        ScalarFunction::JustifyInterval => interval::fn_justify_interval(args),

        ScalarFunction::ArrayLength => array::fn_array_length(args),
        ScalarFunction::ArrayToString => array::fn_array_to_string(args),
        ScalarFunction::ArrayConcat => array::fn_array_concat(args),
        ScalarFunction::ArrayReverse => array::fn_array_reverse(args),
        ScalarFunction::ArrayContains => array::fn_array_contains(args),
        ScalarFunction::ArrayTransform => array::fn_array_transform(args),
        ScalarFunction::ArrayFilter => array::fn_array_filter(args),
        ScalarFunction::ArrayIncludes => array::fn_array_includes(args),
        ScalarFunction::ArrayIncludesAny => array::fn_array_includes_any(args),
        ScalarFunction::ArrayIncludesAll => array::fn_array_includes_all(args),
        ScalarFunction::ArrayFirst => array::fn_array_first(args),
        ScalarFunction::ArrayFirstN => array::fn_array_first_n(args),
        ScalarFunction::ArrayLast => array::fn_array_last(args),
        ScalarFunction::ArrayLastN => array::fn_array_last_n(args),
        ScalarFunction::ArrayMin => array::fn_array_min(args),
        ScalarFunction::ArrayMax => array::fn_array_max(args),
        ScalarFunction::ArraySum => array::fn_array_sum(args),
        ScalarFunction::ArrayAvg => array::fn_array_avg(args),
        ScalarFunction::ArrayOffset => array::fn_array_offset(args),
        ScalarFunction::ArrayOrdinal => array::fn_array_ordinal(args),
        ScalarFunction::ArraySlice => array::fn_array_slice(args),
        ScalarFunction::ArrayFlatten => array::fn_array_flatten(args),
        ScalarFunction::ArrayDistinct => array::fn_array_distinct(args),
        ScalarFunction::ArrayPosition => array::fn_array_position(args),
        ScalarFunction::ArrayCompact => array::fn_array_compact(args),
        ScalarFunction::ArraySort => array::fn_array_sort(args),
        ScalarFunction::ArrayZip => array::fn_array_zip(args),
        ScalarFunction::Unnest => array::fn_unnest(args),
        ScalarFunction::SafeOffset => array::fn_safe_offset(args),
        ScalarFunction::SafeOrdinal => array::fn_safe_ordinal(args),

        ScalarFunction::GenerateArray => array::fn_generate_array(args),
        ScalarFunction::GenerateDateArray => datetime::fn_generate_date_array(args),
        ScalarFunction::GenerateTimestampArray => datetime::fn_generate_timestamp_array(args),
        ScalarFunction::GenerateUuid => generate::fn_generate_uuid(args),

        ScalarFunction::Struct => conversion::fn_struct(args),
        ScalarFunction::String => conversion::fn_string(args),
        ScalarFunction::SafeCast => conversion::fn_safe_cast(args),
        ScalarFunction::Cast => conversion::fn_cast(args),
        ScalarFunction::SafeConvert => conversion::fn_safe_convert(args),
        ScalarFunction::TypeOf => conversion::fn_type_of(args),
        ScalarFunction::SafeConvertBytesToString => {
            conversion::fn_safe_convert_bytes_to_string(args)
        }
        ScalarFunction::ConvertBytesToString => conversion::fn_convert_bytes_to_string(args),
        ScalarFunction::ToBase64 => conversion::fn_to_base64(args),
        ScalarFunction::FromBase64 => conversion::fn_from_base64(args),
        ScalarFunction::ToBase32 => conversion::fn_to_base32(args),
        ScalarFunction::FromBase32 => conversion::fn_from_base32(args),
        ScalarFunction::ToHex => conversion::fn_to_hex(args),
        ScalarFunction::FromHex => conversion::fn_from_hex(args),
        ScalarFunction::BitCount => conversion::fn_bit_count(args),

        ScalarFunction::ToJson => json::fn_to_json(args),
        ScalarFunction::ToJsonString => json::fn_to_json_string(args),
        ScalarFunction::JsonExtract => json::fn_json_extract(args),
        ScalarFunction::JsonExtractScalar => json::fn_json_extract_scalar(args),
        ScalarFunction::JsonExtractArray => json::fn_json_extract_array(args),
        ScalarFunction::JsonExtractStringArray => json::fn_json_extract_string_array(args),
        ScalarFunction::JsonQuery => json::fn_json_query(args),
        ScalarFunction::JsonValue => json::fn_json_value(args),
        ScalarFunction::JsonQueryArray => json::fn_json_query_array(args),
        ScalarFunction::JsonValueArray => json::fn_json_value_array(args),
        ScalarFunction::ParseJson => json::fn_parse_json(args),
        ScalarFunction::JsonType => json::fn_json_type(args),
        ScalarFunction::JsonKeys => json::fn_json_keys(args),
        ScalarFunction::JsonArrayLength => json::fn_json_array_length(args),

        ScalarFunction::Int64FromJson => from_json::fn_int64_from_json(args),
        ScalarFunction::Float64FromJson => from_json::fn_float64_from_json(args),
        ScalarFunction::BoolFromJson => from_json::fn_bool_from_json(args),
        ScalarFunction::StringFromJson => from_json::fn_string_from_json(args),

        ScalarFunction::Md5 => crypto::fn_md5(args),
        ScalarFunction::Sha1 => crypto::fn_sha1(args),
        ScalarFunction::Sha256 => crypto::fn_sha256(args),
        ScalarFunction::Sha512 => crypto::fn_sha512(args),
        ScalarFunction::FarmFingerprint => crypto::fn_farm_fingerprint(args),

        ScalarFunction::NetHost => net::fn_net_host(args),
        ScalarFunction::NetPublicSuffix => net::fn_net_public_suffix(args),
        ScalarFunction::NetRegDomain => net::fn_net_reg_domain(args),
        ScalarFunction::NetIpFromString => net::fn_net_ip_from_string(args),
        ScalarFunction::NetIpToString => net::fn_net_ip_to_string(args),
        ScalarFunction::NetIpNetMask => net::fn_net_ip_net_mask(args),
        ScalarFunction::NetIpTrunc => net::fn_net_ip_trunc(args),
        ScalarFunction::NetSafeIpFromString => net::fn_net_safe_ip_from_string(args),

        ScalarFunction::Range => range::fn_range(args),
        ScalarFunction::RangeBucket => range::fn_range_bucket(args),

        ScalarFunction::CosineDistance => distance::fn_cosine_distance(args),
        ScalarFunction::EuclideanDistance => distance::fn_euclidean_distance(args),

        ScalarFunction::Greatest => comparison::fn_greatest(args),
        ScalarFunction::Least => comparison::fn_least(args),

        ScalarFunction::SessionUser => format::fn_session_user(args),
        ScalarFunction::Error => format::fn_error(args),

        ScalarFunction::Custom(name) => Err(Error::unsupported(format!(
            "Custom scalar function '{}' requires context for dispatch",
            name
        ))),
    }
}
