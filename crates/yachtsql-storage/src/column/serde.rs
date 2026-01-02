use aligned_vec::{AVec, ConstAlign};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub type A64 = ConstAlign<64>;

pub fn serialize_avec_i64<S>(
    data: &AVec<i64, A64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    data.as_slice().serialize(serializer)
}

pub fn deserialize_avec_i64<'de, D>(
    deserializer: D,
) -> std::result::Result<AVec<i64, A64>, D::Error>
where
    D: Deserializer<'de>,
{
    let vec = Vec::<i64>::deserialize(deserializer)?;
    Ok(AVec::from_iter(64, vec))
}

pub fn serialize_avec_f64<S>(
    data: &AVec<f64, A64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    data.as_slice().serialize(serializer)
}

pub fn deserialize_avec_f64<'de, D>(
    deserializer: D,
) -> std::result::Result<AVec<f64, A64>, D::Error>
where
    D: Deserializer<'de>,
{
    let vec = Vec::<f64>::deserialize(deserializer)?;
    Ok(AVec::from_iter(64, vec))
}
