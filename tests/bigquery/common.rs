use yachtsql::{Value, YachtSQLEngine, YachtSQLSession};

#[path = "../test_helpers.rs"]
mod test_helpers;

pub use test_helpers::*;

pub fn create_session() -> YachtSQLSession {
    let engine = YachtSQLEngine::new();
    let session = engine.create_session();
    session
        .session()
        .set_variable("PARALLEL_EXECUTION", Value::Bool(true));
    session
        .session()
        .set_variable("OPTIMIZER_LEVEL", Value::String("FULL".to_string()));
    session
}
