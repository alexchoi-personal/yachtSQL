pub mod constant_folding;
pub mod cross_to_hash_join;
pub(crate) mod cte_optimization;
pub mod empty_propagation;
pub mod filter_merging;
pub mod limit_pushdown;
pub mod predicate_simplification;
pub mod sort_elimination;
pub mod trivial_predicate;

pub use constant_folding::fold_constants;
pub use cross_to_hash_join::apply_cross_to_hash_join;
pub use empty_propagation::apply_empty_propagation;
pub use filter_merging::apply_filter_merging;
pub use limit_pushdown::apply_limit_pushdown;
pub use predicate_simplification::apply_predicate_simplification;
pub use sort_elimination::apply_sort_elimination;
pub use trivial_predicate::apply_trivial_predicate_removal;
