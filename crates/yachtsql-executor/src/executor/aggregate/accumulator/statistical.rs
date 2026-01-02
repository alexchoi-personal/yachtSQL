#![coverage(off)]

use ordered_float::OrderedFloat;
use yachtsql_common::types::Value;

use super::utils::value_to_f64;

#[derive(Clone)]
pub(crate) struct VarianceAccumulator {
    pub(crate) count: i64,
    pub(crate) mean: f64,
    pub(crate) m2: f64,
    pub(crate) is_sample: bool,
    pub(crate) is_stddev: bool,
}

impl VarianceAccumulator {
    pub(crate) fn new_variance_sample() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            is_sample: true,
            is_stddev: false,
        }
    }

    pub(crate) fn new_variance_pop() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            is_sample: false,
            is_stddev: false,
        }
    }

    pub(crate) fn new_stddev_sample() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            is_sample: true,
            is_stddev: true,
        }
    }

    pub(crate) fn new_stddev_pop() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            is_sample: false,
            is_stddev: true,
        }
    }

    pub(crate) fn accumulate(&mut self, value: &Value) {
        if let Some(x) = value_to_f64(value) {
            self.count += 1;
            let delta = x - self.mean;
            self.mean += delta / self.count as f64;
            let delta2 = x - self.mean;
            self.m2 += delta * delta2;
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        if self.count < 2 && self.is_sample {
            return Value::Null;
        }
        if self.count == 0 {
            return Value::Null;
        }
        let divisor = if self.is_sample {
            self.count - 1
        } else {
            self.count
        };
        let variance = self.m2 / divisor as f64;
        let result = if self.is_stddev {
            variance.sqrt()
        } else {
            variance
        };
        Value::Float64(OrderedFloat(result))
    }
}

#[derive(Clone, Copy)]
pub(crate) enum CovarianceStatType {
    Covariance,
    Correlation,
}

#[derive(Clone)]
pub(crate) struct CovarianceAccumulator {
    pub(crate) count: i64,
    pub(crate) mean_x: f64,
    pub(crate) mean_y: f64,
    pub(crate) c_xy: f64,
    pub(crate) m2_x: f64,
    pub(crate) m2_y: f64,
    pub(crate) is_sample: bool,
    pub(crate) stat_type: CovarianceStatType,
}

impl CovarianceAccumulator {
    pub(crate) fn new_covar_pop() -> Self {
        Self {
            count: 0,
            mean_x: 0.0,
            mean_y: 0.0,
            c_xy: 0.0,
            m2_x: 0.0,
            m2_y: 0.0,
            is_sample: false,
            stat_type: CovarianceStatType::Covariance,
        }
    }

    pub(crate) fn new_covar_samp() -> Self {
        Self {
            count: 0,
            mean_x: 0.0,
            mean_y: 0.0,
            c_xy: 0.0,
            m2_x: 0.0,
            m2_y: 0.0,
            is_sample: true,
            stat_type: CovarianceStatType::Covariance,
        }
    }

    pub(crate) fn new_correlation() -> Self {
        Self {
            count: 0,
            mean_x: 0.0,
            mean_y: 0.0,
            c_xy: 0.0,
            m2_x: 0.0,
            m2_y: 0.0,
            is_sample: false,
            stat_type: CovarianceStatType::Correlation,
        }
    }

    pub(crate) fn accumulate_bivariate(&mut self, x: &Value, y: &Value) {
        let x_val = value_to_f64(x);
        let y_val = value_to_f64(y);

        if let (Some(x), Some(y)) = (x_val, y_val) {
            self.count += 1;
            let n = self.count as f64;
            let delta_x = x - self.mean_x;
            let delta_y = y - self.mean_y;
            self.mean_x += delta_x / n;
            self.mean_y += delta_y / n;
            let delta_x2 = x - self.mean_x;
            let delta_y2 = y - self.mean_y;
            self.c_xy += delta_x * delta_y2;
            self.m2_x += delta_x * delta_x2;
            self.m2_y += delta_y * delta_y2;
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        if self.count < 2 {
            return Value::Null;
        }
        let divisor = if self.is_sample {
            (self.count - 1) as f64
        } else {
            self.count as f64
        };
        match self.stat_type {
            CovarianceStatType::Covariance => Value::Float64(OrderedFloat(self.c_xy / divisor)),
            CovarianceStatType::Correlation => {
                let var_x = self.m2_x / self.count as f64;
                let var_y = self.m2_y / self.count as f64;
                if var_x <= 0.0 || var_y <= 0.0 {
                    return Value::Null;
                }
                let std_x = var_x.sqrt();
                let std_y = var_y.sqrt();
                let cov = self.c_xy / self.count as f64;
                let corr = cov / (std_x * std_y);
                Value::Float64(OrderedFloat(corr))
            }
        }
    }
}
