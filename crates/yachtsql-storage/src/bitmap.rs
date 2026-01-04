#![coverage(off)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NullBitmap {
    data: Vec<u64>,
    len: usize,
}

impl NullBitmap {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            len: 0,
        }
    }

    pub fn new_valid(len: usize) -> Self {
        let num_words = len.div_ceil(64);
        Self {
            data: vec![0; num_words],
            len,
        }
    }

    pub fn new_null(len: usize) -> Self {
        let num_words = len.div_ceil(64);
        Self {
            data: vec![u64::MAX; num_words],
            len,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn is_null(&self, index: usize) -> bool {
        if index >= self.len {
            return true;
        }
        let word = index / 64;
        let bit = index % 64;
        (self.data[word] >> bit) & 1 == 1
    }

    #[inline]
    pub fn is_valid(&self, index: usize) -> bool {
        !self.is_null(index)
    }

    #[inline]
    pub fn set(&mut self, index: usize, is_null: bool) {
        if index >= self.len {
            return;
        }
        let word = index / 64;
        let bit = index % 64;
        if is_null {
            self.data[word] |= 1 << bit;
        } else {
            self.data[word] &= !(1 << bit);
        }
    }

    #[inline]
    pub fn set_valid(&mut self, index: usize) {
        self.set(index, false);
    }

    #[inline]
    pub fn set_null(&mut self, index: usize) {
        self.set(index, true);
    }

    pub fn push(&mut self, is_null: bool) {
        let word = self.len / 64;
        let bit = self.len % 64;
        if word >= self.data.len() {
            self.data.push(0);
        }
        if is_null {
            self.data[word] |= 1 << bit;
        }
        self.len += 1;
    }

    pub fn remove(&mut self, index: usize) {
        if index >= self.len {
            return;
        }
        for i in index..self.len - 1 {
            let next_null = self.is_null(i + 1);
            self.set(i, next_null);
        }
        self.len -= 1;
        let num_words = self.len.div_ceil(64);
        self.data.truncate(num_words.max(1));
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.len = 0;
    }

    pub fn count_null(&self) -> usize {
        if self.len == 0 {
            return 0;
        }
        let full_words = self.len / 64;
        let remaining_bits = self.len % 64;
        let mut count: usize = self.data[..full_words]
            .iter()
            .map(|w| w.count_ones() as usize)
            .sum();
        if remaining_bits > 0 && full_words < self.data.len() {
            let mask = (1u64 << remaining_bits) - 1;
            count += (self.data[full_words] & mask).count_ones() as usize;
        }
        count
    }

    pub fn count_valid(&self) -> usize {
        self.len - self.count_null()
    }

    pub fn is_all_null(&self) -> bool {
        self.len > 0 && self.count_null() == self.len
    }

    pub fn words(&self) -> &[u64] {
        &self.data
    }

    pub fn from_words(data: Vec<u64>, len: usize) -> Self {
        Self { data, len }
    }

    pub fn union(&self, other: &NullBitmap) -> NullBitmap {
        let len = self.len.max(other.len);
        if len == 0 {
            return NullBitmap::new();
        }
        let num_words = len.div_ceil(64);
        let mut data = vec![0u64; num_words];
        for (i, word) in data.iter_mut().enumerate() {
            let lw = self.data.get(i).copied().unwrap_or(0);
            let rw = other.data.get(i).copied().unwrap_or(0);
            *word = lw | rw;
        }
        NullBitmap { data, len }
    }

    pub fn gather(&self, indices: &[usize]) -> NullBitmap {
        let len = indices.len();
        if len == 0 {
            return NullBitmap::new();
        }
        let num_words = len.div_ceil(64);
        let mut data = vec![0u64; num_words];
        for (out_idx, &src_idx) in indices.iter().enumerate() {
            if self.is_null(src_idx) {
                let word = out_idx / 64;
                let bit = out_idx % 64;
                data[word] |= 1 << bit;
            }
        }
        NullBitmap { data, len }
    }

    pub fn extend(&mut self, other: &NullBitmap) {
        if other.len == 0 {
            return;
        }

        let start_bit = self.len % 64;

        if start_bit == 0 {
            self.data.extend_from_slice(&other.data);
        } else {
            let shift = start_bit;
            let inv_shift = 64 - shift;

            for (i, &word) in other.data.iter().enumerate() {
                let low_bits = word << shift;
                let high_bits = word >> inv_shift;

                if let Some(last) = self.data.last_mut() {
                    *last |= low_bits;
                } else {
                    self.data.push(low_bits);
                }

                let other_full_words = other.len / 64;
                let is_last_word = i == other.data.len() - 1;
                let other_remaining = other.len % 64;

                if !is_last_word || (other_remaining > inv_shift) || (i < other_full_words) {
                    self.data.push(high_bits);
                }
            }
        }

        self.len += other.len;

        let num_words = self.len.div_ceil(64);
        self.data.truncate(num_words);
    }
}

impl Default for NullBitmap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_valid() {
        let bitmap = NullBitmap::new_valid(100);
        assert_eq!(bitmap.len(), 100);
        for i in 0..100 {
            assert!(bitmap.is_valid(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_null() {
        let bitmap = NullBitmap::new_null(100);
        assert_eq!(bitmap.len(), 100);
        for i in 0..100 {
            assert!(bitmap.is_null(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_push_and_check() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(false);
        bitmap.push(true);
        bitmap.push(false);
        assert_eq!(bitmap.len(), 3);
        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_null(1));
        assert!(bitmap.is_valid(2));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set() {
        let mut bitmap = NullBitmap::new_valid(10);
        bitmap.set(5, true);
        assert!(bitmap.is_null(5));
        bitmap.set(5, false);
        assert!(bitmap.is_valid(5));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(false);
        bitmap.push(true);
        bitmap.push(false);
        bitmap.remove(1);
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_valid(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count() {
        let mut bitmap = NullBitmap::new();
        for i in 0..100 {
            bitmap.push(i % 3 == 0);
        }
        assert_eq!(bitmap.count_null(), 34);
        assert_eq!(bitmap.count_valid(), 66);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_across_word_boundary() {
        let mut bitmap = NullBitmap::new();
        for i in 0..130 {
            bitmap.push(i % 2 == 0);
        }
        assert_eq!(bitmap.len(), 130);
        for i in 0..130 {
            assert_eq!(bitmap.is_null(i), i % 2 == 0);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_empty() {
        let bitmap = NullBitmap::new();
        assert_eq!(bitmap.len(), 0);
        assert!(bitmap.is_empty());
        assert_eq!(bitmap.words().len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_default() {
        let bitmap = NullBitmap::default();
        assert_eq!(bitmap.len(), 0);
        assert!(bitmap.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_empty() {
        let mut bitmap = NullBitmap::new();
        assert!(bitmap.is_empty());
        bitmap.push(false);
        assert!(!bitmap.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_null_out_of_bounds() {
        let bitmap = NullBitmap::new_valid(10);
        assert!(bitmap.is_null(10));
        assert!(bitmap.is_null(100));
        assert!(bitmap.is_null(usize::MAX));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_out_of_bounds() {
        let mut bitmap = NullBitmap::new_valid(10);
        bitmap.set(100, true);
        assert_eq!(bitmap.len(), 10);
        for i in 0..10 {
            assert!(bitmap.is_valid(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_valid() {
        let mut bitmap = NullBitmap::new_null(10);
        bitmap.set_valid(5);
        assert!(bitmap.is_valid(5));
        for i in 0..10 {
            if i != 5 {
                assert!(bitmap.is_null(i));
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_null() {
        let mut bitmap = NullBitmap::new_valid(10);
        bitmap.set_null(5);
        assert!(bitmap.is_null(5));
        for i in 0..10 {
            if i != 5 {
                assert!(bitmap.is_valid(i));
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_out_of_bounds() {
        let mut bitmap = NullBitmap::new_valid(10);
        bitmap.remove(100);
        assert_eq!(bitmap.len(), 10);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_last_element() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.remove(0);
        assert_eq!(bitmap.len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_shifts_bits_across_boundary() {
        let mut bitmap = NullBitmap::new();
        for i in 0..70 {
            bitmap.push(i == 63 || i == 64);
        }
        assert!(bitmap.is_null(63));
        assert!(bitmap.is_null(64));
        bitmap.remove(0);
        assert_eq!(bitmap.len(), 69);
        assert!(bitmap.is_null(62));
        assert!(bitmap.is_null(63));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_clear() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..100 {
            bitmap.push(true);
        }
        assert_eq!(bitmap.len(), 100);
        bitmap.clear();
        assert_eq!(bitmap.len(), 0);
        assert!(bitmap.is_empty());
        assert_eq!(bitmap.words().len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_null_empty() {
        let bitmap = NullBitmap::new();
        assert_eq!(bitmap.count_null(), 0);
        assert_eq!(bitmap.count_valid(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_null_exact_word_boundary() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..64 {
            bitmap.push(true);
        }
        assert_eq!(bitmap.count_null(), 64);
        assert_eq!(bitmap.count_valid(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_null_partial_word() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..65 {
            bitmap.push(true);
        }
        assert_eq!(bitmap.count_null(), 65);
        assert_eq!(bitmap.count_valid(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_all_null_true() {
        let bitmap = NullBitmap::new_null(100);
        assert!(bitmap.is_all_null());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_all_null_false() {
        let mut bitmap = NullBitmap::new_null(100);
        bitmap.set_valid(50);
        assert!(!bitmap.is_all_null());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_all_null_empty() {
        let bitmap = NullBitmap::new();
        assert!(!bitmap.is_all_null());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_words() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(true);
        let words = bitmap.words();
        assert_eq!(words.len(), 1);
        assert_eq!(words[0], 0b101);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_words_multiple() {
        let bitmap = NullBitmap::new_null(130);
        let words = bitmap.words();
        assert_eq!(words.len(), 3);
        assert_eq!(words[0], u64::MAX);
        assert_eq!(words[1], u64::MAX);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_extend() {
        let mut bitmap1 = NullBitmap::new();
        bitmap1.push(true);
        bitmap1.push(false);

        let mut bitmap2 = NullBitmap::new();
        bitmap2.push(false);
        bitmap2.push(true);
        bitmap2.push(true);

        bitmap1.extend(&bitmap2);
        assert_eq!(bitmap1.len(), 5);
        assert!(bitmap1.is_null(0));
        assert!(bitmap1.is_valid(1));
        assert!(bitmap1.is_valid(2));
        assert!(bitmap1.is_null(3));
        assert!(bitmap1.is_null(4));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_extend_empty() {
        let mut bitmap1 = NullBitmap::new();
        bitmap1.push(true);
        let bitmap2 = NullBitmap::new();
        bitmap1.extend(&bitmap2);
        assert_eq!(bitmap1.len(), 1);
        assert!(bitmap1.is_null(0));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_extend_across_word_boundary() {
        let mut bitmap1 = NullBitmap::new();
        for _ in 0..60 {
            bitmap1.push(false);
        }

        let mut bitmap2 = NullBitmap::new();
        for i in 0..10 {
            bitmap2.push(i % 2 == 0);
        }

        bitmap1.extend(&bitmap2);
        assert_eq!(bitmap1.len(), 70);
        for i in 0..60 {
            assert!(bitmap1.is_valid(i));
        }
        for i in 60..70 {
            assert_eq!(bitmap1.is_null(i), (i - 60) % 2 == 0);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_valid_zero_length() {
        let bitmap = NullBitmap::new_valid(0);
        assert_eq!(bitmap.len(), 0);
        assert!(bitmap.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_null_zero_length() {
        let bitmap = NullBitmap::new_null(0);
        assert_eq!(bitmap.len(), 0);
        assert!(bitmap.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_push_triggers_new_word() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..64 {
            bitmap.push(false);
        }
        assert_eq!(bitmap.words().len(), 1);
        bitmap.push(true);
        assert_eq!(bitmap.words().len(), 2);
        assert!(bitmap.is_null(64));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_at_word_boundary() {
        let mut bitmap = NullBitmap::new_valid(128);
        bitmap.set_null(63);
        bitmap.set_null(64);
        bitmap.set_null(127);
        assert!(bitmap.is_null(63));
        assert!(bitmap.is_null(64));
        assert!(bitmap.is_null(127));
        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_valid(62));
        assert!(bitmap.is_valid(65));
        assert!(bitmap.is_valid(126));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_from_beginning() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(true);
        bitmap.remove(0);
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_null(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_from_end() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(true);
        bitmap.remove(2);
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.is_null(0));
        assert!(bitmap.is_valid(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_valid_out_of_bounds() {
        let bitmap = NullBitmap::new_valid(5);
        assert!(!bitmap.is_valid(5));
        assert!(!bitmap.is_valid(100));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_partial_eq() {
        let bitmap1 = NullBitmap::new_valid(10);
        let bitmap2 = NullBitmap::new_valid(10);
        let bitmap3 = NullBitmap::new_null(10);
        assert_eq!(bitmap1, bitmap2);
        assert_ne!(bitmap1, bitmap3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_clone() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.push(false);
        let cloned = bitmap.clone();
        assert_eq!(bitmap, cloned);
        assert_eq!(cloned.len(), 2);
        assert!(cloned.is_null(0));
        assert!(cloned.is_valid(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_truncates_words() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..65 {
            bitmap.push(false);
        }
        assert_eq!(bitmap.words().len(), 2);
        bitmap.remove(64);
        assert_eq!(bitmap.len(), 64);
        assert_eq!(bitmap.words().len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_with_partial_last_word() {
        let mut bitmap = NullBitmap::new();
        for i in 0..100 {
            bitmap.push(i < 64);
        }
        assert_eq!(bitmap.count_null(), 64);
        assert_eq!(bitmap.count_valid(), 36);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_serialize_deserialize() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(true);
        let serialized = serde_json::to_string(&bitmap).unwrap();
        let deserialized: NullBitmap = serde_json::from_str(&serialized).unwrap();
        assert_eq!(bitmap, deserialized);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_serialize_deserialize_empty() {
        let bitmap = NullBitmap::new();
        let serialized = serde_json::to_string(&bitmap).unwrap();
        let deserialized: NullBitmap = serde_json::from_str(&serialized).unwrap();
        assert_eq!(bitmap, deserialized);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_serialize_deserialize_large() {
        let bitmap = NullBitmap::new_null(200);
        let serialized = serde_json::to_string(&bitmap).unwrap();
        let deserialized: NullBitmap = serde_json::from_str(&serialized).unwrap();
        assert_eq!(bitmap, deserialized);
        assert_eq!(deserialized.len(), 200);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_debug_format() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        let debug_str = format!("{:?}", bitmap);
        assert!(debug_str.contains("NullBitmap"));
        assert!(debug_str.contains("data"));
        assert!(debug_str.contains("len"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_extend_to_empty() {
        let mut bitmap1 = NullBitmap::new();
        let mut bitmap2 = NullBitmap::new();
        bitmap2.push(true);
        bitmap2.push(false);
        bitmap1.extend(&bitmap2);
        assert_eq!(bitmap1.len(), 2);
        assert!(bitmap1.is_null(0));
        assert!(bitmap1.is_valid(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_extend_both_empty() {
        let mut bitmap1 = NullBitmap::new();
        let bitmap2 = NullBitmap::new();
        bitmap1.extend(&bitmap2);
        assert_eq!(bitmap1.len(), 0);
        assert!(bitmap1.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_middle_multiple_times() {
        let mut bitmap = NullBitmap::new();
        for i in 0..10 {
            bitmap.push(i % 2 == 0);
        }
        bitmap.remove(5);
        bitmap.remove(5);
        bitmap.remove(5);
        assert_eq!(bitmap.len(), 7);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_to_same_value() {
        let mut bitmap = NullBitmap::new_valid(10);
        bitmap.set(5, false);
        assert!(bitmap.is_valid(5));
        let mut bitmap2 = NullBitmap::new_null(10);
        bitmap2.set(5, true);
        assert!(bitmap2.is_null(5));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_valid_exact_word_boundary() {
        let bitmap = NullBitmap::new_valid(64);
        assert_eq!(bitmap.len(), 64);
        assert_eq!(bitmap.words().len(), 1);
        for i in 0..64 {
            assert!(bitmap.is_valid(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_null_exact_word_boundary() {
        let bitmap = NullBitmap::new_null(64);
        assert_eq!(bitmap.len(), 64);
        assert_eq!(bitmap.words().len(), 1);
        for i in 0..64 {
            assert!(bitmap.is_null(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_valid_one_over_boundary() {
        let bitmap = NullBitmap::new_valid(65);
        assert_eq!(bitmap.len(), 65);
        assert_eq!(bitmap.words().len(), 2);
        for i in 0..65 {
            assert!(bitmap.is_valid(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_null_one_over_boundary() {
        let bitmap = NullBitmap::new_null(65);
        assert_eq!(bitmap.len(), 65);
        assert_eq!(bitmap.words().len(), 2);
        for i in 0..65 {
            assert!(bitmap.is_null(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_null_multiple_full_words() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..128 {
            bitmap.push(true);
        }
        assert_eq!(bitmap.count_null(), 128);
        assert_eq!(bitmap.count_valid(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_valid_multiple_full_words() {
        let bitmap = NullBitmap::new_valid(128);
        assert_eq!(bitmap.count_null(), 0);
        assert_eq!(bitmap.count_valid(), 128);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_null_on_empty_bitmap() {
        let bitmap = NullBitmap::new();
        assert!(bitmap.is_null(0));
        assert!(bitmap.is_null(100));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_valid_on_empty_bitmap() {
        let bitmap = NullBitmap::new();
        assert!(!bitmap.is_valid(0));
        assert!(!bitmap.is_valid(100));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_valid_out_of_bounds() {
        let mut bitmap = NullBitmap::new_null(10);
        bitmap.set_valid(100);
        assert_eq!(bitmap.len(), 10);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_null_out_of_bounds() {
        let mut bitmap = NullBitmap::new_valid(10);
        bitmap.set_null(100);
        assert_eq!(bitmap.len(), 10);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_all_elements_one_by_one() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(true);
        bitmap.remove(0);
        bitmap.remove(0);
        bitmap.remove(0);
        assert_eq!(bitmap.len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_push_alternating_across_multiple_words() {
        let mut bitmap = NullBitmap::new();
        for i in 0..200 {
            bitmap.push(i % 2 == 0);
        }
        assert_eq!(bitmap.len(), 200);
        for i in 0..200 {
            assert_eq!(bitmap.is_null(i), i % 2 == 0);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_across_all_bits_in_word() {
        let mut bitmap = NullBitmap::new_valid(64);
        for i in 0..64 {
            bitmap.set_null(i);
            assert!(bitmap.is_null(i));
        }
        for i in 0..64 {
            bitmap.set_valid(i);
            assert!(bitmap.is_valid(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_extend_large_bitmaps() {
        let bitmap1_len = 100;
        let bitmap2_len = 150;
        let mut bitmap1 = NullBitmap::new_valid(bitmap1_len);
        let bitmap2 = NullBitmap::new_null(bitmap2_len);
        bitmap1.extend(&bitmap2);
        assert_eq!(bitmap1.len(), bitmap1_len + bitmap2_len);
        for i in 0..bitmap1_len {
            assert!(bitmap1.is_valid(i));
        }
        for i in bitmap1_len..(bitmap1_len + bitmap2_len) {
            assert!(bitmap1.is_null(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_words_content_after_operations() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..64 {
            bitmap.push(true);
        }
        assert_eq!(bitmap.words()[0], u64::MAX);
        bitmap.set_valid(0);
        assert_eq!(bitmap.words()[0], u64::MAX - 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_null_with_mixed_words() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..64 {
            bitmap.push(true);
        }
        for _ in 0..64 {
            bitmap.push(false);
        }
        for _ in 0..32 {
            bitmap.push(true);
        }
        assert_eq!(bitmap.count_null(), 96);
        assert_eq!(bitmap.count_valid(), 64);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_preserves_correct_bits() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(true);
        bitmap.push(false);
        bitmap.remove(2);
        assert_eq!(bitmap.len(), 4);
        assert!(bitmap.is_null(0));
        assert!(bitmap.is_null(1));
        assert!(bitmap.is_null(2));
        assert!(bitmap.is_valid(3));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_all_null_single_element_null() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        assert!(bitmap.is_all_null());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_all_null_single_element_valid() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(false);
        assert!(!bitmap.is_all_null());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_single_element() {
        let bitmap = NullBitmap::new_valid(1);
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.is_valid(0));
        let bitmap2 = NullBitmap::new_null(1);
        assert_eq!(bitmap2.len(), 1);
        assert!(bitmap2.is_null(0));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_clear_and_reuse() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..100 {
            bitmap.push(true);
        }
        bitmap.clear();
        assert!(bitmap.is_empty());
        bitmap.push(false);
        bitmap.push(true);
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_null(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_partial_eq_different_lengths() {
        let bitmap1 = NullBitmap::new_valid(10);
        let bitmap2 = NullBitmap::new_valid(20);
        assert_ne!(bitmap1, bitmap2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_partial_eq_same_length_different_bits() {
        let mut bitmap1 = NullBitmap::new_valid(64);
        let bitmap2 = NullBitmap::new_valid(64);
        bitmap1.set_null(32);
        assert_ne!(bitmap1, bitmap2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_null_no_remaining_bits() {
        let bitmap = NullBitmap::new_null(64);
        assert_eq!(bitmap.count_null(), 64);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_to_zero_length() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        assert_eq!(bitmap.len(), 1);
        assert_eq!(bitmap.words().len(), 1);
        bitmap.remove(0);
        assert_eq!(bitmap.len(), 0);
        assert_eq!(bitmap.words().len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_across_word_boundary_shrinks() {
        let mut bitmap = NullBitmap::new();
        for i in 0..129 {
            bitmap.push(i % 2 == 0);
        }
        assert_eq!(bitmap.words().len(), 3);
        assert_eq!(bitmap.len(), 129);
        bitmap.remove(128);
        assert_eq!(bitmap.len(), 128);
        assert_eq!(bitmap.words().len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_push_valid_at_word_boundary() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..63 {
            bitmap.push(false);
        }
        assert_eq!(bitmap.words().len(), 1);
        bitmap.push(false);
        assert_eq!(bitmap.words().len(), 1);
        assert!(bitmap.is_valid(63));
        bitmap.push(false);
        assert_eq!(bitmap.words().len(), 2);
        assert!(bitmap.is_valid(64));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_push_null_at_word_boundary() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..64 {
            bitmap.push(false);
        }
        assert_eq!(bitmap.words().len(), 1);
        bitmap.push(true);
        assert_eq!(bitmap.words().len(), 2);
        assert!(bitmap.is_null(64));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_null_192_elements() {
        let mut bitmap = NullBitmap::new();
        for i in 0..192 {
            bitmap.push(!(64..128).contains(&i));
        }
        assert_eq!(bitmap.count_null(), 128);
        assert_eq!(bitmap.count_valid(), 64);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_at_bit_63() {
        let mut bitmap = NullBitmap::new_valid(64);
        bitmap.set_null(63);
        assert!(bitmap.is_null(63));
        assert!(bitmap.is_valid(62));
        bitmap.set_valid(63);
        assert!(bitmap.is_valid(63));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_at_bit_0() {
        let mut bitmap = NullBitmap::new_valid(64);
        bitmap.set_null(0);
        assert!(bitmap.is_null(0));
        assert!(bitmap.is_valid(1));
        bitmap.set_valid(0);
        assert!(bitmap.is_valid(0));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_when_len_equals_65() {
        let mut bitmap = NullBitmap::new();
        for i in 0..65 {
            bitmap.push(i == 64);
        }
        assert_eq!(bitmap.words().len(), 2);
        bitmap.remove(64);
        assert_eq!(bitmap.len(), 64);
        assert_eq!(bitmap.words().len(), 1);
        for i in 0..64 {
            assert!(bitmap.is_valid(i));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_words_after_push() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        assert_eq!(bitmap.words()[0], 1);
        bitmap.push(true);
        assert_eq!(bitmap.words()[0], 3);
        bitmap.push(false);
        assert_eq!(bitmap.words()[0], 3);
        bitmap.push(true);
        assert_eq!(bitmap.words()[0], 11);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_null_at_exact_boundary() {
        let bitmap = NullBitmap::new_valid(64);
        assert!(bitmap.is_valid(63));
        assert!(bitmap.is_null(64));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_null_when_full_words_equals_data_len() {
        let bitmap = NullBitmap::new_null(128);
        assert_eq!(bitmap.count_null(), 128);
        assert_eq!(bitmap.count_valid(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_shifts_nulls_correctly() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(false);
        bitmap.push(true);
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(true);
        bitmap.remove(1);
        assert_eq!(bitmap.len(), 4);
        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_null(1));
        assert!(bitmap.is_valid(2));
        assert!(bitmap.is_null(3));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_extend_self_with_itself_equivalent() {
        let mut bitmap1 = NullBitmap::new();
        bitmap1.push(true);
        bitmap1.push(false);
        let bitmap2 = bitmap1.clone();
        bitmap1.extend(&bitmap2);
        assert_eq!(bitmap1.len(), 4);
        assert!(bitmap1.is_null(0));
        assert!(bitmap1.is_valid(1));
        assert!(bitmap1.is_null(2));
        assert!(bitmap1.is_valid(3));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_on_empty_bitmap() {
        let mut bitmap = NullBitmap::new();
        bitmap.set(0, true);
        assert_eq!(bitmap.len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_on_empty_bitmap() {
        let mut bitmap = NullBitmap::new();
        bitmap.remove(0);
        assert_eq!(bitmap.len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_null_single_word_partial() {
        let mut bitmap = NullBitmap::new();
        for i in 0..32 {
            bitmap.push(i % 2 == 0);
        }
        assert_eq!(bitmap.count_null(), 16);
        assert_eq!(bitmap.count_valid(), 16);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_valid_large() {
        let bitmap = NullBitmap::new_valid(1000);
        assert_eq!(bitmap.len(), 1000);
        assert_eq!(bitmap.count_null(), 0);
        assert_eq!(bitmap.count_valid(), 1000);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_new_null_large() {
        let bitmap = NullBitmap::new_null(1000);
        assert_eq!(bitmap.len(), 1000);
        assert_eq!(bitmap.count_null(), 1000);
        assert_eq!(bitmap.count_valid(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_all_null_with_single_valid() {
        let mut bitmap = NullBitmap::new_null(100);
        assert!(bitmap.is_all_null());
        bitmap.set_valid(99);
        assert!(!bitmap.is_all_null());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_first_of_many() {
        let mut bitmap = NullBitmap::new();
        for i in 0..100 {
            bitmap.push(i % 3 == 0);
        }
        let original_count = bitmap.count_null();
        bitmap.remove(0);
        assert_eq!(bitmap.len(), 99);
        assert_eq!(bitmap.count_null(), original_count - 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_push_after_clear() {
        let mut bitmap = NullBitmap::new();
        for _ in 0..100 {
            bitmap.push(true);
        }
        bitmap.clear();
        bitmap.push(false);
        bitmap.push(true);
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_null(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_words_span_three_words() {
        let bitmap = NullBitmap::new_null(150);
        let words = bitmap.words();
        assert_eq!(words.len(), 3);
        assert_eq!(words[0], u64::MAX);
        assert_eq!(words[1], u64::MAX);
        assert_eq!(words[2], u64::MAX);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_null_with_zero_remaining() {
        let bitmap = NullBitmap::new_valid(64);
        assert_eq!(bitmap.count_null(), 0);
        assert_eq!(bitmap.len() % 64, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_set_toggle_multiple_times() {
        let mut bitmap = NullBitmap::new_valid(10);
        for _ in 0..10 {
            bitmap.set_null(5);
            assert!(bitmap.is_null(5));
            bitmap.set_valid(5);
            assert!(bitmap.is_valid(5));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_is_null_returns_true_for_index_at_len() {
        let bitmap = NullBitmap::new_valid(10);
        assert!(bitmap.is_null(10));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_extend_preserves_original() {
        let mut bitmap1 = NullBitmap::new();
        bitmap1.push(true);
        bitmap1.push(false);
        let bitmap2 = NullBitmap::new();
        bitmap1.extend(&bitmap2);
        assert_eq!(bitmap1.len(), 2);
        assert!(bitmap1.is_null(0));
        assert!(bitmap1.is_valid(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_remove_at_len_minus_one() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(true);
        bitmap.remove(2);
        assert_eq!(bitmap.len(), 2);
        assert!(bitmap.is_null(0));
        assert!(bitmap.is_valid(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_count_with_alternating_full_words() {
        let mut bitmap = NullBitmap::new();
        for i in 0..128 {
            bitmap.push(i < 64);
        }
        assert_eq!(bitmap.count_null(), 64);
        assert_eq!(bitmap.count_valid(), 64);
    }
}
