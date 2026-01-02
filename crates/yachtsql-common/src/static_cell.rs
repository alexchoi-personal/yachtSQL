use std::ops::{Deref, DerefMut};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct StaticCell<T>(RwLock<T>);

impl<T> StaticCell<T> {
    pub const fn new(value: T) -> Self {
        Self(RwLock::new(value))
    }

    pub fn with<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        f(&*self.0.read().unwrap())
    }

    pub fn with_mut<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        f(&mut *self.0.write().unwrap())
    }

    pub fn set(&self, value: T) {
        *self.0.write().unwrap() = value;
    }
}

impl<T: Copy> StaticCell<T> {
    pub fn get_copy(&self) -> T {
        *self.0.read().unwrap()
    }
}

impl<T: Default> StaticCell<T> {
    pub fn take(&self) -> T {
        std::mem::take(&mut *self.0.write().unwrap())
    }
}

pub struct StaticRefCell<T>(RwLock<T>);

impl<T> StaticRefCell<T> {
    pub const fn new(value: T) -> Self {
        Self(RwLock::new(value))
    }

    pub fn with<R, F: FnOnce(&T) -> R>(&self, f: F) -> R {
        f(&*self.0.read().unwrap())
    }

    pub fn with_mut<R, F: FnOnce(&mut T) -> R>(&self, f: F) -> R {
        f(&mut *self.0.write().unwrap())
    }

    pub fn set(&self, value: T) {
        *self.0.write().unwrap() = value;
    }
}

impl<T: Clone> StaticRefCell<T> {
    pub fn clone_inner(&self) -> T {
        self.0.read().unwrap().clone()
    }
}

pub struct StaticRef<'a, T>(RwLockReadGuard<'a, T>);

impl<T> Deref for StaticRef<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

pub struct StaticRefMut<'a, T>(RwLockWriteGuard<'a, T>);

impl<T> Deref for StaticRefMut<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for StaticRefMut<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> StaticRefCell<T> {
    pub fn borrow(&self) -> StaticRef<'_, T> {
        StaticRef(self.0.read().unwrap())
    }

    pub fn borrow_mut(&self) -> StaticRefMut<'_, T> {
        StaticRefMut(self.0.write().unwrap())
    }
}

impl<T: Default> StaticRefCell<T> {
    pub fn take(&self) -> T {
        std::mem::take(&mut *self.0.write().unwrap())
    }
}

pub struct LazyStaticRefCell<T, F = fn() -> T> {
    cell: RwLock<Option<T>>,
    init: F,
}

impl<T, F: Fn() -> T> LazyStaticRefCell<T, F> {
    pub const fn new(init: F) -> Self {
        Self {
            cell: RwLock::new(None),
            init,
        }
    }

    fn get_or_init<R, G: FnOnce(&T) -> R>(&self, f: G) -> R {
        {
            let guard = self.cell.read().unwrap();
            if let Some(ref val) = *guard {
                return f(val);
            }
        }
        {
            let mut guard = self.cell.write().unwrap();
            if guard.is_none() {
                *guard = Some((self.init)());
            }
            f(guard.as_ref().unwrap())
        }
    }

    fn get_or_init_mut<R, G: FnOnce(&mut T) -> R>(&self, f: G) -> R {
        let mut guard = self.cell.write().unwrap();
        if guard.is_none() {
            *guard = Some((self.init)());
        }
        f(guard.as_mut().unwrap())
    }

    pub fn with<R, G: FnOnce(&T) -> R>(&self, f: G) -> R {
        self.get_or_init(f)
    }

    pub fn with_mut<R, G: FnOnce(&mut T) -> R>(&self, f: G) -> R {
        self.get_or_init_mut(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_cell_new() {
        let cell: StaticCell<i32> = StaticCell::new(42);
        cell.with(|v| assert_eq!(*v, 42));
    }

    #[test]
    fn test_static_cell_with() {
        let cell: StaticCell<i32> = StaticCell::new(42);
        let result = cell.with(|v| *v + 1);
        assert_eq!(result, 43);
    }

    #[test]
    fn test_static_cell_with_mut() {
        let cell: StaticCell<i32> = StaticCell::new(42);
        cell.with_mut(|v| *v = 100);
        cell.with(|v| assert_eq!(*v, 100));
    }

    #[test]
    fn test_static_cell_set() {
        let cell: StaticCell<i32> = StaticCell::new(42);
        cell.set(100);
        cell.with(|v| assert_eq!(*v, 100));
    }

    #[test]
    fn test_static_cell_get_copy() {
        let cell: StaticCell<i32> = StaticCell::new(42);
        assert_eq!(cell.get_copy(), 42);
        cell.set(100);
        assert_eq!(cell.get_copy(), 100);
    }

    #[test]
    fn test_static_cell_take() {
        let cell: StaticCell<i32> = StaticCell::new(42);
        let taken = cell.take();
        assert_eq!(taken, 42);
        assert_eq!(cell.get_copy(), 0);
    }

    #[test]
    fn test_static_ref_cell_new() {
        let cell: StaticRefCell<String> = StaticRefCell::new("hello".to_string());
        cell.with(|v| assert_eq!(v, "hello"));
    }

    #[test]
    fn test_static_ref_cell_with() {
        let cell: StaticRefCell<String> = StaticRefCell::new("hello".to_string());
        let result = cell.with(|v| v.len());
        assert_eq!(result, 5);
    }

    #[test]
    fn test_static_ref_cell_with_mut() {
        let cell: StaticRefCell<String> = StaticRefCell::new("hello".to_string());
        cell.with_mut(|v| v.push_str(" world"));
        cell.with(|v| assert_eq!(v, "hello world"));
    }

    #[test]
    fn test_static_ref_cell_set() {
        let cell: StaticRefCell<String> = StaticRefCell::new("hello".to_string());
        cell.set("world".to_string());
        cell.with(|v| assert_eq!(v, "world"));
    }

    #[test]
    fn test_static_ref_cell_clone_inner() {
        let cell: StaticRefCell<String> = StaticRefCell::new("hello".to_string());
        let cloned = cell.clone_inner();
        assert_eq!(cloned, "hello");
    }

    #[test]
    fn test_static_ref_cell_borrow() {
        let cell: StaticRefCell<String> = StaticRefCell::new("hello".to_string());
        {
            let borrowed = cell.borrow();
            assert_eq!(&*borrowed, "hello");
        }
    }

    #[test]
    fn test_static_ref_cell_borrow_mut() {
        let cell: StaticRefCell<String> = StaticRefCell::new("hello".to_string());
        {
            let mut borrowed = cell.borrow_mut();
            borrowed.push_str(" world");
        }
        cell.with(|v| assert_eq!(v, "hello world"));
    }

    #[test]
    fn test_static_ref_cell_take() {
        let cell: StaticRefCell<String> = StaticRefCell::new("hello".to_string());
        let taken = cell.take();
        assert_eq!(taken, "hello");
        cell.with(|v| assert_eq!(v, ""));
    }

    #[test]
    fn test_static_ref_deref() {
        let cell: StaticRefCell<i32> = StaticRefCell::new(42);
        let borrowed = cell.borrow();
        assert_eq!(*borrowed, 42);
    }

    #[test]
    fn test_static_ref_mut_deref() {
        let cell: StaticRefCell<i32> = StaticRefCell::new(42);
        let borrowed = cell.borrow_mut();
        assert_eq!(*borrowed, 42);
    }

    #[test]
    fn test_static_ref_mut_deref_mut() {
        let cell: StaticRefCell<i32> = StaticRefCell::new(42);
        {
            let mut borrowed = cell.borrow_mut();
            *borrowed = 100;
        }
        assert_eq!(cell.clone_inner(), 100);
    }

    #[test]
    fn test_lazy_static_ref_cell_new() {
        let cell: LazyStaticRefCell<i32, fn() -> i32> = LazyStaticRefCell::new(|| 42);
        cell.with(|v| assert_eq!(*v, 42));
    }

    #[test]
    fn test_lazy_static_ref_cell_with() {
        let cell: LazyStaticRefCell<i32, fn() -> i32> = LazyStaticRefCell::new(|| 42);
        let result = cell.with(|v| *v + 1);
        assert_eq!(result, 43);
    }

    #[test]
    fn test_lazy_static_ref_cell_with_mut() {
        let cell: LazyStaticRefCell<i32, fn() -> i32> = LazyStaticRefCell::new(|| 42);
        cell.with_mut(|v| *v = 100);
        cell.with(|v| assert_eq!(*v, 100));
    }

    #[test]
    fn test_lazy_static_ref_cell_lazy_init() {
        use std::sync::atomic::{AtomicBool, Ordering};

        static INITIALIZED: AtomicBool = AtomicBool::new(false);

        fn init() -> i32 {
            INITIALIZED.store(true, Ordering::SeqCst);
            42
        }

        let cell: LazyStaticRefCell<i32, fn() -> i32> = LazyStaticRefCell::new(init);

        INITIALIZED.store(false, Ordering::SeqCst);
        assert!(!INITIALIZED.load(Ordering::SeqCst));

        cell.with(|v| assert_eq!(*v, 42));
        assert!(INITIALIZED.load(Ordering::SeqCst));
    }

    #[test]
    fn test_lazy_static_ref_cell_init_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static INIT_COUNT: AtomicUsize = AtomicUsize::new(0);

        fn init() -> i32 {
            INIT_COUNT.fetch_add(1, Ordering::SeqCst);
            42
        }

        let cell: LazyStaticRefCell<i32, fn() -> i32> = LazyStaticRefCell::new(init);

        INIT_COUNT.store(0, Ordering::SeqCst);

        cell.with(|_| {});
        cell.with(|_| {});
        cell.with(|_| {});

        assert_eq!(INIT_COUNT.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_lazy_static_ref_cell_with_mut_init() {
        let cell: LazyStaticRefCell<i32, fn() -> i32> = LazyStaticRefCell::new(|| 42);
        cell.with_mut(|v| {
            assert_eq!(*v, 42);
            *v = 100;
        });
        cell.with(|v| assert_eq!(*v, 100));
    }
}
