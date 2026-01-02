use std::collections::HashMap;

use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use loom::sync::{Arc, Mutex, RwLock};
use loom::thread;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int64(i64),
    String(String),
    Null,
}

#[derive(Debug)]
pub struct SyncCatalogModel {
    tables: Arc<RwLock<HashMap<String, TableModel>>>,
    transaction_active: Arc<AtomicBool>,
    operation_count: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct TableModel {
    rows: Vec<Vec<Value>>,
    version: u64,
}

impl TableModel {
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            version: 0,
        }
    }

    pub fn insert(&mut self, row: Vec<Value>) {
        self.rows.push(row);
        self.version += 1;
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn version(&self) -> u64 {
        self.version
    }
}

impl Default for TableModel {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncCatalogModel {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            transaction_active: Arc::new(AtomicBool::new(false)),
            operation_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn create_table(&self, name: &str) {
        let mut tables = self.tables.write().unwrap();
        tables.insert(name.to_uppercase(), TableModel::new());
        self.operation_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn insert(&self, table_name: &str, row: Vec<Value>) -> bool {
        let mut tables = self.tables.write().unwrap();
        if let Some(table) = tables.get_mut(&table_name.to_uppercase()) {
            table.insert(row);
            self.operation_count.fetch_add(1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    pub fn select(&self, table_name: &str) -> Option<Vec<Vec<Value>>> {
        let tables = self.tables.read().unwrap();
        tables
            .get(&table_name.to_uppercase())
            .map(|t| t.rows.clone())
    }

    pub fn table_row_count(&self, table_name: &str) -> Option<usize> {
        let tables = self.tables.read().unwrap();
        tables
            .get(&table_name.to_uppercase())
            .map(|t| t.row_count())
    }

    pub fn table_version(&self, table_name: &str) -> Option<u64> {
        let tables = self.tables.read().unwrap();
        tables.get(&table_name.to_uppercase()).map(|t| t.version())
    }

    pub fn begin_transaction(&self) {
        self.transaction_active.store(true, Ordering::SeqCst);
    }

    pub fn commit(&self) {
        self.transaction_active.store(false, Ordering::SeqCst);
    }

    pub fn rollback(&self) {
        self.transaction_active.store(false, Ordering::SeqCst);
    }

    pub fn is_transaction_active(&self) -> bool {
        self.transaction_active.load(Ordering::SeqCst)
    }

    pub fn operation_count(&self) -> u64 {
        self.operation_count.load(Ordering::SeqCst)
    }
}

impl Default for SyncCatalogModel {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SyncCatalogModel {
    fn clone(&self) -> Self {
        Self {
            tables: Arc::clone(&self.tables),
            transaction_active: Arc::clone(&self.transaction_active),
            operation_count: Arc::clone(&self.operation_count),
        }
    }
}

#[derive(Debug)]
pub struct SyncTableLockModel {
    read_count: Arc<AtomicU64>,
    write_locked: Arc<AtomicBool>,
    data: Arc<Mutex<Vec<Vec<Value>>>>,
}

impl SyncTableLockModel {
    pub fn new() -> Self {
        Self {
            read_count: Arc::new(AtomicU64::new(0)),
            write_locked: Arc::new(AtomicBool::new(false)),
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn try_read_lock(&self) -> bool {
        if self.write_locked.load(Ordering::SeqCst) {
            return false;
        }
        self.read_count.fetch_add(1, Ordering::SeqCst);
        true
    }

    pub fn read_unlock(&self) {
        self.read_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn try_write_lock(&self) -> bool {
        if self.read_count.load(Ordering::SeqCst) > 0 {
            return false;
        }
        self.write_locked
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn write_unlock(&self) {
        self.write_locked.store(false, Ordering::SeqCst);
    }

    pub fn insert(&self, row: Vec<Value>) -> bool {
        if !self.write_locked.load(Ordering::SeqCst) {
            return false;
        }
        let mut data = self.data.lock().unwrap();
        data.push(row);
        true
    }

    pub fn read_all(&self) -> Vec<Vec<Value>> {
        let data = self.data.lock().unwrap();
        data.clone()
    }

    pub fn row_count(&self) -> usize {
        let data = self.data.lock().unwrap();
        data.len()
    }
}

impl Default for SyncTableLockModel {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SyncTableLockModel {
    fn clone(&self) -> Self {
        Self {
            read_count: Arc::clone(&self.read_count),
            write_locked: Arc::clone(&self.write_locked),
            data: Arc::clone(&self.data),
        }
    }
}

#[test]
fn test_concurrent_insert_model() {
    loom::model(|| {
        let catalog = SyncCatalogModel::new();
        catalog.create_table("test");

        let catalog1 = catalog.clone();
        let catalog2 = catalog.clone();

        let t1 = thread::spawn(move || {
            catalog1.insert(
                "test",
                vec![Value::Int64(1), Value::String("a".to_string())],
            );
        });

        let t2 = thread::spawn(move || {
            catalog2.insert(
                "test",
                vec![Value::Int64(2), Value::String("b".to_string())],
            );
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let count = catalog.table_row_count("test").unwrap();
        assert_eq!(count, 2);
    });
}

#[test]
fn test_concurrent_read_write_model() {
    loom::model(|| {
        let catalog = SyncCatalogModel::new();
        catalog.create_table("test");
        catalog.insert("test", vec![Value::Int64(0)]);

        let catalog1 = catalog.clone();
        let catalog2 = catalog.clone();

        let t1 = thread::spawn(move || {
            catalog1.insert("test", vec![Value::Int64(1)]);
        });

        let t2 = thread::spawn(move || {
            let _ = catalog2.select("test");
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let count = catalog.table_row_count("test").unwrap();
        assert!(count >= 1 && count <= 2);
    });
}

#[test]
fn test_table_lock_read_write_exclusion() {
    loom::model(|| {
        let table = SyncTableLockModel::new();

        let table1 = table.clone();
        let table2 = table.clone();

        let t1 = thread::spawn(move || {
            if table1.try_read_lock() {
                let _data = table1.read_all();
                table1.read_unlock();
            }
        });

        let t2 = thread::spawn(move || {
            if table2.try_write_lock() {
                table2.insert(vec![Value::Int64(1)]);
                table2.write_unlock();
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

#[test]
fn test_multiple_readers_model() {
    loom::model(|| {
        let table = SyncTableLockModel::new();

        let table1 = table.clone();
        let table2 = table.clone();

        let t1 = thread::spawn(move || {
            if table1.try_read_lock() {
                let _data = table1.read_all();
                table1.read_unlock();
            }
        });

        let t2 = thread::spawn(move || {
            if table2.try_read_lock() {
                let _data = table2.read_all();
                table2.read_unlock();
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

#[test]
fn test_transaction_state_model() {
    loom::model(|| {
        let catalog = SyncCatalogModel::new();

        let catalog1 = catalog.clone();
        let catalog2 = catalog.clone();

        let t1 = thread::spawn(move || {
            catalog1.begin_transaction();
            catalog1.commit();
        });

        let t2 = thread::spawn(move || {
            let _ = catalog2.is_transaction_active();
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

#[test]
fn test_version_increment_model() {
    loom::model(|| {
        let catalog = SyncCatalogModel::new();
        catalog.create_table("test");

        let catalog1 = catalog.clone();
        let catalog2 = catalog.clone();

        let t1 = thread::spawn(move || {
            catalog1.insert("test", vec![Value::Int64(1)]);
        });

        let t2 = thread::spawn(move || {
            catalog2.insert("test", vec![Value::Int64(2)]);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let version = catalog.table_version("test").unwrap();
        assert_eq!(version, 2);
    });
}
