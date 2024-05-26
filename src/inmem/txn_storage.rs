use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use crate::prelude::*;

enum Storage {
    Vec(Vec<Vec<u8>>),
    HashMap(fn(&[u8]) -> &[u8], HashMap<Vec<u8>, Vec<u8>>),
    BTreeMap(fn(&[u8]) -> &[u8], BTreeMap<Vec<u8>, Vec<u8>>),
}

impl Storage {
    fn new(c_type: ContainerType) -> Self {
        match c_type {
            ContainerType::Vec => Storage::Vec(Vec::new()),
            ContainerType::Hash(f) => Storage::HashMap(f, HashMap::new()),
            ContainerType::BTree(f) => Storage::BTreeMap(f, BTreeMap::new()),
        }
    }

    fn insert(&mut self, val: Vec<u8>) -> Result<Vec<u8>, Status> {
        match self {
            Storage::Vec(v) => {
                let len = v.len();
                v.push(val);
                Ok(len.to_be_bytes().to_vec())
            }
            Storage::HashMap(f, h) => {
                let key = f(&val).to_vec();
                h.insert(key.clone(), val);
                Ok(key)
            }
            Storage::BTreeMap(f, b) => {
                let key = f(&val).to_vec();
                b.insert(key.clone(), val);
                Ok(key)
            }
        }
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Status> {
        match self {
            Storage::Vec(v) => {
                let idx = u64::from_be_bytes(key.try_into().unwrap()) as usize;
                match v.get(idx) {
                    Some(val) => Ok(val.clone()),
                    None => Err(Status::KeyNotFound),
                }
            }
            Storage::HashMap(_, h) => match h.get(key) {
                Some(val) => Ok(val.clone()),
                None => Err(Status::KeyNotFound),
            },
            Storage::BTreeMap(_, b) => match b.get(key) {
                Some(val) => Ok(val.clone()),
                None => Err(Status::KeyNotFound),
            },
        }
    }

    fn update(&mut self, key: &[u8], val: Vec<u8>) -> Result<Vec<u8>, Status> {
        match self {
            Storage::Vec(v) => {
                let idx = u64::from_be_bytes(key.try_into().unwrap()) as usize;
                if idx < v.len() {
                    v[idx] = val;
                    Ok(key.to_vec())
                } else {
                    Err(Status::KeyNotFound)
                }
            }
            Storage::HashMap(f, h) => {
                assert_eq!(f(&val), key);
                match h.insert(key.to_vec(), val) {
                    Some(old_val) => Ok(old_val),
                    None => Err(Status::KeyNotFound),
                }
            }
            Storage::BTreeMap(f, b) => {
                assert_eq!(f(&val), key);
                match b.insert(key.to_vec(), val) {
                    Some(old_val) => Ok(old_val),
                    None => Err(Status::KeyNotFound),
                }
            }
        }
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), Status> {
        match self {
            Storage::Vec(v) => {
                let idx = u64::from_be_bytes(key.try_into().unwrap()) as usize;
                if idx < v.len() {
                    v.remove(idx);
                    Ok(())
                } else {
                    Err(Status::KeyNotFound)
                }
            }
            Storage::HashMap(_, h) => match h.remove(key) {
                Some(_) => Ok(()),
                None => Err(Status::KeyNotFound),
            },
            Storage::BTreeMap(_, b) => match b.remove(key) {
                Some(_) => Ok(()),
                None => Err(Status::KeyNotFound),
            },
        }
    }
}

pub struct InMemIterator {
    inner: Vec<(Vec<u8>, Vec<u8>)>, // Copy all the data from the storage
    idx: AtomicUsize,
}

impl InMemIterator {
    fn new(storage: &Storage) -> Self {
        let mut inner = Vec::new();
        match storage {
            Storage::Vec(v) => {
                for (i, val) in v.iter().enumerate() {
                    inner.push((i.to_be_bytes().to_vec(), val.clone()));
                }
            }
            Storage::HashMap(_, h) => {
                for (key, val) in h.iter() {
                    inner.push((key.clone(), val.clone()));
                }
            }
            Storage::BTreeMap(_, b) => {
                for (key, val) in b.iter() {
                    inner.push((key.clone(), val.clone()));
                }
            }
        }
        InMemIterator {
            inner,
            idx: AtomicUsize::new(0),
        }
    }

    fn next(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let idx = self.idx.fetch_add(1, Ordering::AcqRel);
        if idx < self.inner.len() {
            Some(self.inner[idx].clone())
        } else {
            None
        }
    }
}

struct InMemStorageInner {
    dbs: HashMap<DatabaseId, HashMap<ContainerId, Storage>>,
}

pub struct InMemStorage {
    inner: Mutex<InMemStorageInner>,
}

impl Default for InMemStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemStorage {
    pub fn new() -> Self {
        InMemStorage {
            inner: Mutex::new(InMemStorageInner {
                dbs: HashMap::new(),
            }),
        }
    }
}

pub struct InMemDummyTxnHandle;

impl TxnStorageTrait for InMemStorage {
    type TxnHandle = InMemDummyTxnHandle;
    type IteratorHandle = InMemIterator;

    // Open connection with the db
    fn open_db(&self, _options: DBOptions) -> Result<DatabaseId, Status> {
        let mut inner = self.inner.lock().unwrap();
        let db_id = inner.dbs.len() as DatabaseId;
        match inner.dbs.insert(db_id, HashMap::new()) {
            Some(_) => Err(Status::DBExists),
            None => Ok(db_id),
        }
    }

    // Close connection with the db
    fn close_db(&self, _db_id: &DatabaseId) -> Result<(), Status> {
        // Do nothing
        Ok(())
    }

    // Delete the db
    fn delete_db(&self, db_id: &DatabaseId) -> Result<(), Status> {
        let mut inner = self.inner.lock().unwrap();
        match inner.dbs.remove(db_id) {
            Some(_) => Ok(()),
            None => Err(Status::DBNotFound),
        }
    }

    // Create a container in the db
    fn create_container(
        &self,
        _txn: &Self::TxnHandle,
        db_id: &DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, Status> {
        let mut inner = self.inner.lock().unwrap();
        let c_id = match inner.dbs.get_mut(db_id) {
            Some(db) => {
                let c_id = db.len() as ContainerId;
                let storage = Storage::new(options.get_type());
                db.insert(c_id, storage);
                c_id
            }
            None => return Err(Status::DBNotFound),
        };
        Ok(c_id)
    }

    // Delete a container from the db
    fn delete_container(
        &self,
        _txn: &Self::TxnHandle,
        db_id: &DatabaseId,
        c_id: &ContainerId,
    ) -> Result<(), Status> {
        let mut inner = self.inner.lock().unwrap();
        match inner.dbs.get_mut(db_id) {
            Some(db) => {
                db.remove(c_id);
                Ok(())
            }
            None => Err(Status::DBNotFound),
        }
    }

    // List all container names in the db
    fn list_containers(
        &self,
        txn: &Self::TxnHandle,
        db_id: &DatabaseId,
    ) -> Result<HashSet<ContainerId>, Status> {
        let inner = self.inner.lock().unwrap();
        match inner.dbs.get(db_id) {
            Some(db) => Ok(db.keys().cloned().collect()),
            None => Err(Status::DBNotFound),
        }
    }

    // Begin a transaction
    fn begin_tx(&self, db_id: &DatabaseId, options: TxnOptions) -> Result<Self::TxnHandle, Status> {
        Ok(InMemDummyTxnHandle)
    }

    // Commit a transaction
    fn commit_tx(&self, txn: &Self::TxnHandle, async_commit: bool) -> Result<(), Status> {
        Ok(())
    }

    // Abort a transaction
    fn abort_tx(&self, txn: &Self::TxnHandle) -> Result<(), Status> {
        Ok(())
    }

    // Wait for a transaction to finish
    fn wait_for_tx(&self, txn: &Self::TxnHandle) -> Result<(), Status> {
        Ok(())
    }

    // Drop a transaction handle
    fn drop_tx(&self, handle: Self::TxnHandle) -> Result<(), Status> {
        Ok(())
    }

    // Check if value exists
    fn check_value(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: &[u8],
    ) -> Result<bool, Status> {
        let inner = self.inner.lock().unwrap();
        match inner.dbs.get(&0) {
            Some(db) => match db.get(c_id) {
                Some(storage) => Ok(storage.get(key).is_ok()),
                None => Err(Status::ContainerNotFound),
            },
            None => Err(Status::DBNotFound),
        }
    }

    // Get value
    fn get_value(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: &[u8],
    ) -> Result<Vec<u8>, Status> {
        let inner = self.inner.lock().unwrap();
        match inner.dbs.get(&0) {
            Some(db) => match db.get(c_id) {
                Some(storage) => storage.get(key),
                None => Err(Status::ContainerNotFound),
            },
            None => Err(Status::DBNotFound),
        }
    }

    // Insert value
    fn insert_value(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        value: Vec<u8>,
    ) -> Result<Vec<u8>, Status> {
        let mut inner = self.inner.lock().unwrap();
        match inner.dbs.get_mut(&0) {
            Some(db) => match db.get_mut(c_id) {
                Some(storage) => storage.insert(value),
                None => Err(Status::ContainerNotFound),
            },
            None => Err(Status::DBNotFound),
        }
    }

    // Insert values
    fn insert_values(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        values: Vec<Vec<u8>>,
    ) -> Result<Vec<Vec<u8>>, Status> {
        let mut inner = self.inner.lock().unwrap();
        match inner.dbs.get_mut(&0) {
            Some(db) => match db.get_mut(c_id) {
                Some(storage) => {
                    let mut keys = Vec::new();
                    for val in values {
                        keys.push(storage.insert(val)?);
                    }
                    Ok(keys)
                }
                None => Err(Status::ContainerNotFound),
            },
            None => Err(Status::DBNotFound),
        }
    }

    // Update value
    fn update_value(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<Vec<u8>, Status> {
        let mut inner = self.inner.lock().unwrap();
        match inner.dbs.get_mut(&0) {
            Some(db) => match db.get_mut(c_id) {
                Some(storage) => storage.update(key, value),
                None => Err(Status::ContainerNotFound),
            },
            None => Err(Status::DBNotFound),
        }
    }

    // Delete value
    fn delete_value(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: &[u8],
    ) -> Result<(), Status> {
        let mut inner = self.inner.lock().unwrap();
        match inner.dbs.get_mut(&0) {
            Some(db) => match db.get_mut(c_id) {
                Some(storage) => storage.remove(key),
                None => Err(Status::ContainerNotFound),
            },
            None => Err(Status::DBNotFound),
        }
    }

    // Scan range
    fn scan_range(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        options: ScanOptions,
    ) -> Result<Self::IteratorHandle, Status> {
        let inner = self.inner.lock().unwrap();
        match inner.dbs.get(&0) {
            Some(db) => match db.get(c_id) {
                Some(storage) => Ok(InMemIterator::new(storage)),
                None => Err(Status::ContainerNotFound),
            },
            None => Err(Status::DBNotFound),
        }
    }

    // Iterate next
    fn iter_next(&self, iter: &Self::IteratorHandle) -> Result<Option<(Vec<u8>, Vec<u8>)>, Status> {
        Ok(iter.next())
    }

    // Drop an iterator handle
    fn drop_iterator_handle(&self, iter: Self::IteratorHandle) -> Result<(), Status> {
        // Do nothing
        Ok(())
    }
}
