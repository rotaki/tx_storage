use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use crate::prelude::*;

enum Storage {
    HashMap(HashMap<Vec<u8>, Vec<u8>>),
    BTreeMap(BTreeMap<Vec<u8>, Vec<u8>>),
}

impl Storage {
    fn new(c_type: ContainerType) -> Self {
        match c_type {
            ContainerType::Hash => Storage::HashMap(HashMap::new()),
            ContainerType::BTree => Storage::BTreeMap(BTreeMap::new()),
        }
    }

    fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) -> Result<(), Status> {
        match self {
            Storage::HashMap(h) => match h.entry(key) {
                std::collections::hash_map::Entry::Occupied(_) => Err(Status::KeyExists),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(val);
                    Ok(())
                }
            },
            Storage::BTreeMap(b) => match b.entry(key) {
                std::collections::btree_map::Entry::Occupied(_) => Err(Status::KeyExists),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(val);
                    Ok(())
                }
            },
        }
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, Status> {
        match self {
            Storage::HashMap(h) => match h.get(key) {
                Some(val) => Ok(val.clone()),
                None => Err(Status::KeyNotFound),
            },
            Storage::BTreeMap(b) => match b.get(key) {
                Some(val) => Ok(val.clone()),
                None => Err(Status::KeyNotFound),
            },
        }
    }

    fn update(&mut self, key: &[u8], val: Vec<u8>) -> Result<(), Status> {
        match self {
            Storage::HashMap(h) => match h.get_mut(key) {
                Some(v) => {
                    *v = val;
                    Ok(())
                }
                None => Err(Status::KeyNotFound),
            },
            Storage::BTreeMap(b) => match b.get_mut(key) {
                Some(v) => {
                    *v = val;
                    Ok(())
                }
                None => Err(Status::KeyNotFound),
            },
        }
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), Status> {
        match self {
            Storage::HashMap(h) => match h.remove(key) {
                Some(_) => Ok(()),
                None => Err(Status::KeyNotFound),
            },
            Storage::BTreeMap(b) => match b.remove(key) {
                Some(_) => Ok(()),
                None => Err(Status::KeyNotFound),
            },
        }
    }
}

pub struct InMemIterator {
    inner: Mutex<Vec<(Vec<u8>, Vec<u8>)>>, // Copy all the data from the storage
}

impl InMemIterator {
    fn new(storage: &Storage) -> Self {
        let mut inner = Vec::new();
        match storage {
            Storage::HashMap(h) => {
                for (k, v) in h.iter() {
                    inner.push((k.clone(), v.clone()));
                }
            }
            Storage::BTreeMap(b) => {
                for (k, v) in b.iter().rev() {
                    inner.push((k.clone(), v.clone()));
                }
            }
        }
        InMemIterator {
            inner: Mutex::new(inner),
        }
    }

    fn next(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        // keep popping the last element
        let mut inner = self.inner.lock().unwrap();
        inner.pop()
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

pub struct InMemDummyTxnHandle {
    db_id: DatabaseId,
}

impl InMemDummyTxnHandle {
    pub fn new(db_id: DatabaseId) -> Self {
        InMemDummyTxnHandle { db_id }
    }

    pub fn db_id(&self) -> DatabaseId {
        self.db_id
    }
}

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
    fn begin_txn(
        &self,
        db_id: &DatabaseId,
        options: TxnOptions,
    ) -> Result<Self::TxnHandle, Status> {
        Ok(InMemDummyTxnHandle::new(*db_id))
    }

    // Commit a transaction
    fn commit_txn(&self, txn: &Self::TxnHandle, async_commit: bool) -> Result<(), Status> {
        Ok(())
    }

    // Abort a transaction
    fn abort_txn(&self, txn: &Self::TxnHandle) -> Result<(), Status> {
        Ok(())
    }

    // Wait for a transaction to finish
    fn wait_for_txn(&self, txn: &Self::TxnHandle) -> Result<(), Status> {
        Ok(())
    }

    // Drop a transaction handle
    fn drop_txn(&self, txn: Self::TxnHandle) -> Result<(), Status> {
        Ok(())
    }

    // Check if value exists
    fn check_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<bool, Status> {
        let inner = self.inner.lock().unwrap();
        match inner.dbs.get(&txn.db_id()) {
            Some(db) => match db.get(c_id) {
                Some(storage) => Ok(storage.get(key.as_ref()).is_ok()),
                None => Err(Status::ContainerNotFound),
            },
            None => Err(Status::DBNotFound),
        }
    }

    // Get value
    fn get_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<Vec<u8>, Status> {
        let inner = self.inner.lock().unwrap();
        match inner.dbs.get(&txn.db_id()) {
            Some(db) => match db.get(c_id) {
                Some(storage) => storage.get(key.as_ref()),
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
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), Status> {
        let mut inner = self.inner.lock().unwrap();
        match inner.dbs.get_mut(&txn.db_id()) {
            Some(db) => match db.get_mut(c_id) {
                Some(storage) => storage.insert(key, value),
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
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), Status> {
        let mut inner = self.inner.lock().unwrap();
        match inner.dbs.get_mut(&txn.db_id()) {
            Some(db) => match db.get_mut(c_id) {
                Some(storage) => {
                    for (k, v) in kvs {
                        storage.insert(k, v)?;
                    }
                    Ok(())
                }
                None => Err(Status::ContainerNotFound),
            },
            None => Err(Status::DBNotFound),
        }
    }

    // Update value
    fn update_value<K>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
        value: Vec<u8>,
    ) -> Result<(), Status>
    where
        K: AsRef<[u8]>,
    {
        let mut inner = self.inner.lock().unwrap();
        match inner.dbs.get_mut(&txn.db_id()) {
            Some(db) => match db.get_mut(c_id) {
                Some(storage) => storage.update(key.as_ref(), value),
                None => Err(Status::ContainerNotFound),
            },
            None => Err(Status::DBNotFound),
        }
    }

    // Delete value
    fn delete_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<(), Status> {
        let mut inner = self.inner.lock().unwrap();
        match inner.dbs.get_mut(&txn.db_id()) {
            Some(db) => match db.get_mut(c_id) {
                Some(storage) => storage.remove(key.as_ref()),
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
        match inner.dbs.get(&txn.db_id()) {
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
