use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub enum Status {
    // Not found
    DBNotFound,
    ContainerNotFound,
    TxNotFound,
    KeyNotFound,

    // Already exists
    DBExists,
    ContainerExists,
    KeyExists,

    // Transaction errors
    TxnConflict,

    // System errors
    SystemAbort,

    // Other errors
    Error,
}

// To String conversion
impl From<Status> for String {
    fn from(status: Status) -> String {
        match status {
            Status::DBNotFound => "DB not found".to_string(),
            Status::ContainerNotFound => "Container not found".to_string(),
            Status::TxNotFound => "Tx not found".to_string(),
            Status::KeyNotFound => "Key not found".to_string(),
            Status::DBExists => "DB already exists".to_string(),
            Status::ContainerExists => "Container already exists".to_string(),
            Status::KeyExists => "Key already exists".to_string(),
            Status::TxnConflict => "Txn conflict".to_string(),
            Status::SystemAbort => "System abort".to_string(),
            Status::Error => "Error".to_string(),
        }
    }
}

pub type DatabaseId = u16;
pub type ContainerId = u16;

pub struct DBOptions {
    name: String,
}

impl DBOptions {
    pub fn new(name: &str) -> Self {
        DBOptions {
            name: String::from(name),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone)]
pub enum ContainerType {
    Hash,
    BTree,
}

pub struct ContainerOptions {
    name: String,
    c_type: ContainerType,
}

impl ContainerOptions {
    pub fn new(name: &str, c_type: ContainerType) -> Self {
        ContainerOptions {
            name: String::from(name),
            c_type,
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn get_type(&self) -> ContainerType {
        self.c_type.clone()
    }
}

#[derive(Default)]
pub struct TxnOptions {}

#[derive(Default)]
pub struct ScanOptions {
    // currently scans all keys
}

impl ScanOptions {
    pub fn new() -> Self {
        ScanOptions::default()
    }
}

pub trait TxnStorageTrait {
    type TxnHandle;
    type IteratorHandle;

    // Open connection with the db
    fn open_db(&self, options: DBOptions) -> Result<DatabaseId, Status>;

    // Close connection with the db
    fn close_db(&self, db_id: &DatabaseId) -> Result<(), Status>;

    // Delete the db
    fn delete_db(&self, db_id: &DatabaseId) -> Result<(), Status>;

    // Create a container in the db
    fn create_container(
        &self,
        txn: &Self::TxnHandle,
        db_id: &DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, Status>;

    // Delete a container from the db
    fn delete_container(
        &self,
        txn: &Self::TxnHandle,
        db_id: &DatabaseId,
        c_id: &ContainerId,
    ) -> Result<(), Status>;

    // List all container names in the db
    fn list_containers(
        &self,
        txn: &Self::TxnHandle,
        db_id: &DatabaseId,
    ) -> Result<HashSet<ContainerId>, Status>;

    // Begin a transaction
    fn begin_txn(&self, db_id: &DatabaseId, options: TxnOptions)
        -> Result<Self::TxnHandle, Status>;

    // Commit a transaction
    fn commit_txn(&self, txn: &Self::TxnHandle, async_commit: bool) -> Result<(), Status>;

    // Abort a transaction
    fn abort_txn(&self, txn: &Self::TxnHandle) -> Result<(), Status>;

    // Wait for a transaction to finish
    fn wait_for_txn(&self, txn: &Self::TxnHandle) -> Result<(), Status>;

    // Drop a transaction handle
    fn drop_txn(&self, txn: Self::TxnHandle) -> Result<(), Status>;

    // Check if value exists
    fn check_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<bool, Status>;

    // Get value
    fn get_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<Vec<u8>, Status>;

    // Insert value
    fn insert_value(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), Status>;

    // Insert values
    fn insert_values(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), Status>;

    // Update value
    fn update_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
        value: Vec<u8>,
    ) -> Result<(), Status>;

    // Delete value
    fn delete_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<(), Status>;

    // Scan range. While iterating, the container should be alive.
    fn scan_range(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        options: ScanOptions,
    ) -> Result<Self::IteratorHandle, Status>;

    // Iterate next
    fn iter_next(&self, iter: &Self::IteratorHandle) -> Result<Option<(Vec<u8>, Vec<u8>)>, Status>;

    // Drop an iterator handle.
    fn drop_iterator_handle(&self, iter: Self::IteratorHandle) -> Result<(), Status>;
}
