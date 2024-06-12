mod txn_storage_trait;

mod inmem;

pub use crate::inmem::{InMemDummyTxnHandle, InMemIterator, InMemStorage};
pub use txn_storage_trait::{
    ContainerId, ContainerOptions, ContainerType, DBOptions, DatabaseId, ScanOptions, Status,
    TxnOptions, TxnStorageTrait,
};

pub mod prelude {
    pub use crate::{
        ContainerId, ContainerOptions, ContainerType, DBOptions, DatabaseId, InMemDummyTxnHandle,
        InMemIterator, InMemStorage, ScanOptions, Status, TxnOptions, TxnStorageTrait,
    };
}

#[cfg(test)]
mod tests {
    #[cfg(test)]
    use super::*;
    use std::{sync::Arc, thread};

    fn get_in_mem_storage() -> Arc<InMemStorage> {
        Arc::new(InMemStorage::new())
    }

    #[test]
    fn test_open_and_delete_db() {
        let storage = get_in_mem_storage();
        let db_options = DBOptions::new("test_db");
        let db_id = storage.open_db(db_options).unwrap();
        assert!(storage.delete_db(&db_id).is_ok());
    }

    fn setup_table<'a, T: TxnStorageTrait<'a>>(
        storage: impl AsRef<T>,
        c_type: ContainerType,
    ) -> (DatabaseId, ContainerId) {
        let storage = storage.as_ref();
        let db_options = DBOptions::new("test_db");
        let db_id = storage.open_db(db_options).unwrap();
        let container_options = ContainerOptions::new("test_container", c_type);
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let c_id = storage
            .create_container(&txn, &db_id, container_options)
            .unwrap();
        storage.commit_txn(&txn, false).unwrap();
        (db_id, c_id)
    }

    #[test]
    fn test_create_and_delete_container() {
        let storage = get_in_mem_storage();
        let (db_id, c_id) = setup_table(&storage, ContainerType::Hash);
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        assert!(storage.delete_container(&txn, &db_id, &c_id).is_ok());
        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_insert_and_get_value() {
        let storage = get_in_mem_storage();
        let (db_id, c_id) = setup_table(&storage, ContainerType::Hash);
        let key = vec![0];
        let value = vec![1, 2, 3, 4];
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        storage
            .insert_value(&txn, &c_id, key.clone(), value.clone())
            .unwrap();
        let retrieved_value = storage.get_value(&txn, &c_id, &key).unwrap();
        assert_eq!(value, retrieved_value);
        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_update_and_remove_value() {
        let storage = get_in_mem_storage();
        let (db_id, c_id) = setup_table(&storage, ContainerType::Hash);
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let key = vec![0];
        let value = vec![1, 2, 3, 4];
        storage
            .insert_value(&txn, &c_id, key.clone(), value.clone())
            .unwrap();
        let new_value = vec![4, 3, 2, 1];
        storage
            .update_value(&txn, &c_id, &key, new_value.clone())
            .unwrap();
        let updated_value = storage.get_value(&txn, &c_id, &key).unwrap();
        assert_eq!(new_value, updated_value);

        assert!(storage.delete_value(&txn, &c_id, &key).is_ok());
        assert!(matches!(
            storage.get_value(&txn, &c_id, &key),
            Err(Status::KeyNotFound)
        ));
        storage.commit_txn(&txn, false).unwrap();
    }

    #[test]
    fn test_scan_range() {
        let storage = get_in_mem_storage();
        let (db_id, c_id) = setup_table(&storage, ContainerType::BTree);

        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        // Insert some values
        for i in 0..4 {
            let key = vec![i];
            let value = vec![i as u8; 4];
            storage.insert_value(&txn, &c_id, key, value).unwrap();
        }
        let iter_handle = storage.scan_range(&txn, &c_id, ScanOptions::new()).unwrap();
        let mut count = 0;
        while let Ok(Some((key, val))) = storage.iter_next(&iter_handle) {
            assert_eq!(key, vec![count]);
            assert_eq!(val, vec![count as u8; 4]);
            count += 1;
        }
        assert_eq!(count, 4);
        storage.commit_txn(&txn, false).unwrap();
    }


    #[test]
    fn test_concurrent_insert() {
        let storage = get_in_mem_storage();
        let (db_id, c_id) = setup_table(&storage, ContainerType::BTree);
        let num_threads = 4;
        let num_keys_per_thread = 10000;
        let mut threads = Vec::with_capacity(num_threads);
        for i in 0..num_threads {
            let storage = storage.clone();
            let db_id = db_id.clone();
            let c_id = c_id.clone();
            threads.push(thread::spawn(move || {
                for k in 0..num_keys_per_thread {
                    let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
                    let key: usize = i*num_keys_per_thread + k;
                    let key = key.to_be_bytes().to_vec();
                    let value = key.clone();
                    storage
                        .insert_value(&txn, &c_id, key.clone(), value.clone())
                        .unwrap();
                    storage.commit_txn(&txn, false).unwrap();
                }
            }));
        };
        for t in threads {
            t.join().unwrap();
        }

        // Check if all values are inserted
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let iter_handle = storage.scan_range(&txn, &c_id, ScanOptions::new()).unwrap();
        let mut count = 0;
        while let Ok(Some((key, val))) = storage.iter_next(&iter_handle) {
            let key = usize::from_be_bytes(key.as_slice().try_into().unwrap());
            assert_eq!(key, count);
            assert_eq!(val, key.to_be_bytes().to_vec());
            count += 1;
        }
        assert_eq!(count, num_threads*num_keys_per_thread);
    }

    #[test]
    fn test_concurrent_insert_and_container_ops() {
        // Create two containers.
        // Keep inserting into the second container. Remove the first container. Create a new container at the same time.
        let storage = get_in_mem_storage();
        let db_id = storage.open_db(DBOptions::new("test_db")).unwrap();
        let c_id1 = storage
            .create_container(
                &storage.begin_txn(&db_id, TxnOptions::default()).unwrap(),
                &db_id,
                ContainerOptions::new("test_container1", ContainerType::BTree),
            )
            .unwrap();
        let c_id2 = storage
            .create_container(
                &storage.begin_txn(&db_id, TxnOptions::default()).unwrap(),
                &db_id,
                ContainerOptions::new("test_container2", ContainerType::BTree),
            )
            .unwrap();

        let num_threads = 4; // Threads to insert into the second container
        let num_keys_per_thread = 10000;
        let mut threads = Vec::with_capacity(num_threads);
        for i in 0..num_threads {
            let storage = storage.clone();
            let db_id = db_id.clone();
            let c_id2 = c_id2.clone();
            threads.push(thread::spawn(move || {
                for k in 0..num_keys_per_thread {
                    let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
                    let key: usize = i*num_keys_per_thread + k;
                    let key = key.to_be_bytes().to_vec();
                    let value = key.clone();
                    storage
                        .insert_value(&txn, &c_id2, key.clone(), value.clone())
                        .unwrap();
                    storage.commit_txn(&txn, false).unwrap();
                }
            }));
        };
        // Create a new container and delete the first container
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let _c_id3 = storage
            .create_container(
                &txn,
                &db_id,
                ContainerOptions::new("test_container3", ContainerType::BTree),
            )
            .unwrap();
        storage.commit_txn(&txn, false).unwrap();
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        storage.delete_container(&txn, &db_id, &c_id1).unwrap();
        storage.commit_txn(&txn, false).unwrap();
        for t in threads {
            t.join().unwrap();
        }

        // Check if all values are inserted
        let txn = storage.begin_txn(&db_id, TxnOptions::default()).unwrap();
        let iter_handle = storage.scan_range(&txn, &c_id2, ScanOptions::new()).unwrap();
        let mut count = 0;
        while let Ok(Some((key, val))) = storage.iter_next(&iter_handle) {
            let key = usize::from_be_bytes(key.as_slice().try_into().unwrap());
            assert_eq!(key, count);
            assert_eq!(val, key.to_be_bytes().to_vec());
            count += 1;
        }
        assert_eq!(count, num_threads*num_keys_per_thread);
        // println!("list_containers: {:?}", storage.list_containers(&txn, &db_id).unwrap());
    }
}
