use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::{Header, Page};
use common::delta_storage_trait::Value;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_dir;
use common::testutil::init;
use common::PAGE_SIZE;
use core::num;
use serde::ser::{Serialize, SerializeStruct, Serializer};
use serde::Deserialize;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::slice::SliceIndex;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    #[serde(skip)]
    containers: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>,
    containers2: Arc<RwLock<HashMap<ContainerId, PathBuf>>>,
    pub storage_path: String,
    is_temp: bool,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        let mut containers_unlock = self.containers.write().unwrap();
        // check if given container exists
        if containers_unlock.contains_key(&container_id) {
            // if heapfile path corresponding with container_id exists then extract that heapfile
            let hf = containers_unlock.get(&container_id).unwrap();
            let hf_free_space_map = HeapFile::build_free_space_map_for_heapfile(&hf);

            if hf_free_space_map.is_empty() {
                drop(containers_unlock);
                return None;
            }
            if page_id > (hf_free_space_map.len() - 1) as u16 {
                drop(containers_unlock);
                return None;
            }
            // if page_id exists, then read page from file & return the page
            return Some(HeapFile::read_page_from_file(&hf, page_id).unwrap());
        }
        drop(containers_unlock);
        None
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        let containers_unlock = self.containers.write().unwrap();
        // extract the heapfile corresponding with container_id
        if containers_unlock.get(&container_id).is_none() {
            //  println!("SM write_page: hf is none"); //WHY??
            return Ok(());
        }
        let hf = containers_unlock.get(&container_id).unwrap();
        // write page to file
        hf.write_page_to_file(page)
        //   panic!("TODO milestone hs");
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let containers_unlock = self.containers.read().unwrap();
        if containers_unlock.contains_key(&container_id) {
            let hf = containers_unlock.get(&container_id).unwrap();
            let num_pages = HeapFile::num_pages(hf);
            num_pages
        } else {
            panic!("container id does not exist in container!");
        }
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        let containers_unlock = self.containers.read().unwrap();

        if !containers_unlock.contains_key(&container_id) {
            drop(containers_unlock);
            return (0, 0);
        }

        let hf = containers_unlock.get(&container_id).unwrap();

        let write_count = hf.write_count.load(Ordering::Relaxed);
        let read_count = hf.read_count.load(Ordering::Relaxed);

        // return
        drop(containers_unlock);
        return (read_count, write_count);
        //  panic!("TODO milestone hs");
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk)
    fn new(storage_path: String) -> Self {
        // check if storage path already exists
        let storage_path_copy = storage_path.clone();
        let storage_path_copy_1 = storage_path_copy.clone();
        let storage_path_copy_new = storage_path_copy_1.clone();
        //let mut storage_path_PathBuf = PathBuf::new();
        //storage_path_PathBuf.push(storage_path.clone());

        if Path::new(&(storage_path + &"/serialized_hm.json".to_owned())).exists() {
            // get the file path for the serialized heapfile info
            let serialized_file =
                fs::File::open(storage_path_copy + &"/serialized_hm.json".to_owned())
                    .expect("error opening file");
            // open up the serialized hf for this SM directory
            // println!("I am in here now! so apparenly file also exists!");

            // deserialize the file back into a heapfile struct
            let mut containers2: HashMap<ContainerId, PathBuf> =
                serde_json::from_reader(serialized_file).expect("cannot deserialize file");
            /* for (key, val) in containers2.clone().iter() {
                println!("SM NEW CONTAINERS2: {} -> {:?}", key, val);
            }*/
            let mut containers: HashMap<ContainerId, Arc<HeapFile>> = HashMap::new();
            //    let mut containers_unlock = containers.write();

            // now recreate the 'containers' hashmap with container_id:HashFile mapping
            for (c_id, hf_path) in containers2.iter() {
                // println!("trying to insert constructed hf into containers hashmap");
                // println!("hf_file path used to recreate hf: {:?}", hf_path);
                let hf = HeapFile::new(hf_path.to_path_buf()).expect("failed to create hf");
                // println!("SM NEW hf contents: {:?}", hf.free_space_map);
                containers.insert(*c_id, Arc::new(hf));
            }
            /*for (key, val) in containers.clone().iter() {
                println!("SM NEW CONTAINERS: {} -> {:?}", key, val.hf_file_path);
            }*/

            // recreate the SM
            let reloaded_SM = StorageManager {
                containers: Arc::new(RwLock::new(containers)),
                containers2: Arc::new(RwLock::new(containers2)),
                storage_path: storage_path_copy_1,
                is_temp: false,
            };
            return reloaded_SM;
        } else {
            // if storage path does not already exist, then no need to reload anything
            // construct a new, empty SM
            let storage_path_copy_again = storage_path_copy_new.clone();
            fs::create_dir_all(storage_path_copy_again);
            // println!("I'm in new() now! creating a new SM");
            let new_SM = StorageManager {
                containers: Arc::new(RwLock::new(HashMap::new())),
                containers2: Arc::new(RwLock::new(HashMap::new())),
                storage_path: storage_path_copy_new,
                is_temp: false,
            };
            // return it
            // println!("just finished creating a new one :)");
            new_SM
            //  panic!("TODO hs")
        }
    }

    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self {
        let storage_path = gen_random_dir().to_string_lossy().to_string();
        //  println!("storage path: {:?}", storage_path);
        /*let storage_path = fs::create_dir_all("/some/dir");
        match storage_path {
            Result() => {
                let path = storage_path.to_string();
                let test_SM = StorageManager::new(path);
                // return test SM
                return test_SM;
            }
            _ => panic!("cannot make temp SM"),
        }*/
        //    let storage_path_str =
        //debug!("Making new temp storage_manager {}", storage_path);
        // use new() and storage_path to make test SM
        StorageManager::new(storage_path)
        //  panic!("TODO milestone hs");
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        // I discussed the logic for this function with Ellyn Liu...
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
        let num_pages = self.get_num_pages(container_id);
        // sift through pages in heapfile
        for i in 0..num_pages {
            if num_pages == 0 {
                continue;
            }
            // if an existing page has space for record, insert it
            //    println!("SM inserts: before getting page");
            let mut page =
                Self::get_page(&self, container_id, i, tid, Permissions::ReadOnly, false).unwrap();
            let record_id = page.add_value(&value);
            //     println!("SM inserts: after getting page");
            //page.add_value(&value);
            //    println!("SM inserts: after adding value to page");
            // check again that value was added to page
            if record_id.clone().is_none() {
                // println!("SM inserts:nothing added to page...");
                continue;
            }
            self.write_page(container_id, page, tid);
            //HeapFile::write_page_to_file(&self, page)

            return ValueId {
                container_id,
                segment_id: None,
                page_id: Some(i),
                slot_id: record_id.clone(),
            };
        }

        let mut new_page = Page::new(num_pages);
        let mut slot_id = new_page.add_value(&value);

        self.write_page(container_id, new_page, tid);

        return ValueId {
            container_id,
            segment_id: None,
            page_id: Some(num_pages),
            slot_id: slot_id.clone(),
        };
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        // create vector of value ids that I will return
        let mut vec_of_value_ids = Vec::new();
        for val in values.iter() {
            // println!("SM insert values: val:{:?}", val);
            let val_id = self.insert_value(container_id, val.to_vec(), tid);
            // println!("SM insert values: val_id:{}", val_id.slot_id.unwrap());
            vec_of_value_ids.push(val_id);
        }
        vec_of_value_ids
        //     panic!("TODO milestone hs");
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        let container_id = id.container_id;
        let page_id = id.page_id.unwrap();
        let slot_id = id.slot_id.unwrap();
        // find the heapfile associated with container_id that's stored in ValueID
        let mut containers_unlock = self.containers.write().unwrap();
        if containers_unlock.contains_key(&container_id) {
            // extract heapfile's file path from hashmap
            let hf = containers_unlock.get(&container_id).unwrap();
            // check if page with given page id exists & extract
            if page_id <= self.get_num_pages(container_id) - 1 {
                // extract page with given page_id
                let mut extracted_page = HeapFile::read_page_from_file(&hf, page_id).unwrap();
                // delete_value() on page for given slot_id if the corresponding record exists
                Page::delete_value(&mut extracted_page, slot_id);
                // reinsert back into heapfile
                return HeapFile::write_page_to_file(&hf, extracted_page);
            }
        }
        Ok(())
        //  panic!("TODO milestone hs");
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        /*
        let container_id = id.container_id;
        let page_id = id.page_id.unwrap();
        let slot_id = id.slot_id.unwrap();
        // first delete the record
        self.delete_value(id, _tid);
        // then re-insert record into heapfile
        Ok(self.insert_value(container_id, value, _tid))*/
        panic!("TODO milestone hs");
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _container_config: common::ContainerConfig,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        // unlock containers field
        let mut containers = self.containers.write().unwrap();
        let mut containers2 = self.containers2.write().unwrap();
        // construct the new heapfile
        let mut hf_file_path = PathBuf::new();
        hf_file_path.push(self.storage_path.clone());
        hf_file_path.push(container_id.clone().to_string());
        hf_file_path.with_extension("txt");
        let new_hf = HeapFile::new(hf_file_path.clone()).unwrap();
        // if container already has container id then don't add container id again
        if containers.contains_key(&container_id) {
            debug!(
                "memstore::create_container container_id: {:?} already exists",
                &container_id
            );
            // dropping lock to fix my deadlock error!!
            //println!("create_container calling drop");
            drop(containers);
            drop(containers2);
            return Ok(());
        }
        // else, insert new heapfile (value) with given container_id (key)
        containers.insert(container_id, Arc::new(new_hf));
        containers2.insert(container_id, hf_file_path);
        Ok(())
        // panic!("TODO milestone hs");
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(
            container_id,
            StorageManager::get_simple_config(),
            None,
            common::ids::StateType::BaseTable,
            None,
        )
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        /*// delete the heapfile w associated container id
        let mut containers_unlock = self.containers.write().unwrap();
        let hf = containers_unlock.get(&container_id);
        match hf {
            Some(hf) => {
                fs::remove_file(hf.hf_file_path.clone());
                containers_unlock.remove(&container_id);
            }
            _ => {
                return Err(CrustyError::IOError(
                    "Could not remove container".to_string(),
                ))
            }
        };
        Ok(())*/
        panic!("TODO milestone hs");
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        // find the heapfile associated with container_id that's stored in ValueID

        let mut containers_unlock = self.containers.write().unwrap();

        if !containers_unlock.get_key_value(&container_id).is_none() {
            //  println!("in get_iterator");
            let hf = containers_unlock.get(&container_id).unwrap();
            let hf_clone = hf.clone();
            // drop(containers_unlock);
            // println!("get_iterator: heapfile fsm: {:?}", hf_clone.hf_file_path);
            drop(containers_unlock);
            return HeapFileIterator::new(container_id, tid, hf_clone);
        } else {
            //  println!("get_iterator: invalid container id");
            panic!("get_iterator panics! container id does not exist!")
        }
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        // first check if container id exists
        let mut containers_unlock = self.containers.read().unwrap();
        let container_id = id.container_id;

        let hf = containers_unlock.get(&container_id);
        match hf {
            Some(hf) => {
                let page_id = id.page_id.unwrap();
                let mut page_read = HeapFile::read_page_from_file(&hf, page_id).unwrap();
                let slot_id = id.slot_id;
                match slot_id {
                    Some(slot_id) => {
                        let val = Page::get_value(&page_read, slot_id);
                        match val {
                            Some(val) => Ok(val),
                            _ => {
                                return Err(CrustyError::ValidationError("Invalid val".to_string()))
                            }
                        }
                    }
                    _ => return Err(CrustyError::ValidationError("Invalid SlotId".to_string())),
                }
            }
            _ => {
                return Err(CrustyError::ValidationError(
                    "Invalid heap file".to_string(),
                ))
            }
        }
        //  panic!("TODO milestone hs");
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm (IGNORE)");
    }

    /// Testing utility to reset all state associated the storage manager.
    fn reset(&self) -> Result<(), CrustyError> {
        // if SM is temp, then delete the temp file
        //   println!("and now im here in reset!");
        if self.containers.write().is_err() || self.containers2.write().is_err() {
            //   println!("reset results in ERR");
            return Err(CrustyError::ExecutionError(
                "rest results in ERR!".to_string(),
            ));
        }
        let mut containers_unlock = self.containers.write().unwrap();
        let mut containers2_unlock = self.containers2.write().unwrap();
        // println!("and now im here in reset!");
        /*if self.is_temp {
            // extract heapfile's temp file from hashmap
            let temp_hf_file_path = containers2_unlock.g);
            // remove the file!
            fs::remove_file(temp_hf_file_path);
        }*/
        // just reset the SM hashmap. Everything else stays the same.
        containers_unlock.clear();
        containers2_unlock.clear();

        Ok(())
        //  panic!("TODO milestone hs");
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    fn clear_cache(&self) {
        panic!("TODO milestone hs (IGNORE)");
    }

    /// Shutdown the storage manager. Can call drop. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    /// If not a temp SM, this should serialize the mapping between containerID and Heapfile.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        // first check if the SM is temporary, and if it's not then serialize & store its contents
        //let mut containers_unlock = self.containers.write().unwrap();
        // println!("beans!");
        let mut containers2_unlock = self.containers2.write().unwrap();
        if !self.is_temp {
            //    println!("self was not temp!");
            // serialize the SM's hashmap w container id & heapfile file paths
            let serialized_hm =
                serde_json::to_string(&*containers2_unlock).expect("failed to serialize");
            //   println!("serialized containers2 hm: {}", serialized_hm);
            // create file path for the file that I will write serialized_hm into
            let mut new_heapfile_path = PathBuf::new();
            new_heapfile_path.push(&self.storage_path.clone());
            new_heapfile_path.push("serialized_hm.json");
            let mut serialized_hm_file = fs::File::create(new_heapfile_path).unwrap();
            // write the serialized info into the file
            serialized_hm_file.write(serialized_hm.as_bytes());
        }
        /*if !self.is_temp {
            for (key, value) in &*containers_unlock {
                // serialize the contents of HeapFile struct
                //  let serialized_hf_file = serde_json::to_string(&value.hf_file_object)
                //     .expect("failed to serialize hf_file");
                let serialized_hf_file_path = serde_json::to_string(&value.hf_file_path)
                    .expect("failed to serialize hf_file");
                let serialized_hf_fsm = serde_json::to_string(&value.free_space_map)
                    .expect("failed to serialize hf_file");
                // create file to store serialized contents
                let mut new_heapfile_path = PathBuf::new();
                new_heapfile_path.push(&self.storage_path.clone());
                new_heapfile_path.push("serialized_hm.json");
                let mut serialized_hm_file = fs::File::create(new_heapfile_path).unwrap();

                // write serialized fields into the file
                //serialized_hm_file.write(serialized_hf_file.as_bytes());
                serialized_hm_file.write(serialized_hf_file_path.as_bytes());
                serialized_hm_file.write(serialized_hf_fsm.as_bytes());
            }
        }*/
        drop(containers2_unlock);
        // then just remove all stored files
        self.reset();
        //panic!("TODO milestone hs");
    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
        _timestamp: LogicalTimeStamp,
    ) -> Result<(), CrustyError> {
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = fs::File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.get_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError(
                        "Could not read row from CSV".to_string(),
                    ));
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    /// Shutdown the storage manager. Can call be called by shutdown. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    fn drop(&mut self) {
        /*if self.is_temp {
            self.reset()
        }*/
        self.shutdown()
        // panic!("TODO milestone hs");
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();
        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());
        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.get_bytes()[..], p2.get_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(1),
            get_random_byte_vec(1),
            get_random_byte_vec(1),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        //  println!("IM HERE IN HS_SM_B_ITER_SMALL 0");
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        //   println!("IM HERE IN HS_SM_B_ITER_SMALL 0.5");

        for (i, x) in iter.enumerate() {
            //   println!("IM HERE IN HS_SM_B_ITER_SMALL: (mine) {:?}", x);
            //   println!("IM HERE IN HS_SM_B_ITER_SMALL: (correct) {:?}", byte_vec[i]);
            assert_eq!(byte_vec[i], x);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);
        //println!("IM HERE IN HS_SM_B_ITER_SMALL 1");
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);
        // println!("IM HERE IN HS_SM_B_ITER_SMALL 2");
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
