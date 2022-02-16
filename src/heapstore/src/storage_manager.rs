use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::{Header, Page};
use common::delta_storage_trait::Value;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_dir;
use common::testutil::init;
use common::PAGE_SIZE;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};

/// The StorageManager struct
#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    pub containers: Arc<RwLock<HashMap<ContainerId, (PathBuf, Vec<u16>)>>>,
    /// Path to database metadata files.
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
            let hf_file_path = containers_unlock[&container_id].0.clone();
            let hf_free_space_map = containers_unlock[&container_id].1.clone();
            let hf =
                HeapFile::returns_heapfile_from_filepath_and_fsm(hf_file_path, hf_free_space_map);
            // check if page_id exists in heapfile
            if hf.free_space_map.write().unwrap().is_empty() {
                return None;
            }
            if page_id > (hf.free_space_map.write().unwrap().len() - 1) as u16 {
                return None;
            }
            // if page_id exists, then read page from file & return the page
            Some(HeapFile::read_page_from_file(&hf, page_id).unwrap());
        }
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
        let hf_file_path = &containers_unlock[&container_id].0.clone();
        let hf_free_space_map = &containers_unlock[&container_id].1.clone();
        let hf = HeapFile::returns_heapfile_from_filepath_and_fsm(
            hf_file_path.to_path_buf(),
            hf_free_space_map.to_vec(),
        );
        // let hf = HeapFile::new(hf_file_path.to_path_buf()).unwrap();
        // write page to file
        HeapFile::write_page_to_file(&hf, page)
        //  panic!("TODO hs")
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        //return 100;
        let containers_unlock = self.containers.read().unwrap();
        //for (key, value) in &*self.containers.write().unwrap() {
        //    println!("{} / {:?}", key, value);
        //}
        //return -100;
        let hf_free_space_map = containers_unlock[&container_id].1.clone();
        println!("GET NUM PAGES free space map: {:?}", hf_free_space_map);
        return hf_free_space_map.len() as u16;
        panic!("TODO hs")
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        /*let containers_unlock = self.containers.write().unwrap();
        // iterate through containers,
        for (key, value) in containers_unlock.iter() {
            // check if containerid == container_id
            if key == &container_id {
                // save that heapfile
                let hf_file_path = value.0.clone();
                let hf_free_space_map = value.1.clone();
                let hf = HeapFile::returns_heapfile_from_filepath_and_fsm(
                    hf_file_path,
                    hf_free_space_map,
                );
                // store the read count from file
                let write_count = hf.write_count.load(Ordering::Relaxed);
                // store write count from file
                let read_count = hf.read_count.load(Ordering::Relaxed);
                // return (read_count, write_count)
                return (read_count, write_count);
            }
        }
        // return (0, 0) for invlaid container_id
        (0, 0) */
        panic!("TODO hs")
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk)
    fn new(storage_path: String) -> Self {
        // check if storage path already exists
        let mut storage_path_PathBuf = PathBuf::new();
        storage_path_PathBuf.push(storage_path.clone());
        if storage_path_PathBuf.is_dir() {
            // get the file path for the serialized heapfile info
            let mut saved_heapfile_path = PathBuf::new();
            saved_heapfile_path.push(storage_path.clone());
            saved_heapfile_path.push("serialized_hm.json");
            // open up the serialized hf for this SM directory
            let mut serialized_hm_file = fs::File::open(saved_heapfile_path).unwrap();
            // deserialize the file back into a hashmap
            // let config_str = std::fs::read_to_string("testdata.json").unwrap();
            let containers: HashMap<ContainerId, (PathBuf, Vec<u16>)> =
                serde_json::from_reader(serialized_hm_file).expect("cannot deserialize file");
            // recreate the SM
            let reloaded_SM = StorageManager {
                containers: Arc::new(RwLock::new(containers)),
                storage_path: storage_path,
                is_temp: false,
            };
            return reloaded_SM;
        }
        // if storage path does not already exist, then no need to reload anything
        // construct a new, empty SM
        fs::create_dir_all(storage_path.clone());
        let new_SM = StorageManager {
            containers: Arc::new(RwLock::new(HashMap::new())),
            storage_path: storage_path,
            is_temp: false,
        };
        // return it
        new_SM
        //  panic!("TODO hs")
    }

    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self {
        init();
        let storage_path = gen_random_dir().to_string_lossy().to_string();
        debug!("Making new temp storage_manager {}", storage_path);
        // use new() and storage_path to make test SM
        let test_SM = StorageManager::new(storage_path);
        // return test SM
        test_SM
        // panic!("TODO hs")
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
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }

        ////////////////-------------------------------------------------------------//////////////////////
        /*let mut unlock_hashmap = self.containers.write().unwrap();

        let curr_file_path_hf = unlock_hashmap.clone()[&container_id].0.clone();
        let mut new_free_space = unlock_hashmap.clone()[&container_id].1.clone();

        let mut page_id_found = false;
        let mut page_id: PageId = 0;

        let mut new_file = fs::File::open(curr_file_path_hf);
        let mut curr_file_path_hf_again = unlock_hashmap.clone()[&container_id].0.clone();

        let new_hf = HeapFile {
            hf_file_path: Arc::new(RwLock::new(curr_file_path_hf_again)),
            free_space_map: Arc::new(RwLock::new(new_free_space.clone())),
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        };
        for x in 0..new_free_space.len() as u16 {
            if new_free_space[x as usize] >= value.len() as u16 {
                page_id_found = true;
                page_id = x;
            }
        }
        let mut free_space_len = new_free_space.len();

        let mut res_slot_id = 0;
        let mut read_page = Page::new(page_id);

        if free_space_len > 0 {
            let mut read_page: Page = HeapFile::read_page_from_file(&new_hf, page_id).unwrap();
            let res_slot_id_opt = Page::add_value(&mut read_page, &value);
            match res_slot_id_opt {
                Some(value) => res_slot_id = value,
                None => page_id_found = false,
            };
        }
        if free_space_len == 0 {
            page_id_found = false;
        }

        if page_id_found {
            HeapFile::write_page_to_file(&new_hf, read_page);

            let mut unlock_fsv = unlock_hashmap.clone()[&container_id].1.clone();
            let mut lent = unlock_fsv.len();
            let mut read_page_again = HeapFile::read_page_from_file(&new_hf, page_id).unwrap();
            unlock_fsv[page_id as usize] =
                Page::get_largest_free_contiguous_space(&read_page_again) as u16;
            let mut unlock_hf_fsv = new_hf.free_space_map.write().unwrap();
            let mut new_fsv = unlock_hf_fsv.clone();
            new_fsv.clear();
            for x in 0..lent {
                new_fsv.push(unlock_fsv.len().try_into().unwrap());
            }

            drop(unlock_hf_fsv);
            drop(unlock_hashmap);

            return ValueId {
                container_id,
                segment_id: None,
                page_id: Some(page_id),
                slot_id: Some(res_slot_id),
            };
        } else {
            let mut new_page = Page::new(free_space_len as PageId);

            let res_slot_id = Page::add_value(&mut new_page, &value).unwrap();
            let mut new_free_space = unlock_hashmap.clone()[&container_id].1.clone();
            let mut free_space_len_fin = 0;
            if (new_free_space.len() <= page_id as usize) || (new_free_space.len() == 0) {
                let mut new_free_space = &mut unlock_hashmap.get_mut(&container_id).unwrap().1;
                new_free_space.push(
                    Page::get_largest_free_contiguous_space(&new_page)
                        .try_into()
                        .unwrap(),
                );
                free_space_len_fin = new_free_space.len();
            } else {
                let mut new_free_space = &mut unlock_hashmap.get_mut(&container_id).unwrap().1;
                new_free_space[page_id as usize] =
                    Page::get_largest_free_contiguous_space(&new_page) as u16;
                free_space_len_fin = new_free_space.len();
                let mut unlock_hf_fsv = new_hf.free_space_map.write().unwrap();
                let mut new_fsv = unlock_hf_fsv.clone();
                new_fsv.clear();
                for x in 0..free_space_len_fin {
                    new_fsv.push(new_free_space[x]);
                }
            }
            let new_len = free_space_len_fin - 1;
            drop(new_free_space);
            HeapFile::write_page_to_file(&new_hf, new_page);
            return ValueId {
                container_id,
                segment_id: None,
                page_id: Some(new_len as u16),
                slot_id: Some(res_slot_id),
            };
        }*/
        ////////////////-------------------------------------------------------------//////////////////////

        // if it's a new heapfile, create a page
        // first determine if there is a page with space available
        // if yes, add the value to an existing page
        // if no, create a new page
        let n = self.get_num_pages(container_id);
        for i in 0..n {
            //debug!("loop for page id: {}", i);
            let mut pg =
                Self::get_page(&self, container_id, i, tid, Permissions::ReadOnly, false).unwrap();
            let slot_id = pg.add_value(&value);
            if slot_id.is_none() {
                continue;
            }

            self.write_page(container_id, pg, tid).unwrap();
            return ValueId {
                container_id,
                segment_id: None,
                page_id: Some(i),
                slot_id,
            };
        }
        // if page doesn't exist, then create a page and add the value
        let mut pg = Page::new(n);
        let slot_id = pg.add_value(&value);
        pg.add_value(&value);

        self.write_page(container_id, pg, tid).unwrap();

        ValueId {
            container_id,
            segment_id: None,
            page_id: Some(n),
            slot_id,
        }

        /*let hf = HeapFile {
            hf_file_path: Arc::new(RwLock::new(hf_file_path.clone())),
            free_space_map: Arc::new(RwLock::new(hf_free_space_map.clone())),
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        };
        // check if hf is new (aka no pages to insert value)
        if hf_free_space_map.is_empty() {
            println!("IM HERE IN INSERT_VALUE3");
            // if no existing available page, then write new page to hf
            let new_page_id = 0 as u16;
            let mut new_Page = Page::new(new_page_id);
            // write value to new page
            Page::add_value(&mut new_Page, &value);
            println!("IM HERE IN INSERT_VALUE4");
            //  let new_page_free_space = Page::get_largest_free_contiguous_space(&new_Page) as u16;
            hf_free_space_map.push(
                Page::get_largest_free_contiguous_space(&new_Page)
                    .try_into()
                    .unwrap(),
            );
            println!("IM HERE IN INSERT_VALUE5");
            // let mut fsm = hf.free_space_map.write().unwrap();
            HeapFile::write_page_to_file(&hf, new_Page);
            println!("insert_value hs_free_space_map2: {:?}", hf.free_space_map);
            println!("IM HERE IN INSERT_VALUE6");
            // create ValueID
            let new_value_id = ValueId {
                container_id: container_id,
                segment_id: None,
                page_id: Some(new_page_id),
                slot_id: Some(0),
            };
            // return ValueID
            return new_value_id;
        } else {
            // **THIS IS WRONG**
            ValueId {
                container_id: container_id,
                segment_id: None,
                page_id: Some(9),
                slot_id: Some(9),
            }
        }*/

        /*
        ////   let mut page_id_found = false;
        let mut page_id: PageId = 0;

        let mut new_file = fs::File::open(hf_file_path);
        let mut hf_file_path_again = containers_lock.clone()[&container_id].0.clone();

        let hf = HeapFile {
            hf_file_path: Arc::new(RwLock::new(hf_file_path_again)),
            free_space_map: Arc::new(RwLock::new(hf_free_space_map.clone())),
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        }; */

        ////////////////////////////////////////////////////////////////////////////
        /*// check hf's free space map to find first available page to insert value
        for i in 0..(hf_free_space_map.len() - 1) as u16 {
            let mut page_from_hf = HeapFile::read_page_from_file(&hf, i).unwrap();
            // if available page exists, add_value to page and update free space map
            if Page::add_value(&mut page_from_hf, &value).is_some() {
                let new_slot_id = Page::add_value(&mut page_from_hf, &value);
                hf_free_space_map[i as usize].borrow_mut() =
                    Page::get_largest_free_contiguous_space(&page_from_hf) as u16;
                // create ValueID
                let new_value_id = ValueId {
                    container_id: container_id,
                    segment_id: None,
                    page_id: serde::__private::Some(i),
                    slot_id: new_slot_id,
                };
                // dropping lock to fix my deadlock error!!
                // println!("insert_value calling drop");
                //  drop(containers_lock);
                // return ValueID
                return new_value_id;
            }
        } */
        //  }
        ////////////////////////////////////////////////////////////////////////////////
        /*let hf_file_path = containers_lock[&container_id].0.clone();

        let mut hf_free_space_map = containers_lock[&container_id].1.clone();
        println!("insert_value hs_free_space_map1: {:?}", hf_free_space_map);
        let mut hf = HeapFile::returns_heapfile_from_filepath_and_fsm(
            hf_file_path.to_path_buf(),
            hf_free_space_map.to_vec(),
        );
        // if no existing available page, then write new page to hf
        let new_page_id = hf_free_space_map.len();
        let mut new_Page = Page::new(new_page_id.try_into().unwrap());
        let mut new_Page_clone = Page::new(new_page_id.try_into().unwrap());
        HeapFile::write_page_to_file(&hf, new_Page);
        // add_value to page, and update free space map
        let new_slot_id = Page::add_value(&mut new_Page_clone, &value);
        let new_page_free_space = Page::get_largest_free_contiguous_space(&new_Page_clone) as u16;
        hf_free_space_map.push(new_page_free_space);
        // create ValueID
        let new_value_id = ValueId {
            container_id: container_id,
            segment_id: None,
            page_id: Some(new_page_id.try_into().unwrap()),
            slot_id: new_slot_id,
        };
        // return ValueID
        return new_value_id; */
        //////////////////////////////////////////////////////////////////////
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
            let val_id = self.insert_value(container_id, val.to_vec(), tid);
            vec_of_value_ids.push(val_id);
        }
        vec_of_value_ids
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        let container_id = id.container_id;
        let page_id = id.page_id.unwrap();
        let slot_id = id.slot_id.unwrap();
        // find the heapfile associated with container_id that's stored in ValueID
        let mut containers_lock = self.containers.write().unwrap();
        if containers_lock.contains_key(&container_id) {
            // extract heapfile's file path from hashmap
            let hf_file_path = containers_lock[&container_id].0.clone();
            let hf_free_space_map = containers_lock[&container_id].1.clone();
            let hf =
                HeapFile::returns_heapfile_from_filepath_and_fsm(hf_file_path, hf_free_space_map);
            let mut fsm = hf.free_space_map.write().unwrap();
            // check if page with given page id exists & extract
            if page_id <= (fsm.len() - 1) as u16 {
                // extract page with given page_id
                let mut extracted_page = HeapFile::read_page_from_file(&hf, page_id).unwrap();
                // delete_value() on page for given slot_id if the corresponding record exists
                Page::delete_value(&mut extracted_page, slot_id);
                // dropping lock to fix my deadlock error!!
                println!("delete_value calling drop");
                drop(containers_lock);
                // done
                return Ok(());
            }
        }
        Ok(())
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
        let container_id = id.container_id;
        let page_id = id.page_id.unwrap();
        let slot_id = id.slot_id.unwrap();
        // first delete the record
        self.delete_value(id, _tid);
        // then re-insert record into heapfile
        Ok(self.insert_value(container_id, value, _tid))
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
        // construct the new heapfile path
        let mut new_heapfile_path = PathBuf::new();
        new_heapfile_path.push(&self.storage_path);
        new_heapfile_path.push("hf_path.hf");
        // construct new empty free space map
        let mut free_space_map = Vec::new();
        // if container already has container id then don't add container id again
        if containers.contains_key(&container_id) {
            debug!(
                "memstore::create_container container_id: {:?} already exists",
                &container_id
            );
            // dropping lock to fix my deadlock error!!
            println!("create_container calling drop");
            drop(containers);
            return Ok(());
        }
        // else, insert new heapfile (value) with given container_id (key)

        containers.insert(container_id, (new_heapfile_path, free_space_map));
        // dropping lock to fix my deadlock error!!
        println!("create_container calling drop");
        drop(containers);
        Ok(())
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
        /*let mut containers_lock = self.containers.write().unwrap();
        if containers_lock.contains_key(&container_id) {
            // extract heapfile's file path from hashmap
            let hf_file_path = containers_lock[&container_id].0.clone();
            let hf_free_space_map = containers_lock[&container_id].1.clone();
            let hf =
                HeapFile::returns_heapfile_from_filepath_and_fsm(hf_file_path, hf_free_space_map);
            let mut fsm = hf.free_space_map.write().unwrap(); */
        // delete the heapfile w associated container id
        let mut containers_unlock = self.containers.write().unwrap();
        let hf_file_path = containers_unlock[&container_id].0.clone();
        fs::remove_file(hf_file_path);
        // remove the tuple value associated with container id key
        containers_unlock.remove(&container_id);
        // remove container id from hashmap
        // let mut hm_container_id_lock = self.containers.write().unwrap();
        // hm_container_id_lock.remove(&container_id);
        // dropping lock to fix my deadlock error!!
        println!("remove_container calling drop");
        drop(containers_unlock);
        Ok(())
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        // find the heapfile associated with container_id that's stored in ValueID
        let mut containers_lock = self.containers.write().unwrap();
        //   if containers_lock.contains_key(&container_id) {
        // extract heapfile's file path from hashmap
        let hf_file_path = containers_lock[&container_id].0.clone();
        let hf_free_space_map = containers_lock[&container_id].1.clone();
        let hf = HeapFile::returns_heapfile_from_filepath_and_fsm(hf_file_path, hf_free_space_map);
        let mut arc_hf = Arc::new(hf);
        // dropping lock to fix my deadlock error!!
        println!("get_iterator calling drop");
        drop(containers_lock);
        // return a new heapfile iterator!
        return HeapFileIterator::new(container_id, tid, arc_hf);
        //  }
        // if container id is invalid, then panic!
        // panic!("Invaid container id!");
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
        //  if containers_unlock.contains_key(&container_id) {
        // extract heapfile from hashmap
        // extract heapfile's file path from hashmap
        let hf_file_path = containers_unlock[&container_id].0.clone();
        let hf_free_space_map = containers_unlock[&container_id].1.clone();
        let hf = HeapFile::returns_heapfile_from_filepath_and_fsm(hf_file_path, hf_free_space_map);
        // read page from hf (returns Crusty Error if doesn't exist)
        let page_id = id.page_id.unwrap();
        let mut page_read = HeapFile::read_page_from_file(&hf, page_id).unwrap();
        // get value from page with given slot id
        let slot_id = id.slot_id.unwrap();
        let val = Page::get_value(&page_read, slot_id);
        // convert val from Option<> to Result<> type
        return Option::ok_or(
            val,
            CrustyError::ValidationError("Invalid value id".to_string()),
        );
        //  }
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm (IGNORE)");
    }

    /// Testing utility to reset all state associated the storage manager.
    fn reset(&self) -> Result<(), CrustyError> {
        // if SM is temp, then delete the temp file
        let mut containers_unlock = self.containers.write().unwrap();
        /* if self.is_temp {
             // extract heapfile's temp file from hashmap
             let temp_hf_file_path = containers_unlock[].0.clone();
             // remove the file!
            fs::remove_file(temp_hf_file_path);
        } */
        // just reset the SM hashmap. Everything else stays the same.
        containers_unlock.clear();
        Ok(())
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
        if !self.is_temp {
            // serialize the SM's hashmap w container id & heapfile maps
            let containers_lock = self.containers.write().unwrap();
            let serialized_hm =
                serde_json::to_string(&*containers_lock).expect("failed to serialize");
            // create file path for the file that I will write serialized_hm into
            let mut new_heapfile_path = PathBuf::new();
            new_heapfile_path.push(&self.storage_path.clone());
            new_heapfile_path.push("serialized_hm.json");
            let mut serialized_hm_file = fs::File::create(new_heapfile_path).unwrap();
            // write the serialized info into the file
            serialized_hm_file.write(serialized_hm.as_bytes());
        }
        // then just remove all stored files
        self.reset();
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
        self.shutdown()
        //  panic!("TODO milestone hs");
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
        println!("I'M HERE IN HS_SM_A_INSERT1");
        sm.create_table(cid);
        println!("I'M HERE IN HS_SM_A_INSERT2");
        let bytes = get_random_byte_vec(40);
        println!("I'M HERE IN HS_SM_A_INSERT3");
        let tid = TransactionId::new();
        println!("I'M HERE IN HS_SM_A_INSERT4");
        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        println!("I'M HERE IN HS_SM_A_INSERT5");
        assert_eq!(1, sm.get_num_pages(cid));
        println!("I'M HERE IN HS_SM_A_INSERT6");
        assert_eq!(0, val1.page_id.unwrap());
        println!("I'M HERE IN HS_SM_A_INSERT7");
        assert_eq!(0, val1.slot_id.unwrap());
        println!("I'M HERE IN HS_SM_A_INSERT8");

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
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
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
