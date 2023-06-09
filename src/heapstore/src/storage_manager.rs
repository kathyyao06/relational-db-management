use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_test_sm_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

/// The StorageManager struct
//#[derive(Serialize, Deserialize)]
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_path: PathBuf,
    tracker: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>,
    /// Indicates if this is a temp StorageManager (for testing)
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
        //gets heapfile from storage path
        let file_path = &self.storage_path.join(format!("{}.hf", container_id));
        let hf = match HeapFile::new(file_path.to_path_buf(), container_id) {
            Ok(hf) => hf,
            Err(_) => panic!("Err"),
        };
        //reads page from file
        match hf.read_page_from_file(page_id) {
            Err(_) => None,
            Ok(page) => Some(page),
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        //gets heapfile from storage path
        let file_path = &self.storage_path.join(format!("{}.hf", container_id));
        let hf = match HeapFile::new(file_path.to_path_buf(), container_id) {
            Ok(hf) => hf,
            Err(_) => panic!("Err"),
        };
        //write page to heapfile
        match hf.write_page_to_file(page) {
            Ok(()) => {
                self.tracker
                    .write()
                    .unwrap()
                    .insert(container_id, Arc::new(hf));
                Ok(())
            }
            Err(_) => Err(CrustyError::CrustyError(
                "Unable to write a page".to_string(),
            )),
        }
    }

    /// Get the number of pages for a container
    #[allow(dead_code)]
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        //gets heapfile from storage path and gets number of pages
        let file_path = &self.storage_path.join(format!("{}.hf", container_id));
        let hf = match HeapFile::new(file_path.to_path_buf(), container_id) {
            Ok(hf) => hf,
            Err(_) => return 0,
        };
        hf.num_pages()
    }

    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        let file_path = &self.storage_path;
        let hf = match HeapFile::new(file_path.to_path_buf(), container_id) {
            Ok(hf) => hf,
            Err(_) => return (0, 0),
        };
        let read = hf.read_count.load(Ordering::Relaxed);
        let write = hf.write_count.load(Ordering::Relaxed);
        (read, write)
    }

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => {
                format!("{:?}", p)
            }
            None => String::new(),
        }
    }

    /// For testing
    pub fn get_page_bytes(&self, container_id: ContainerId, page_id: PageId) -> Vec<u8> {
        match self.get_page(
            container_id,
            page_id,
            TransactionId::new(),
            Permissions::ReadOnly,
            false,
        ) {
            Some(p) => p.to_bytes(),
            None => Vec::new(),
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk; not the case for memstore)
    /// For startup/shutdown: check the storage_path for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_path: PathBuf) -> Self {
        let tracker = Arc::new(RwLock::new(HashMap::new()));
        let path = storage_path.join("serialized");
        if path.exists() && path.is_file() {
            let mut file = File::open(path).unwrap();
            let mut bytes: Vec<u8> = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            let mut i = 0;
            while i < bytes.len() {
                let container_id = u16::from_be_bytes([bytes[i], bytes[i + 1]]);
                let file_path = storage_path.join(format!("{}.hf", container_id));
                let hf = match HeapFile::new(file_path.to_path_buf(), container_id) {
                    Ok(hf) => hf,
                    Err(_) => panic!("Err"),
                };
                tracker
                    .write()
                    .unwrap()
                    .insert(container_id, Arc::new(hf))
                    .unwrap();
                i += 2;
            }
        }
        StorageManager {
            storage_path,
            tracker,
            is_temp: false,
        }
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        let storage_path = gen_random_test_sm_dir();
        let tracker = Arc::new(RwLock::new(HashMap::new()));
        StorageManager {
            storage_path,
            tracker,
            is_temp: true,
        }
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
        _tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
        let mut val = ValueId {
            container_id,
            segment_id: None,
            page_id: None,
            slot_id: None,
        };

        //gets heapfile from storage path
        let file_path = &self.storage_path.join(format!("{}.hf", container_id));
        let hf = match HeapFile::new(file_path.to_path_buf(), container_id) {
            Ok(hf) => hf,
            Err(_) => panic!("Container doesn't exist!"),
        };
        //increments through pages in heapfile, seeing if theres free space to add value
        for p in 0..hf.num_pages() {
            let mut pg = match hf.read_page_from_file(p) {
                Ok(pg) => pg,
                Err(_) => panic!("Could not read page from file"),
            };

            match pg.add_value(&value) {
                Some(slot_id) => {
                    val.page_id = Some(p);
                    val.slot_id = Some(slot_id);
                    match hf.write_page_to_file(pg) {
                        Ok(pg) => pg,
                        Err(_) => panic!("Cannot write page to file"),
                    }
                    return val;
                }
                None => {
                    continue;
                }
            }
        }

        //if no free space, creates new page and adds value
        let mut new_page: Page = Page::new(hf.num_pages());
        val.page_id = Some(hf.num_pages());
        val.slot_id = new_page.add_value(&value);
        hf.write_page_to_file(new_page)
            .expect("Cannot write page to file");
        self.tracker
            .write()
            .unwrap()
            .insert(container_id, Arc::new(hf));

        val
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
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, _tid: TransactionId) -> Result<(), CrustyError> {
        let container_id = id.container_id;
        let file_path = &self.storage_path.join(format!("{}.hf", container_id));

        if id.page_id.is_none() || id.slot_id.is_none() {
            //if valueID not found
            Ok(())
        } else {
            let slot_id = id.slot_id.unwrap();
            let page_id = id.page_id.unwrap();
            //reads heapfile from storage path
            let hf = match HeapFile::new(file_path.to_path_buf(), container_id) {
                Ok(hf) => hf,
                Err(_) => return Ok(()),
            };

            //reads page from heapfile with page_id and deletes value
            let mut pg = match hf.read_page_from_file(page_id) {
                Ok(pg) => pg,
                Err(_) => return Ok(()),
            };
            pg.delete_value(slot_id);

            match hf.write_page_to_file(pg) {
                Ok(pg) => pg,
                Err(_) => panic!("Cannot write page to file"),
            }
            Ok(())
        }
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
        if self.delete_value(id, _tid).is_err() {
            Err(CrustyError::CrustyError(
                "Unable to write a page".to_string(),
            ))
        } else {
            let container_id = id.container_id;
            Ok(self.insert_value(container_id, value, _tid))
        }
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
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        let file_path = &self.storage_path;
        match fs::create_dir_all(file_path) {
            Ok(_) => {}
            Err(_) => {
                return Err(CrustyError::CrustyError(
                    "Unable to write a page".to_string(),
                ))
            }
        }
        //creates new heapfile in heap_file_path
        let heap_file_path = self.storage_path.join(format!("{}.hf", container_id));
        let hf =
            HeapFile::new(heap_file_path, container_id).expect("Unable to create new heapfile.");

        //adds (container_id, heapfile) to tracker
        let hf_arc = Arc::new(hf);
        self.tracker.write().unwrap().insert(container_id, hf_arc);
        Ok(())
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(container_id, None, common::ids::StateType::BaseTable, None)
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let mut tracker = self.tracker.write().unwrap();
        if tracker.remove(&container_id).is_some() {
            Ok(())
        } else {
            Err(CrustyError::CrustyError(
                "Container doesn't exist".to_string(),
            ))
        }
    }

    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let file_path = &self.storage_path.join(format!("{}.hf", container_id));
        let hf = match HeapFile::new(file_path.to_path_buf(), container_id) {
            Ok(hf) => hf,
            Err(_) => panic!("Err"),
        };
        let iter: HeapFileIterator = HeapFileIterator::new(tid, Arc::new(hf));
        iter
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        _tid: TransactionId,
        _perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let container_id = id.container_id;
        if id.page_id.is_none() || id.slot_id.is_none() {
            Err(CrustyError::CrustyError(
                "Container doesn't exist".to_string(),
            ))
        } else {
            let hm = self.tracker.read().unwrap();
            let hf = hm.get(&container_id).unwrap().clone();
            let page = hf
                .read_page_from_file(id.page_id.unwrap())
                .expect("Unable to read page");
            let val = page.get_value(id.slot_id.unwrap()).unwrap();
            Ok(val)
        }
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, _tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state.
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        fs::remove_dir_all(self.storage_path.clone())?;
        fs::create_dir_all(self.storage_path.clone()).unwrap();

        self.tracker.write().unwrap().clear();
        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    fn clear_cache(&self) {}

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new.
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        let mut bytes: Vec<u8> = Vec::new();
        //let mut file = OpenOptions::new().read(true).write(true).create(true).open("store_file.txt").unwrap();
        let path = self.storage_path.clone();
        let filename = format!("{}{}", path.to_str().unwrap(), "serialized");
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
        {
            Ok(f) => f,
            Err(_) => {
                println!("err");
                panic!("err")
            }
        };

        let heap_files = self.tracker.read().unwrap().clone();
        let size_hm = heap_files.len();
        for i in 0..size_hm {
            let container_id: [u8; 2] = (i as u16).to_be_bytes();
            bytes.push(container_id[0]);
            bytes.push(container_id[1]);
        }
        serde_cbor::to_writer(file, &bytes).expect("error");
    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
    ) -> Result<(), CrustyError> {
        // Err(CrustyError::CrustyError(String::from("TODO")))
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
                    self.insert_value(container_id, tuple.to_bytes(), _tid);
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
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_path);
            let remove_all = fs::remove_dir_all(self.storage_path.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
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
        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
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
            assert_eq!(byte_vec[i], x.0);
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
            assert_eq!(byte_vec[i], x.0);
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
            assert_eq!(byte_vec[i], x.0);
        }
    }

    #[test]
    //#[ignore]
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
