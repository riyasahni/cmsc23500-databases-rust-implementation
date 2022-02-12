use crate::page::Page;
use common::ids::PageId;
use common::{CrustyError, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::{Seek, SeekFrom};
// use std::os::unix::prelude::FileExt;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex, RwLock};
/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
///*****************************************************************************
// idea: keep track of the fraction of free space remaining in a page w a
// "free space" vector.
//*********************************************************************************
pub(crate) struct HeapFile {
    pub hf_file: Arc<RwLock<File>>,
    pub free_space_map: Arc<RwLock<Vec<u8>>>,
    //  pub tot_num_of_pages: u16,
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}
/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path and container Id. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf) -> Result<Self, CrustyError> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };

        // TODO milestone hs
        let f = file;
        let f_lock = Arc::new(RwLock::new(f));
        let fsm = Vec::new();
        let fsm_lock = Arc::new(RwLock::new(fsm));
        Ok(HeapFile {
            hf_file: f_lock,
            free_space_map: fsm_lock,
            //   tot_num_of_pages: 0,
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }
    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        let mut fsm = self.free_space_map.write().unwrap();
        // the indexes for elements in free_space_map vector are the page ids
        if fsm.is_empty() {
            return 0;
        } else {
            let max_page_id = fsm.len() as u16;
            max_page_id
        }
    }
    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }
        // if pid > the number of pages in the file, return error
        if pid > self.num_pages() {
            return Err(CrustyError::CrustyError(format!(
                "Cannot open or create heap file",
            )));
        }
        // calculate where the beginning of the page is in the heapfile, given pid
        let page_offset = pid * 4096;
        // open file
        let mut f = self.hf_file.write().unwrap();
        // move cursor to the page_offset position in the file
        f.seek(SeekFrom::Start(page_offset as u64));
        // create buffer (to read 4096 bytes)
        let mut full_read_page = [0; 4096];
        // read full page into array
        f.read_exact(&mut full_read_page)
            .expect("error while reading file");
        // THIS DOESN'T SEEM TO BE WORKING!
        println!("full read page (read_page_from_file ){:?}", full_read_page);
        // use "from_bytes" function to convert bytes into full page
        let final_page = Page::from_bytes(&full_read_page);
        // return page
        Ok(final_page)
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        let mut fsm = self.free_space_map.write().unwrap();
        let page_contig_space = page.get_largest_free_contiguous_space() as u8;
        fsm.borrow_mut().push(page_contig_space);
        // open file
        let mut f = self.hf_file.write().unwrap();
        // calculate where the beginning of the page is in the heapfile, given pid
        let page_offset = page.get_page_id() * 4096;
        // move cursor to the page_offset position in the file
        f.seek(SeekFrom::Start(page_offset as u64))?;
        // convert page to a vector of bytes
        let page_bytes: &[u8] = &Page::get_bytes(&page);
        // write/clone bytes into the heapfile
        f.write(page_bytes);
        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf()).unwrap();

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.get_bytes();
        println!("p0_bytes from test {:?}", p0_bytes);
        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.get_bytes();
        println!("p1_bytes from test {:?}", p1_bytes);
        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.get_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
