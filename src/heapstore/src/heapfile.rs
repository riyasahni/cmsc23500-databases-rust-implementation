use crate::page::Header;
use crate::page::Page;
use common::ids::PageId;
use common::{CrustyError, PAGE_SIZE};
use std::fs::{metadata, File, OpenOptions};
use std::io::prelude::*;
use std::io::{Seek, SeekFrom};
use std::mem::drop;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};
/// The struct for a heap file.  
pub(crate) struct HeapFile {
    pub hf_file_path: PathBuf,
    pub hf_file_object: Arc<RwLock<File>>,
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
        // create new HeapFile struct

        Ok(HeapFile {
            hf_file_path: file_path,
            hf_file_object: Arc::new(RwLock::new(file)),
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
        })
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        let hf_file_metadata = metadata(self.hf_file_path.clone()).unwrap();
        let num_bytes_in_hf = hf_file_metadata.len();
        // we know the size of each page
        (num_bytes_in_hf / 4096) as u16
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
        if pid >= self.num_pages() {
            return Err(CrustyError::CrustyError(
                "Cannot open or create heap file".to_string(),
            ));
        }
        // calculate where the beginning of the page is in the heapfile, given pid
        let page_offset = pid as u64 * PAGE_SIZE as u64;
        // open file
        let f = &mut self.hf_file_object.write().unwrap();
        // move cursor to the page_offset position in the file
        f.seek(SeekFrom::Start(page_offset as u64));
        // create buffer (to read 4096 bytes)
        let mut full_read_page = [0; 4096];
        // read full page into array
        f.read_exact(&mut full_read_page)
            .expect("error while reading file");
        // use "from_bytes" function to convert bytes into full page
        let final_page = Page::from_bytes(&full_read_page);
        // return page
        // drop(f);
        Ok(final_page)
    }

    // BUILD FREE SPACE MAP USING NUM PAGES INFO & GET RID OF THE FSM FIELD IN THE HEAPFILE STRUCT!!!
    pub fn build_free_space_map_for_heapfile(&self) -> Vec<u16> {
        let num_pgs = self.num_pages();
        let mut free_space_map = Vec::new();
        if num_pgs == 0 {
            return free_space_map;
        }
        for i in 0..num_pgs {
            let pg = self.read_page_from_file(i).unwrap();
            let contig_space = Page::get_largest_free_contiguous_space(&pg) as u16;
            free_space_map.push(contig_space);
        }
        free_space_map
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        let file = &mut self.hf_file_object.write().unwrap();
        // calculate where the beginning of the page is in the heapfile, given pid
        let page_offset = page.get_page_id() as u64 * PAGE_SIZE as u64;
        //println!("in heapfile: page_offset: {}", page_offset);
        // move cursor to the page_offset position in the file
        file.seek(SeekFrom::Start(page_offset as u64))?;
        // convert page to a vector of bytes
        let page_bytes: &[u8] = &Page::get_bytes(&page);
        // write/clone bytes into the heapfile
        file.write(page_bytes);
        //drop(file);
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
