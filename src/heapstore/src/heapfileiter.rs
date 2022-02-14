use crate::heapfile::HeapFile;
use crate::page::PageIter;
use common::ids::{ContainerId, PageId, TransactionId};
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    //TODO milestone hs
    heapfile: Arc<HeapFile>,
    container_id: ContainerId,
    transaction_id: TransactionId,
    page_index: u16,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the container_id, tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(container_id: ContainerId, tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        // just need to create a new heapfile-iter here and return it...
        let new_HeapFileIter = HeapFileIterator {
            heapfile: hf,
            container_id: container_id,
            transaction_id: tid,
            page_index: 0,
        };
        new_HeapFileIter
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        // open heapfile
        let hf = self.heapfile;
        // iterate through pages in heapfile
        if hf.free_space_map.len().is_empty() {
            // if there are no pages in heapfile, return none
            return None;
        }
        // if we're out of pages, return none
        if self.page_index as u16 > hf.num_pages() {
            return None;
        }
        // save the page we want to iterate
        let page_to_iterate = HeapFile::read_page_from_file(hf, self.page_index);
        // iterate through page
        let mut p_iter = page_to_iterate.into_iter();
        iter.next();
        // once done iterating through page, move to the next page (self.page_index += 1)
        if iter.next().is_none() {
            self.page_index += 1;
        }
    }
}
