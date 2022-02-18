use crate::page::PageIter;
use crate::{heapfile::HeapFile, page::Page};
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
    slot_index: u16,
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
            slot_index: 0,
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
        let hf = &self.heapfile;
        let num_pages = hf.num_pages();

        // iterate through pages in heapfile
        if num_pages == 0 || self.slot_index >= num_pages || self.page_index as u16 > num_pages {
            // if there are no pages in heapfile, return none
            return None;
        }

        // save the page we want to iterate
        let page_to_iterate = HeapFile::read_page_from_file(&hf, self.page_index);
        let page_to_iterate: Page = match self
            .heapfile
            .read_page_from_file(self.page_index.try_into().unwrap())
        {
            Ok(page_to_iterate) => page_to_iterate,
            Err(_) => return None,
        };

        // iterate through page
        let num_records = page_to_iterate.header.vec_of_records.len();
        let mut p_iter = page_to_iterate.into_iter();
        let mut next_rec = p_iter.next();

        // if dont iterating through page, move to the next page (self.page_index += 1)
        if next_rec.is_none() {
            self.page_index += 1;
        }
        while self.slot_index < num_records as u16 {
            for i in 0..num_records {
                p_iter.next();
            }
            self.slot_index += 1;
            return p_iter.next();
        }
        self.slot_index = 0;
        self.page_index += 1;
        return self.next();
    }
}
