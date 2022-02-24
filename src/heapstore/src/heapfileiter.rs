use crate::page::PageIter;
use crate::{heapfile::HeapFile, page::Page};
use common::ids::{ContainerId, PageId, TransactionId};
use core::num;
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
    p_iter: PageIter,
    //slot_index: u16,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the container_id, tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(container_id: ContainerId, tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        let new_p_iter = PageIter {
            page: hf.read_page_from_file(0).unwrap(),
            index: 0,
        };
        // just need to create a new heapfile-iter here and return it...
        let new_HeapFileIter = HeapFileIterator {
            heapfile: hf,
            container_id: container_id,
            transaction_id: tid,
            page_index: 0,
            slot_index: 0,
            p_iter: new_p_iter,
            //slot_index: 0,
        };
        new_HeapFileIter
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        ////////////////// --- alternative method --- ////////////////
        /*let num_pages = self.heapfile.num_pages();
        if self.page_index >= num_pages {
            None
        } else {
            let p = match self
                .heapfile
                .read_page_from_file(self.page_index.try_into().unwrap())
            {
                Ok(p) => p,
                Err(_) => return None,
            };
            let num_slots = p.header.vec_of_records.len().clone() as u16;
            let mut iter = p.into_iter();
            if self.slot_index < num_slots {
                if self.slot_index > 0 {
                    for _ in 0..self.slot_index {
                        iter.next();
                    }
                }
                self.slot_index += 1;
                return iter.next();
            } else {
                self.slot_index = 0;
                self.page_index += 1;
                return self.next();
            }
        }*/
        ////////////////// --- alternative method --- ////////////////
        // open heapfile
        let hf = &self.heapfile;
        let num_pages = hf.num_pages();
        println!("in heapfileiter: num pages {}", num_pages);

        // iterate through pages in heapfile
        if self.page_index as u16 >= num_pages {
            // if there are no pages in heapfile, return none
            return None;
        }
        // first check if I need to read a new page
        let next_val = self.p_iter.next();
        // if next value to read is done, it means we've finished reading this page
        if next_val.is_none() {
            // move to next page
            self.page_index += 1;
            println!(
                "in heapfileiter: moving to the next page {}",
                self.page_index
            );
            if self.page_index as u16 >= num_pages {
                // if there are no pages in heapfile, return none
                println!(
                    "in heapfileiter: we're out of pages. page_index: {}, num_pages: {}",
                    self.page_index, num_pages
                );
                return None;
            }
            // extract the new page
            let new_page = hf.read_page_from_file(self.page_index).unwrap();
            self.p_iter.page = new_page;
            // reset page index
            self.p_iter.index = 0;
            return self.next();
        }
        return next_val;
    }
}
