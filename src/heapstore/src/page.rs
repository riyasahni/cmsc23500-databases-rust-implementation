use common::ids::{PageId, SlotId};
use common::PAGE_SIZE;
use log::Log;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::mem;
use std::sync::mpsc::Receiver;

use crate::heapfile;

/// The struct for a page. Note this can hold more elements/meta data when created,
/// but it must be able to be packed/serialized/marshalled into the data array of size
/// PAGE_SIZE. In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// You do not need reclaim header information for a value inserted (eg 6 bytes per value ever inserted)
/// The rest must filled as much as possible to hold values.

pub(crate) struct Page {
    data: [u8; PAGE_SIZE],
    header: Header,
}

pub(crate) struct Record {
    pub end_location: u16,
    pub beg_location: u16,
    pub is_deleted: u8,
}

pub(crate) struct Header {
    pub ptrEndofFreeSpace: u16,
    pub ptrBegofFreeSpace: u16,
    pub PageID: PageId,
    pub vecOfRecords: Vec<Record>,
}

/// The functions required for page
impl Page {
    /// Create a new page and a struct for header
    pub fn new(page_id: PageId) -> Self {
        let newHeader = Header {
            ptrEndofFreeSpace: 4096,
            ptrBegofFreeSpace: 6,
            PageID: page_id,
            vecOfRecords: Vec::new(),
        };
        let newPage = Page {
            data: [0; PAGE_SIZE],
            header: newHeader,
        };
        newPage
    }

    /// Return the page id for a page
    pub fn get_page_id(&self) -> PageId {
        let pageId = self.header.PageID;
        pageId
    }

    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you can replace the slotId.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);

    pub fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
        // the new header size would be 5 bytes larger if we added a new record
        let new_header_size = self.get_header_size() + 5;
        // the new pointer to end of free space would move up by the length of the data for record
        let new_ptr_end_of_free_space = self.header.ptrEndofFreeSpace - bytes.len() as u16;
        // check to see if there is enough space in the page to add the new record
        if self.get_largest_free_contiguous_space() >= bytes.len() {
            // record's beginning location is offset - record's length
            let new_beg_location = self.header.ptrEndofFreeSpace - bytes.len() as u16;
            println!("new beg loc: {}", new_beg_location);
            // record's end location is what the offset used to be
            let new_end_location = self.header.ptrEndofFreeSpace;
            println!("new end loc: {}", new_end_location);
            // reuse slot id of deleted record by inserting new record info in the
            // deleted vector's index
            for j in 0..self.header.vecOfRecords.len() {
                if self.header.vecOfRecords[j].is_deleted == 1 {
                    // if record has been deleted, the new record takes its slot id
                    self.header.vecOfRecords[j].beg_location = new_beg_location;
                    self.header.vecOfRecords[j].end_location = new_end_location;
                    // update the is_deleted flag
                    self.header.vecOfRecords[j].is_deleted = 0;
                    // save the new record's slot id
                    let new_slotid = j;
                    // copy new record data into the page at the new beginning location
                    self.data[new_beg_location as usize..new_end_location as usize]
                        .clone_from_slice(&bytes);
                    // update the ptrEndOfFreeSpace
                    self.header.ptrEndofFreeSpace = new_beg_location;
                    // return the new slot id for the new record!
                    return Some(new_slotid as u16);
                }
            }
            // otherwise, create a new record
            let new_record = Record {
                beg_location: new_beg_location,
                end_location: new_end_location,
                is_deleted: 0,
            };
            // push new record into vector of records
            // the index of the new_record is its slot id
            self.header.vecOfRecords.push(new_record);
            // update the ptrEndOfFreeSpace
            self.header.ptrEndofFreeSpace = new_beg_location;
            // update the ptrBegOfFreeSpace
            self.header.ptrBegofFreeSpace += 5;
            // copy new record data into the page at the new beginning location
            self.data[new_beg_location as usize..new_end_location as usize]
                .clone_from_slice(&bytes);
            // save the new record's slot id
            let new_slotid = self.header.vecOfRecords.len() - 1;
            // return the new slot id for the new record!
            println!(
                "(add value func) ptr to beg of free space: {}",
                self.header.ptrBegofFreeSpace
            );
            println!(
                "(add value func) ptr to end of free space: {}",
                self.header.ptrEndofFreeSpace
            );
            Some(new_slotid.try_into().unwrap())
        } else {
            None
        }
    }

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        let mut bytesVec = Vec::new();
        for i in 0..self.header.vecOfRecords.len() {
            if i == slot_id as usize {
                if self.header.vecOfRecords[i].is_deleted == 1 {
                    return None;
                } else {
                    let beg = self.header.vecOfRecords[i].beg_location;
                    let end = self.header.vecOfRecords[i].end_location;
                    for byte in beg..end {
                        bytesVec.push(self.data[byte as usize]);
                    }
                    return Some(bytesVec);
                }
            }
        }
        return None;
        /*
        // create a vector to store the record's byte information
        let mut bytesVec = Vec::new();
        // create flag to signal if slotID is occupied/valid
        let slotid_is_valid = 0;
        // if slot_id > the largest index in the vector, then slot_id doesnt exist
        println!("(get_value) slot_id value: {}", slot_id);
        println!(
            "(get value) len of vec of records {}",
            self.header.vecOfRecords.len()
        );
        println!("(get value1) bytesVec {:?}", bytesVec);
        if slot_id > (self.header.vecOfRecords.len() - 1) as u16 {
            println!("(get_value1) slot_id value: {}", slot_id);
            println!(
                "(get value1) len of vec of records {}",
                self.header.vecOfRecords.len()
            );
            return None;
        }
        println!("(get value2) bytesVec {:?}", bytesVec);
        // check if slot_id is valid by going to the corresponding index in the vec
        if self.header.vecOfRecords[slot_id as usize].is_deleted == 1 {
            // if record at index slot_id was deleted, then return None
            return None;
        } else {
            // else, return the bytes for the record with that slot_id
            let record = &self.header.vecOfRecords[slot_id as usize];
            let recordBegLoc = record.beg_location;
            let recordEndLoc = record.end_location;
            // copy each bit from page into vector
            for byte in recordBegLoc..recordEndLoc {
                bytesVec.push(self.data[byte as usize]);
            }
        } */
        //println!("(get value2) bytesVec {:?}", bytesVec);
        //Some(bytesVec)
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// HINT: Return Some(()) for a valid delete
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        // check if slot_id exists in vector of records
        println!(
            "(delete function) ptr to end of free space: {}",
            self.header.ptrEndofFreeSpace
        );
        println!(
            "(delete function) ptr to beg of free space: {}",
            self.header.ptrBegofFreeSpace
        );
        if slot_id > (self.header.vecOfRecords.len() - 1) as u16 {
            return None;
        };
        // check if record with slot_id is not already deleted
        if self.header.vecOfRecords[slot_id as usize].is_deleted == 1 {
            return None;
        };
        // if record is not already deleted, then delete it
        let deleted_record = &mut self.header.vecOfRecords[slot_id as usize];
        deleted_record.is_deleted = 1;
        // now fill in the gap caused by the deleted record by shifting all records
        // with a higher slot_id forward
        let deleted_record_length = deleted_record.end_location - deleted_record.beg_location;
        for i in (slot_id + 1) as usize..self.header.vecOfRecords.len() {
            // get the records after the one I've just deleted
            let rec = &mut self.header.vecOfRecords[i];
            // now shift the actual data for that record in the page down, too
            for byte in rec.beg_location..rec.end_location {
                self.data[(byte + deleted_record_length) as usize] = self.data[byte as usize];
            }
            // shift the beginning and end locations of the record stored in header down
            rec.end_location += deleted_record_length;
            rec.beg_location += deleted_record_length;
            // then update the end of free space
            self.header.ptrEndofFreeSpace += deleted_record_length;
        }
        println!(
            "(delete function) ptr to end of free space: {}",
            self.header.ptrEndofFreeSpace
        );
        println!(
            "(delete function) ptr to beg of free space: {}",
            self.header.ptrBegofFreeSpace
        );
        Some(())
    }

    /// Create a new page from the byte array.
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    pub fn from_bytes(data: &[u8]) -> Self {
        // first deserialize all of the fixed-length data I have in my header
        let deserialized_ptrEndOfFreeSpace = u16::from_le_bytes(data[0..2].try_into().unwrap());
        println!(
            "(from bytes1) deserialized ptr to end of free space {}",
            deserialized_ptrEndOfFreeSpace
        );
        let deserialized_ptrBegOfFreeSpace = u16::from_le_bytes(data[2..4].try_into().unwrap());
        //let deserialized_ptrBegOfFreeSpace = 22;
        println!(
            "(from bytes1) deserialized ptr to beg of free space {}",
            deserialized_ptrBegOfFreeSpace
        );
        let deserialized_pageID = u16::from_le_bytes(data[4..6].try_into().unwrap());
        // now extract the records from vector of bytes
        let deserialized_vec_of_records = Page::return_deserialized_vec_of_records(data);
        println!(
            "length of deserialized vector of records: {}",
            deserialized_vec_of_records.len()
        );
        // create data vector for page from given array of bytes
        let mut dataForDeserializedPage = [0; PAGE_SIZE];
        // fill in the data vector for page
        for byte in deserialized_ptrEndOfFreeSpace..PAGE_SIZE.try_into().unwrap() {
            dataForDeserializedPage[byte as usize] = data[byte as usize];
        }
        let deserialized_ptrEndOfFreeSpace = u16::from_le_bytes(data[0..2].try_into().unwrap());
        println!(
            "(from bytes2) deserialized ptr to end of free space {}",
            deserialized_ptrEndOfFreeSpace
        );
        let deserialized_ptrBegOfFreeSpace = u16::from_le_bytes(data[2..4].try_into().unwrap());
        //let deserialized_ptrBegOfFreeSpace = 22;
        println!(
            "(from bytes2) deserialized ptr to beg of free space {}",
            deserialized_ptrBegOfFreeSpace
        );
        // create the deserialized header
        let deserializedHeader = Header {
            ptrEndofFreeSpace: deserialized_ptrEndOfFreeSpace,
            ptrBegofFreeSpace: deserialized_ptrBegOfFreeSpace,
            PageID: deserialized_pageID,
            vecOfRecords: deserialized_vec_of_records,
        };
        // create the deserialized page
        let deserializedPage = Page {
            data: dataForDeserializedPage,
            header: deserializedHeader,
        };
        let deserialized_ptrEndOfFreeSpace = u16::from_le_bytes(data[0..2].try_into().unwrap());
        println!(
            "(from bytes3) deserialized ptr to end of free space {}",
            deserialized_ptrEndOfFreeSpace
        );
        let deserialized_ptrBegOfFreeSpace = u16::from_le_bytes(data[2..4].try_into().unwrap());
        //let deserialized_ptrBegOfFreeSpace = 22;
        println!(
            "(from bytes3) deserialized ptr to beg of free space {}",
            deserialized_ptrBegOfFreeSpace
        );
        // return deserialized page
        deserializedPage
        // panic!("TODO milestone pg");
    }

    /// Convert a page into bytes. This must be same size as PAGE_SIZE.
    /// We use a Vec<u8> for simplicity here.
    ///
    /// HINT: To convert a vec of bytes using little endian, use
    /// to_le_bytes().to_vec()
    pub fn get_bytes(&self) -> Vec<u8> {
        // serialize the fixed-length components of the header
        println!(
            "(get bytes1) deserialized ptr to end of free space {}",
            self.header.ptrBegofFreeSpace
        );
        println!(
            "(get bytes1) deserialized ptr to beg of free space {}",
            self.header.ptrEndofFreeSpace
        );
        let mut serialized_ptrEndofFreeSpace = self.header.ptrEndofFreeSpace.to_le_bytes();
        let mut serialized_ptrBegofFreeSpace = self.header.ptrBegofFreeSpace.to_le_bytes();
        let mut serialized_PageID = self.header.PageID.to_le_bytes();
        // append elements to vector
        let mut finalVec = [0; PAGE_SIZE];
        finalVec[0..2].clone_from_slice(&serialized_ptrEndofFreeSpace);
        finalVec[2..4].clone_from_slice(&serialized_ptrBegofFreeSpace);
        finalVec[4..6].clone_from_slice(&serialized_PageID);
        // iterate records, serialize each component in the record and directly append to finalVec
        let mut count = 6;
        for i in 0..self.header.vecOfRecords.len() {
            let mut serialized_beg_location =
                self.header.vecOfRecords[i].beg_location.to_le_bytes();
            let mut serialized_end_location =
                self.header.vecOfRecords[i].end_location.to_le_bytes();
            let mut serialized_is_deleted = self.header.vecOfRecords[i].is_deleted;
            finalVec[count..count + 2].clone_from_slice(&serialized_beg_location);
            finalVec[count + 2..count + 4].clone_from_slice(&serialized_end_location);
            finalVec[count + 4] = serialized_is_deleted;
            count += 5;
        }
        // now clone in the rest of the actual page data from the beginning
        let mut page_data = &self.data[self.header.ptrEndofFreeSpace as usize..PAGE_SIZE];
        finalVec[self.header.ptrEndofFreeSpace as usize..].clone_from_slice(&page_data);
        // return final vector
        finalVec.to_vec()
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_header_size(&self) -> usize {
        let headerSize = 6 + 5 * self.header.vecOfRecords.len();
        headerSize
    }
    /// A utility function to determine the largest block of free space in the page.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_largest_free_contiguous_space(&self) -> usize {
        let maxContigSpaceRecords = self.header.ptrEndofFreeSpace as usize - self.get_header_size();
        maxContigSpaceRecords
    }

    /// Utility function that returns a deserialized vector of records (extracted from header)
    pub fn return_deserialized_vec_of_records(data: &[u8]) -> Vec<Record> {
        // create a vector of records to fill
        let mut deserialized_vec_of_records = Vec::new();
        // deserialize components in the header
        let deserialized_ptrEndOfFreeSpace = u16::from_le_bytes(data[0..2].try_into().unwrap());
        let deserialized_ptrBegOfFreeSpace = u16::from_le_bytes(data[2..4].try_into().unwrap());
        let deserialized_pageID = u16::from_le_bytes(data[4..6].try_into().unwrap());
        // deserialize components in the vector
        let mut byte = 6 as usize;
        while byte <= (deserialized_ptrBegOfFreeSpace - 5) as usize {
            let deserialized_beg_location =
                u16::from_le_bytes(data[byte..(byte + 2)].try_into().unwrap());
            let deserialized_end_location =
                u16::from_le_bytes(data[(byte + 2)..(byte + 4)].try_into().unwrap());
            let deserialized_is_deleted = data[byte + 4];
            // create record
            let deserialized_record = Record {
                end_location: deserialized_end_location,
                beg_location: deserialized_beg_location,
                is_deleted: deserialized_is_deleted,
            };
            // push record into vector of records
            deserialized_vec_of_records.push(deserialized_record);
            // go to next record
            byte += 5;
        }
        // return deserialized vector of records
        deserialized_vec_of_records
    }
}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
/// See https://stackoverflow.com/questions/30218886/how-to-implement-iterator-and-intoiterator-for-a-simple-struct
pub struct PageIter {
    page: Page,
    index: usize,
}

/// The implementation of the (consuming) page iterator.
impl Iterator for PageIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self.index {
//TODO

        }
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = Vec<u8>;
    type IntoIter = PageIter;

    fn into_iter(self) -> Self::IntoIter {
        PageIter {
            page: self,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());
        assert_eq!(
            PAGE_SIZE - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_largest_free_contiguous_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }

    #[test]
    fn hs_page_get_first_free_space() {
        init();
        let mut p = Page::new(0);

        let _b1 = get_random_byte_vec(100);
        let _b2 = get_random_byte_vec(50);
    }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3 (goes to slot 1 instead)
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new slot 4(goes to slot 2 instead)
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.get_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let bytes = p.get_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.get_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = p.get_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes3.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x);
        }

        let p = Page::from_bytes(&page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = p.get_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(None, iter.next());
    }
}
