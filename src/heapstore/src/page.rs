use common::ids::{PageId, SlotId};
use common::PAGE_SIZE;
//use core::num;
//use std::cmp;
//use std::collections::HashMap;
use std::fmt;
use std::fmt::Write;

// Type to hold any value smaller than the size of a page.
// We choose u16 because it is sufficient to represent any slot that fits in a 4096-byte-sized page.
// Note that you will need to cast Offset to usize if you want to use it to index an array.
pub type Offset = u16;

// For debug
const BYTES_PER_LINE: usize = 40;

/// Page struct. This must occupy not more than PAGE_SIZE when serialized.
/// In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// If you delete a value, you do not need reclaim header space the way you must reclaim page
/// body space. E.g., if you insert 3 values then delete 2 of them, your header can remain 26
/// bytes & subsequent inserts can simply add 6 more bytes to the header as normal.
/// The rest must filled as much as possible to hold values.
pub(crate) struct Page {
    data: [u8; PAGE_SIZE],
}

/// The functions required for page
impl Page {
    /// Create a new page
    #[allow(dead_code)]
    pub fn new(page_id: PageId) -> Self {
        //header format (first 8 bytes): page id (2 bytes), num slots (2 bytes)
        let mut page: Page = Page {
            data: [0; PAGE_SIZE],
        };
        let bytes: [u8; 2] = page_id.to_be_bytes();
        page.data[0] = bytes[0];
        page.data[1] = bytes[1];
        page
    }

    /// Return the page id for a page, which is first two bytes of page.data
    #[allow(dead_code)]
    pub fn get_page_id(&self) -> PageId {
        let bytes: [u8; 2] = [self.data[0], self.data[1]];
        let p_id: u16 = u16::from_be_bytes(bytes);
        p_id
    }

    //returns the number of slots in use
    pub fn get_num_slots(&self) -> u16 {
        u16::from_be_bytes([self.data[2], self.data[3]])
    }

    //given an index, returns the slot_id for the corresponding slot
    pub fn get_slot_id(&self, index: usize) -> u16 {
        let slot_id: u16 =
            u16::from_be_bytes([self.data[10 + index * 6], self.data[11 + index * 6]]);
        slot_id
    }

    //given an index, returns the start_index for the corresponding slot
    pub fn get_start_index(&self, index: usize) -> u16 {
        let start_index: u16 =
            u16::from_be_bytes([self.data[8 + index * 6], self.data[9 + index * 6]]);
        start_index
    }

    //given an index, returns the size of bytes for the corresponding data associated with the slot
    pub fn get_size_bytes(&self, index: usize) -> u16 {
        let size_bytes: u16 =
            u16::from_be_bytes([self.data[12 + index * 6], self.data[13 + index * 6]]);
        size_bytes
    }

    //gets the size of all the bytes data in a page
    pub fn get_all_bytes_size(&self) -> u16 {
        let mut all_data_size: u16 = 0;
        for i in 0..self.get_num_slots() {
            all_data_size += self.get_size_bytes(i as usize);
        }
        all_data_size
    }

    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are storedd in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should reuse the slotId in the future.
    /// The page should always assign the lowest available slot_id to an insertion.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);
    #[allow(dead_code)]
    pub fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
        //six bytes in header formatted as: start index (2 bytes), slot id (2 bytes), size of bytes (2 bytes)
        let mut found_slotid: bool = false;
        let mut slot_id: u16 = 0;
        let mut header_index: u16 = 0;
        if self.get_free_space() < bytes.len() + 6 {
            None
        } else {
            //increments through all header slots to see if there is a slot id not in use
            for i in 0..self.get_num_slots() {
                let ind: usize = i as usize;
                if self.get_size_bytes(ind) == 0 && !found_slotid {
                    slot_id = self.get_slot_id(ind);
                    header_index = 8 + i * 6;
                    found_slotid = true;
                    break;
                }
            }

            let start_index: u16 = (PAGE_SIZE as u16) - self.get_all_bytes_size();

            //if all slot ids in use, creates new slot id
            if !found_slotid {
                slot_id = self.get_num_slots();
                header_index = 8 + slot_id * 6;
                let store_new_num_slots: [u8; 2] = (slot_id + 1).to_be_bytes();
                self.data[2] = store_new_num_slots[0];
                self.data[3] = store_new_num_slots[1];
            }

            //inserts header information and bytes data into page
            let store_start_index: [u8; 2] = start_index.to_be_bytes();
            let store_slot_id: [u8; 2] = slot_id.to_be_bytes();
            let store_size_of_bytes: [u8; 2] = (bytes.len() as u16).to_be_bytes();
            self.data[header_index as usize] = store_start_index[0];
            self.data[header_index as usize + 1] = store_start_index[1];
            self.data[header_index as usize + 2] = store_slot_id[0];
            self.data[header_index as usize + 3] = store_slot_id[1];
            self.data[header_index as usize + 4] = store_size_of_bytes[0];
            self.data[header_index as usize + 5] = store_size_of_bytes[1];
            let s: usize = start_index as usize;
            let e: usize = (start_index - bytes.len() as u16) as usize;
            self.data[e..s].clone_from_slice(bytes);
            Some(slot_id)
        }
    }

    /// Return the bytes for the slotId. If the slotId is not valid then return None
    #[allow(dead_code)]
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        for i in 0..self.get_num_slots() {
            let ind: usize = i as usize;
            let cur_start_index: u16 = self.get_start_index(ind);
            let cur_size_data: u16 = self.get_size_bytes(ind);
            if self.get_slot_id(ind) == slot_id && cur_size_data > 0 {
                let mut val: Vec<u8> = Vec::new();
                for j in (cur_start_index - cur_size_data) as usize..cur_start_index as usize {
                    val.push(self.data[j]);
                }
                return Some(val);
            }
        }
        None
    }

    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    #[allow(dead_code)]
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        let mut size_slot_id: u16 = 0;
        let mut start_index: u16 = 0;
        let mut found: bool = false;

        //Sees if slot id exists in page
        let mut order_slots: Vec<[u16; 3]> = Vec::new();
        for i in 0..self.get_num_slots() {
            let ind: usize = i as usize;
            if slot_id == self.get_slot_id(ind) {
                start_index = self.get_start_index(ind);
                size_slot_id = self.get_size_bytes(ind);
                self.data[12 + ind * 6] = 0;
                self.data[13 + ind * 6] = 0;
                found = true;
            }
        }

        //if slot id doesn't exist, return None
        if !found {
            return None;
        }

        //inserts elements into the vector order_slots, to know the order in which to compact and shift data to the left
        //order_slots is a list of elements, where the elemenst = [start index, size of bytes data, header index]
        for i in 0..self.get_num_slots() {
            let ind: usize = i as usize;
            let working_start_index: u16 = self.get_start_index(ind);
            let working_header_index: u16 = 8 + i * 6;
            if working_start_index < start_index {
                order_slots.push([
                    working_start_index,
                    self.get_size_bytes(ind),
                    working_header_index,
                ]);
            }
        }
        order_slots.sort_unstable_by_key(|&x| std::cmp::Reverse(x[0]));

        //To compact, updates all the headers, and shifts all the bytes associated with header to left
        for slot in order_slots {
            let start: usize = (slot[0] - slot[1]) as usize;
            for j in (start..slot[0] as usize).rev() {
                self.data[j + (size_slot_id as usize)] = self.data[j];
            }
            let store_start_index: [u8; 2] = (slot[0] + size_slot_id).to_be_bytes();
            self.data[slot[2] as usize] = store_start_index[0];
            self.data[slot[2] as usize + 1] = store_start_index[1];
        }

        Some(())
    }

    /// Deserialize bytes into Page
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    #[allow(dead_code)]
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut page: Page = Page {
            data: [0; PAGE_SIZE],
        };
        page.data.clone_from_slice(data);
        page
    }

    /// Serialize page into a byte array. This must be same size as PAGE_SIZE.
    /// We use a Vec<u8> for simplicity here.
    ///
    /// HINT: To convert a vec of bytes using little endian, use
    /// to_le_bytes().to_vec()
    /// HINT: Do not use the self debug ({:?}) in this function, which calls this function.
    #[allow(dead_code)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![0; PAGE_SIZE];
        v[0..].clone_from_slice(&self.data);
        v
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_header_size(&self) -> usize {
        let c: usize = 8 + 6 * (self.get_num_slots() as usize);
        c
    }

    /// A utility function to determine the total current free space in the page.
    /// This should account for the header space used and space that could be reclaimed if needed.
    /// Will be used by tests. Optional for you to use in your code, but strongly suggested
    #[allow(dead_code)]
    pub(crate) fn get_free_space(&self) -> usize {
        PAGE_SIZE - (self.get_header_size() + self.get_all_bytes_size() as usize)
    }

    /// Utility function for comparing the bytes of another page.
    /// Returns a vec  of Offset and byte diff
    #[allow(dead_code)]
    pub fn compare_page(&self, other_page: Vec<u8>) -> Vec<(Offset, Vec<u8>)> {
        let mut res = Vec::new();
        let bytes = self.to_bytes();
        assert_eq!(bytes.len(), other_page.len());
        let mut in_diff = false;
        let mut diff_start = 0;
        let mut diff_vec: Vec<u8> = Vec::new();
        for (i, (b1, b2)) in bytes.iter().zip(&other_page).enumerate() {
            if b1 != b2 {
                if !in_diff {
                    diff_start = i;
                    in_diff = true;
                }
                diff_vec.push(*b1);
            } else if in_diff {
                //end the diff
                res.push((diff_start as Offset, diff_vec.clone()));
                diff_vec.clear();
                in_diff = false;
            }
        }
        res
    }
}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
pub struct PageIntoIter {
    data: Page,
    pub index: usize,
}

/// The implementation of the (consuming) page iterator.
/// This should return the values in slotId order (ascending)
impl Iterator for PageIntoIter {
    // Each item returned by the iterator is the bytes for the value and the slot id.
    type Item = (Vec<u8>, SlotId);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= (self.data.get_num_slots() as usize) {
            None
        } else {
            let size_data: u16 = self.data.get_size_bytes(self.index);

            if size_data == 0 {
                //if slot not in use, go to next slot
                self.index += 1;
                self.next()
            } else {
                //if slot is in use, returns the byets data and slot id
                let next_size_data: u16 = self.data.get_size_bytes(self.index + 1);
                let start_index: u16 = self.data.get_start_index(self.index);
                let slot_id: u16 = self.data.get_slot_id(self.index);
                let mut v: Vec<u8> = Vec::new();
                for j in (start_index - size_data) as usize..start_index as usize {
                    v.push(self.data.data[j]);
                }
                if next_size_data > 0 {
                    self.index += 1;
                } else {
                    self.index += 2;
                }
                Some((v, slot_id))
            }
        }
    }
}

/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = (Vec<u8>, SlotId);
    type IntoIter = PageIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        PageIntoIter {
            data: self,
            index: 0,
        }
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //let bytes: &[u8] = unsafe { any_as_u8_slice(&self) };
        let p = self.to_bytes();
        let mut buffer = String::new();
        let len_bytes = p.len();

        // If you want to add a special header debugger to appear before the bytes add it here
        // buffer += "Header: \n";

        let mut pos = 0;
        let mut remaining;
        let mut empty_lines_count = 0;
        let comp = [0; BYTES_PER_LINE];
        //hide the empty lines
        while pos < len_bytes {
            remaining = len_bytes - pos;
            if remaining > BYTES_PER_LINE {
                let pv = &(p)[pos..pos + BYTES_PER_LINE];
                if pv.eq(&comp) {
                    empty_lines_count += 1;
                    pos += BYTES_PER_LINE;
                    continue;
                }
                if empty_lines_count != 0 {
                    write!(&mut buffer, "{} ", empty_lines_count).unwrap();
                    buffer += "empty lines were hidden\n";
                    empty_lines_count = 0;
                }
                // for hex offset
                write!(&mut buffer, "[{:4}] ", pos).unwrap();
                #[allow(clippy::needless_range_loop)]
                for i in 0..BYTES_PER_LINE {
                    match pv[i] {
                        0x00 => buffer += ".  ",
                        0xff => buffer += "## ",
                        _ => write!(&mut buffer, "{:02x} ", pv[i]).unwrap(),
                    };
                }
            } else {
                let pv = &(p.clone())[pos..pos + remaining];
                if pv.eq(&comp) {
                    empty_lines_count += 1;
                    pos += BYTES_PER_LINE;
                    continue;
                }
                if empty_lines_count != 0 {
                    write!(&mut buffer, "{} ", empty_lines_count).unwrap();
                    buffer += "empty lines were hidden\n";
                    empty_lines_count = 0;
                }
                // for hex offset
                //buffer += &format!("[0x{:08x}] ", pos);
                write!(&mut buffer, "[{:4}] ", pos).unwrap();
                #[allow(clippy::needless_range_loop)]
                for i in 0..remaining {
                    match pv[i] {
                        0x00 => buffer += ".  ",
                        0xff => buffer += "## ",
                        _ => write!(&mut buffer, "{:02x} ", pv[i]).unwrap(),
                    };
                }
            }
            buffer += "\n";
            pos += BYTES_PER_LINE;
        }
        if empty_lines_count != 0 {
            write!(&mut buffer, "{} ", empty_lines_count).unwrap();
            buffer += "empty lines were hidden\n";
        }
        write!(f, "{}", buffer)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;
    use rand::Rng;
    //use log::{debug, error, log_enabled, info, Level};

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());
        assert_eq!(PAGE_SIZE - p.get_header_size(), p.get_free_space());
    }

    #[test]
    fn debug_page_insert() {
        init();
        let mut p = Page::new(0);
        let n = 20;
        let size = 20;
        let vals = get_ascending_vec_of_byte_vec_02x(n, size, size);
        for x in &vals {
            p.add_value(x);
        }

        assert_eq!(
            p.get_free_space(),
            PAGE_SIZE - p.get_header_size() - n * size
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
            p.get_free_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_free_space()
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
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
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
            p.get_free_space()
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
        assert_eq!(PAGE_SIZE - p.get_header_size() - size, p.get_free_space());
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_free_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_free_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_free_space()
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

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.to_bytes();
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
        let bytes = p.to_bytes();
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
        let tuple_bytes3 = tuple3.to_bytes();
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
        let page_bytes = p.to_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes3.clone(), 2)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(None, iter.next());

        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x.0);
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
        let page_bytes = p.to_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some((tuple_bytes.clone(), 0)), iter.next());
        assert_eq!(Some((tuple_bytes2.clone(), 1)), iter.next());
        assert_eq!(Some((tuple_bytes4.clone(), 3)), iter.next());
        assert_eq!(Some((tuple_bytes.clone(), 4)), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_same_size() {
        init();
        let size = 800;
        let values = get_ascending_vec_of_byte_vec_02x(6, size, size);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_larger_size() {
        init();
        let size = 500;
        let values = get_ascending_vec_of_byte_vec_02x(8, size, size);
        let larger_val = get_random_byte_vec(size * 2 - 20);
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(Some(5), p.add_value(&values[5]));
        assert_eq!(Some(6), p.add_value(&values[6]));
        assert_eq!(Some(7), p.add_value(&values[7]));
        assert_eq!(values[5], p.get_value(5).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(()), p.delete_value(6));
        assert_eq!(None, p.get_value(6));
        assert_eq!(Some(1), p.add_value(&larger_val));
        assert_eq!(larger_val, p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_delete_reclaim_smaller_size() {
        init();
        let size = 800;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size / 4),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        assert_eq!(Some(3), p.add_value(&values[3]));
        assert_eq!(Some(4), p.add_value(&values[4]));
        assert_eq!(values[0], p.get_value(0).unwrap());
        assert_eq!(None, p.add_value(&values[0]));
        assert_eq!(Some(()), p.delete_value(1));
        assert_eq!(None, p.get_value(1));
        assert_eq!(Some(1), p.add_value(&values[5]));
        assert_eq!(values[5], p.get_value(1).unwrap());
    }

    #[test]
    pub fn hs_page_test_multi_ser() {
        init();
        let size = 500;
        let values = vec![
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
            get_random_byte_vec(size),
        ];
        let mut p = Page::new(0);
        assert_eq!(Some(0), p.add_value(&values[0]));
        assert_eq!(Some(1), p.add_value(&values[1]));
        assert_eq!(Some(2), p.add_value(&values[2]));
        let bytes = p.to_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(values[0], p2.get_value(0).unwrap());
        assert_eq!(values[1], p2.get_value(1).unwrap());
        assert_eq!(values[2], p2.get_value(2).unwrap());
        assert_eq!(Some(3), p2.add_value(&values[3]));
        assert_eq!(Some(4), p2.add_value(&values[4]));

        let bytes2 = p2.to_bytes();
        let mut p3 = Page::from_bytes(&bytes2);
        assert_eq!(values[0], p3.get_value(0).unwrap());
        assert_eq!(values[1], p3.get_value(1).unwrap());
        assert_eq!(values[2], p3.get_value(2).unwrap());
        assert_eq!(values[3], p3.get_value(3).unwrap());
        assert_eq!(values[4], p3.get_value(4).unwrap());
        assert_eq!(Some(5), p3.add_value(&values[5]));
        assert_eq!(Some(6), p3.add_value(&values[6]));
        assert_eq!(Some(7), p3.add_value(&values[7]));
        assert_eq!(None, p3.add_value(&values[0]));

        let bytes3 = p3.to_bytes();
        let p4 = Page::from_bytes(&bytes3);
        assert_eq!(values[0], p4.get_value(0).unwrap());
        assert_eq!(values[1], p4.get_value(1).unwrap());
        assert_eq!(values[2], p4.get_value(2).unwrap());
        assert_eq!(values[7], p4.get_value(7).unwrap());
    }

    #[test]
    pub fn hs_page_stress_test() {
        //env_logger::init();
        init();
        let mut p = Page::new(23);
        let mut original_vals: VecDeque<Vec<u8>> =
            VecDeque::from_iter(get_ascending_vec_of_byte_vec_02x(300, 20, 100));
        let mut stored_vals: Vec<Vec<u8>> = Vec::new();
        let mut stored_slots: Vec<SlotId> = Vec::new();
        let mut has_space = true;
        let mut rng = rand::thread_rng();

        // Load up page until full
        while has_space {
            let bytes = original_vals
                .pop_front()
                .expect("ran out of data -- shouldn't happen");
            let slot = p.add_value(&bytes);
            match slot {
                Some(slot_id) => {
                    stored_vals.push(bytes);
                    stored_slots.push(slot_id);
                }
                None => {
                    // No space for this record, we are done. go ahead and stop. add back value
                    original_vals.push_front(bytes);
                    has_space = false;
                }
            };
        }

        // let (check_vals, check_slots): (Vec<Vec<u8>>, Vec<SlotId>) = p.into_iter().map(|(a, b)| (a, b)).unzip();
        let bytes = p.to_bytes();
        let p_clone = Page::from_bytes(&bytes);
        let mut check_vals: Vec<Vec<u8>> = p_clone.into_iter().map(|(a, _)| a).collect();
        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
        trace!("\n==================\n PAGE LOADED - now going to delete to make room as needed \n =======================");
        // Delete and add remaining values until goes through all. Should result in a lot of random deletes and adds.
        while !original_vals.is_empty() {
            let bytes = original_vals.pop_front().unwrap();
            trace!("Adding new value (left:{}). Need to make space for new record (len:{}).\n - Stored_slots {:?}", original_vals.len(), &bytes.len(), stored_slots);
            let mut added = false;
            while !added {
                let try_slot = p.add_value(&bytes);
                match try_slot {
                    Some(new_slot) => {
                        stored_slots.push(new_slot);
                        stored_vals.push(bytes.clone());
                        let bytes = p.to_bytes();
                        let p_clone = Page::from_bytes(&bytes);
                        check_vals = p_clone.into_iter().map(|(a, _)| a).collect();
                        assert!(compare_unordered_byte_vecs(&stored_vals, check_vals));
                        trace!("Added new value ({}) {:?}", new_slot, stored_slots);
                        added = true;
                    }
                    None => {
                        //Delete a random value and try again
                        let random_idx = rng.gen_range(0..stored_slots.len());
                        trace!(
                            "Deleting a random val to see if that makes enough space {}",
                            stored_slots[random_idx]
                        );
                        let value_id_to_del = stored_slots.remove(random_idx);
                        stored_vals.remove(random_idx);
                        p.delete_value(value_id_to_del)
                            .expect("Error deleting slot_id");
                        trace!("Stored vals left {}", stored_slots.len());
                    }
                }
            }
        }
    }
}
