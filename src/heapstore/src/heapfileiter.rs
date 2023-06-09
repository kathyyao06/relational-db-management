use crate::heapfile::HeapFile;
//use crate::page::{Page, PageIntoIter};
use crate::page::PageIntoIter;
//use common::{prelude::*, PAGE_SIZE};
use common::prelude::*;
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
    index: u16,
    transaction_id: TransactionId,
    iterator: Option<PageIntoIter>,
    first: bool,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        HeapFileIterator {
            heapfile: hf,
            index: 0,
            transaction_id: tid,
            iterator: None,
            first: false,
        }
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.heapfile.num_pages() {
            return None;
        } else if !self.first {
            self.first = true;
            let page = match self.heapfile.read_page_from_file(self.index) {
                Ok(p) => p,
                Err(_) => return None,
            };
            let p_iter: PageIntoIter = page.into_iter();
            self.iterator = Some(p_iter);
        }

        if self.iterator.is_some() {
            let iter = self.iterator.as_mut().unwrap();
            match iter.next() {
                Some((v, slot_id)) => {
                    let value_id = ValueId {
                        container_id: self.heapfile.container_id,
                        segment_id: None,
                        page_id: Some(self.index),
                        slot_id: Some(slot_id),
                    };
                    Some((v, value_id))
                }
                None => {
                    self.index += 1;
                    self.first = false;
                    self.next()
                }
            }
        } else {
            self.index += 1;
            self.first = false;
            self.next()
        }
    }
}
