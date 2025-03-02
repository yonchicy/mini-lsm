// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl Block {
    fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        let key_len = buf.get_u16() as usize;
        let key = &buf[..key_len];
        KeyVec::from_vec(key.to_vec())
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.get_first_key(),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }
    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block_iter = BlockIterator::new(block);
        block_iter.seek_to(0);

        block_iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut block_iter = BlockIterator::create_and_seek_to_first(block);
        block_iter.seek_to_key(key);
        block_iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    pub fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }
    pub fn seek_to_offset(&mut self, offset: usize) {
        let block = &self.block;
        let mut entry = &block.data[offset..];

        let key_len = entry.get_u16() as usize;
        let key = entry[..key_len].to_vec();
        self.key = KeyVec::from_vec(key);
        entry.advance(key_len);

        let value_len = entry.get_u16() as usize;
        let value_offset_begin = offset + 2 * std::mem::size_of::<u16>() + key_len;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
    }
    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut l = 0_isize;
        let mut r = (self.block.offsets.len() - 1) as isize;
        while l <= r {
            let mid = (r - l) / 2 + l;
            self.seek_to(mid as usize);

            if self.key() < key {
                l = mid + 1;
            } else {
                r = mid - 1;
            }
        }
        self.seek_to(l as usize);
    }
}
