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

use std::ops::Bound;

use anyhow::{bail, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    pre_key: Vec<u8>,
    is_valid: bool,
    ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>, ts: u64) -> Result<Self> {
        let mut ret = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
            pre_key: Vec::new(),
            ts,
        };
        ret.move_to_next_key()?;
        Ok(ret)
    }
    fn check_valid(&mut self) -> bool {
        if !self.inner.is_valid() {
            self.is_valid = false;
            return false;
        }
        match self.end_bound.as_ref() {
            Bound::Included(b) => self.is_valid = self.inner.key().key_ref() <= b,
            Bound::Excluded(b) => self.is_valid = self.inner.key().key_ref() < b,
            Bound::Unbounded => {}
        }
        self.is_valid
    }
    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        self.check_valid();
        Ok(())
    }
    fn move_to_next_key(&mut self) -> Result<()> {
        loop {
            while self.inner.is_valid() && self.inner.key().key_ref() == self.pre_key {
                self.next_inner()?;
            }
            if !self.check_valid() {
                break;
            }
            self.pre_key.clear();
            self.pre_key.extend(self.inner.key().key_ref());

            while self.inner.is_valid()
                && self.inner.key().key_ref() == self.pre_key
                && self.inner.key().ts() > self.ts
            {
                self.next_inner()?;
            }
            if !self.check_valid() {
                break;
            }
            if self.inner.key().key_ref() != self.pre_key {
                continue;
            }

            if !self.inner.value().is_empty() {
                break;
            }
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_next_key()?;
        Ok(())
    }
    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("should not be called");
        }

        if self.iter.is_valid() {
            if let e @ Err(_) = self.iter.next() {
                self.has_errored = true;
                return e;
            }
        }
        Ok(())
    }
    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
