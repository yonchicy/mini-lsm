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

use std::{ptr::addr_eq, sync::Arc, thread::current};

use anyhow::{anyhow, Ok, Result};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    fn check_sst_valid(sstables: &[Arc<SsTable>]) {
        for sst in sstables {
            assert!(sst.first_key() <= sst.last_key());
        }
        if !sstables.is_empty() {
            for i in 0..(sstables.len() - 1) {
                assert!(sstables[i].last_key() < sstables[i + 1].first_key());
            }
        }
    }
    fn move_until_valid(&mut self) -> Result<()> {
        loop {
            if let Some(iter) = self.current.as_mut() {
                if iter.is_valid() {
                    break;
                }
                if self.next_sst_idx >= self.sstables.len() {
                    self.current = None;
                } else {
                    self.current = Some(SsTableIterator::create_and_seek_to_first(
                        self.sstables[self.next_sst_idx].clone(),
                    )?);
                    self.next_sst_idx += 1;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let first = sstables
            .first()
            .ok_or(anyhow::anyhow!("Empty sstables"))?
            .clone();
        let current = SsTableIterator::create_and_seek_to_first(first)?;
        let mut iter = Self {
            current: Some(current),
            next_sst_idx: 1,
            sstables,
        };
        iter.move_until_valid()?;
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        Self::check_sst_valid(&sstables);
        let mut next_sst_idx = 0;
        let idx = sstables
            .partition_point(|x| x.first_key().raw_ref() <= key.raw_ref())
            .saturating_sub(1);
        if idx >= sstables.len() {
            Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            })
        } else {
            let current = SsTableIterator::create_and_seek_to_key(sstables[idx].clone(), key)?;
            let mut iter = Ok(Self {
                current: Some(current),
                next_sst_idx: idx + 1,
                sstables,
            })?;
            iter.move_until_valid()?;
            Ok(iter)
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(ref current) = self.current {
            current.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        self.move_until_valid()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
