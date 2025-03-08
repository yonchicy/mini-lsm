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

mod leveled;
mod simple_leveled;
mod tiered;

use anyhow::anyhow;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{self, SsTableBuilder};
use crate::table::{SsTable, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match _task {
            CompactionTask::Leveled(leveled_compaction_task) => todo!(),
            CompactionTask::Tiered(tiered_compaction_task) => todo!(),
            CompactionTask::Simple(simple_leveled_compaction_task) => todo!(),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.force_full_compaction_inner(l0_sstables, l1_sstables),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().clone();
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let sstables = self.compact(&task)?;

        let l1_tables: Vec<usize> = sstables.iter().map(|s| s.sst_id()).collect();
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            for sstable in l0_sstables.iter().chain(l1_sstables.iter()) {
                let res = state.sstables.remove(sstable);
                assert!(res.is_some());
            }
            for new_sst in sstables.iter() {
                state.sstables.insert(new_sst.sst_id(), new_sst.clone());
            }
            state.levels[0].1 = l1_tables;
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            assert!(l0_sstables_map.is_empty());
            *self.state.write() = Arc::new(state);
        }
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }
        Ok(())
    }
    fn force_full_compaction_inner(
        &self,
        l0_sstables: &Vec<usize>,
        l1_sstables: &Vec<usize>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut iters = Vec::with_capacity(l0_sstables.len() + l1_sstables.len());

        {
            let guard = self.state.read();
            for l0_sstable in l0_sstables.iter() {
                let sstable = guard
                    .sstables
                    .get(l0_sstable)
                    .ok_or(anyhow!("error find l0 sstable"))?
                    .clone();
                iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                    sstable,
                )?));
            }
            for l1_sstable in l1_sstables.iter() {
                let sstable = guard
                    .sstables
                    .get(l1_sstable)
                    .ok_or(anyhow!("error find l1 sstable"))?
                    .clone();
                iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                    sstable,
                )?));
            }
        };
        let mut merge_iterator = MergeIterator::create(iters);
        let mut table_builder = Some(SsTableBuilder::new(self.options.block_size));
        let mut compacted_tables = Vec::new();
        while merge_iterator.is_valid() {
            if table_builder.is_none() {
                table_builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let mut builder = table_builder.as_mut().unwrap();
            if !merge_iterator.value().is_empty() {
                builder.add(merge_iterator.key(), merge_iterator.value());
            }
            if builder.estimated_size() > self.options.target_sst_size {
                let id = self.next_sst_id();
                let builder = table_builder.take().unwrap();
                compacted_tables.push(Arc::new(builder.build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                )?));
                table_builder.take();
            }
            merge_iterator.next()?;
        }
        if table_builder.is_some() {
            let builder = table_builder.take().unwrap();
            let id = self.next_sst_id();
            compacted_tables.push(Arc::new(builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?));
        }

        Ok(compacted_tables)
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        if snapshot.imm_memtables.len() + 1 > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
