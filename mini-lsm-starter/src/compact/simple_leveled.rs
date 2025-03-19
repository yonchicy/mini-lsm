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

use std::{collections::HashSet, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator,
        merge_iterator::MergeIterator,
        two_merge_iterator::{self, TwoMergeIterator},
    },
    lsm_storage::LsmStorageState,
    table::{SsTable, SsTableIterator},
};

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!(
                "compaction triggered at level 0 because L0 has {} SSTs >= {}",
                _snapshot.l0_sstables.len(),
                self.options.level0_file_num_compaction_trigger
            );
            let lower_level = 1;
            let upper_level_sst_ids = _snapshot.l0_sstables.clone();
            let lower_level_sst_ids = _snapshot.levels.get(lower_level - 1).unwrap().clone();
            assert!(lower_level_sst_ids.0 == lower_level);
            let is_lower_level_bottom_level = false;
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids: lower_level_sst_ids.1.clone(),
                is_lower_level_bottom_level,
            });
        }
        for i in 1..self.options.max_levels {
            let level = i;
            let upper_level_len = _snapshot.levels.get(level - 1).unwrap().1.len();
            let low_level_len = _snapshot.levels.get(level).unwrap().1.len();
            let size_ratio = low_level_len as f64 / upper_level_len as f64;
            if size_ratio < self.options.size_ratio_percent as f64 / 100.0 {
                println!(
                    "compaction triggered at level {} and {} with size ratio {}",
                    i,
                    level + 1,
                    size_ratio
                );
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(level),
                    upper_level_sst_ids: _snapshot.levels.get(level - 1).unwrap().1.clone(),
                    lower_level: level + 1,
                    lower_level_sst_ids: _snapshot.levels.get(level).unwrap().1.clone(),
                    is_lower_level_bottom_level: level + 1 == self.options.max_levels,
                });
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        // let lower_tables:Vec<Arc<SsTable>>  = _task.lower_level_sst_ids.iter().map(|x| _snapshot.sstables.get(x).unwrap().clone()).collect() ;
        // let upper_tables :Vec<Arc<SsTable>>  = _task.upper_level_sst_ids.iter().map(|x| _snapshot.sstables.get(x).unwrap().clone()).collect() ;

        // let merge_sst = |two_merge_iterator:| {

        // }

        // let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_tables)?;
        // if let Some(upper_level) = _task.upper_level {
        //     let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_tables)?;
        //     let two_merge_iterator = TwoMergeIterator::create(upper_iter, lower_iter)?;
        // } else {
        //     let iters = upper_tables.iter().map(|x| Box::new(SsTableIterator::create_and_seek_to_first(x.clone())?)).collect::<Vec<_>>();
        //     let upper_iter = MergeIterator::create(iters);
        //     let two_merge_iterator = TwoMergeIterator::create(upper_iter, lower_iter)?;
        // }

        let mut dels = Vec::new();
        let mut snapshot = _snapshot.clone();
        if let Some(upper_level) = _task.upper_level {
            assert_eq!(
                _task.upper_level_sst_ids,
                snapshot.levels.get(upper_level - 1).unwrap().1,
                "sst mismatch"
            );
            dels.extend(&_task.upper_level_sst_ids);
            snapshot.levels[upper_level - 1].1.clear();
        } else {
            let mut upper_level_tables = _task
                .upper_level_sst_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            println!("{:?}", upper_level_tables);
            snapshot.l0_sstables = snapshot
                .l0_sstables
                .iter()
                .filter(|x| !upper_level_tables.remove(x))
                .copied()
                .collect::<Vec<_>>();
            assert!(upper_level_tables.is_empty());
            dels.extend(&_task.upper_level_sst_ids);
        }
        assert_eq!(
            _task.lower_level_sst_ids,
            snapshot.levels[_task.lower_level - 1].1,
            "sst mismatch"
        );
        dels.extend(&_task.lower_level_sst_ids);
        snapshot.levels[_task.lower_level - 1].1 = _output.to_vec();
        (snapshot, dels)
    }
}
