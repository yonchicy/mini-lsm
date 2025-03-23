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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            _snapshot.l0_sstables.is_empty(),
            "tiered compaction should not have l0 sstables"
        );
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let mut upper_levels_size = 0;
        for level in 0..(_snapshot.levels.len() - 1) {
            upper_levels_size += _snapshot.levels[level].1.len();
        }
        let space_amp_ratio =
            upper_levels_size as f64 / _snapshot.levels.last().unwrap().1.len() as f64;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 / 100.0 {
            println!(
                "compaction triggered by space_amp_ratio {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        // size ratio
        let mut previous_tiers_size = 0;
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        for id in 0..(_snapshot.levels.len() - 1) {
            previous_tiers_size += _snapshot.levels[id].1.len();
            let nxt_level_size = _snapshot.levels[id + 1].1.len();
            let size_ratio = nxt_level_size as f64 / previous_tiers_size as f64;
            if size_ratio > size_ratio_trigger && id + 1 >= self.options.min_merge_width {
                println!(
                    "compaction triggered by size_ratio {} at level {}",
                    size_ratio, id
                );
                return Some(TieredCompactionTask {
                    tiers: _snapshot.levels.iter().take(id + 1).cloned().collect(),
                    bottom_tier_included: id + 1 >= _snapshot.levels.len(),
                });
            }
        }
        let num_to_reduce = _snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        println!(
            "compaction triggered by reducing tiers num {}",
            num_to_reduce
        );

        return Some(TieredCompactionTask {
            tiers: _snapshot
                .levels
                .iter()
                .take(num_to_reduce)
                .cloned()
                .collect(),
            bottom_tier_included: num_to_reduce >= _snapshot.levels.len(),
        });
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            _snapshot.l0_sstables.is_empty(),
            "tiered compaction should not have l0 sstables"
        );
        let mut snapshot = _snapshot.clone();
        let mut tiers_to_remove = _task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut file_to_remove = Vec::new();
        let mut had_added_new_tables = false;
        for (tier_id, tier_tables) in &_snapshot.levels {
            if let Some(ttables) = tiers_to_remove.remove(tier_id) {
                assert_eq!(ttables, tier_tables, "error tables");
                file_to_remove.extend(ttables.iter());
            } else {
                levels.push((*tier_id, tier_tables.clone()));
            }
            if tiers_to_remove.is_empty() && !had_added_new_tables {
                had_added_new_tables = true;
                levels.push((_output[0], _output.to_vec()));
            }
        }
        if !tiers_to_remove.is_empty() {
            unreachable!("some tier is not removed");
        }
        snapshot.levels = levels;
        (snapshot, file_to_remove)
    }
}
