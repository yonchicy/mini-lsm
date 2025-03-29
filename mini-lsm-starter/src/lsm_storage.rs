// Copyright (c) 2022-2025 Alex Chi Z
// // Licensed under the Apache License, Version 2.0 (the "License");
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

use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, map_key_bound_plus_ts, MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    pub fn dump_structure(&self) {
        println!("lsm storage state dumping");
        if !self.l0_sstables.is_empty() {
            println!("L0 ({}): {:?}", self.l0_sstables.len(), self.l0_sstables,);
        }
        for (level, files) in &self.levels {
            println!("L{level} ({}): {:?}", files.len(), files)
        }
    }

    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            // NOTE: 为什么这里不用加 state_lock ??? 甚至不用写 manifest
            // self.inner.freeze_memtable_with_memtable(Arc::new(MemTable::create(self.inner.next_sst_id())))?;
            let lock = self.inner.state_lock.lock();
            self.inner.force_freeze_memtable(&lock)?;
        }
        //NOTE: why use while loop ?
        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        println!("open options {:?}", options);
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir(path)?;
        }
        let mut state = LsmStorageState::create(&options);
        // NOTE: 因为自带了一个 id = 0 的  memtable
        let mut next_sst_id = 1;
        let block_cache = Arc::new(BlockCache::new(1024));

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };
        let manifest;
        let manifest_path = path.join("MANIFEST");
        if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            manifest = Manifest::create(&manifest_path).context("failed to create manifest")?;
            //NOTE : 这里其实表明，所有的 state 第一个 manifest 记录都是 flush (0)
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (m, records) = Manifest::recover(&manifest_path)?;
            let mut memtables = BTreeSet::new();

            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        // NOTE: recovery 过程中， next_sst_id 仅仅是拿来记录最大的 sst id.
                        next_sst_id = next_sst_id.max(sst_id); // NOTE: 这个地方选最大的，因为 leveled 可能会出现小的在后面
                    }
                    ManifestRecord::NewMemtable(sst_id) => {
                        next_sst_id = next_sst_id.max(sst_id);
                        memtables.insert(sst_id);
                    }
                    ManifestRecord::Compaction(compaction_task, output) => {
                        let (new_state, _) = compaction_controller.apply_compaction_result(
                            &state,
                            &compaction_task,
                            &output,
                            true,
                        );
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }
            // NOTE: 打开所有文件
            let mut sst_cnt = 0;
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let table_id = *table_id;
                let sst = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, table_id))
                        .with_context(|| format!("failed to open SST: {}", table_id))?,
                )?;
                state.sstables.insert(table_id, Arc::new(sst));
                sst_cnt += 1;
            }
            println!("{} SSTs opened", sst_cnt);
            next_sst_id += 1;

            // Sort SSTs on each level (only for leveled compaction)
            // NOTE: 打开文件后，才能获取到表的 sst first key ，才能排序
            if let CompactionController::Leveled(_) = &compaction_controller {
                for (_id, ssts) in &mut state.levels {
                    ssts.sort_by(|x, y| {
                        state
                            .sstables
                            .get(x)
                            .unwrap()
                            .first_key()
                            .cmp(state.sstables.get(y).unwrap().first_key())
                    })
                }
            }
            if options.enable_wal {
                let mut wal_cnt = 0;
                for id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_cnt += 1;
                    }
                }
                println!("{} WALs recovered", wal_cnt);
                // NOTE: 现在 state 里的 memtable 都是用过了的，所以要新建一个。
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                // NOTE: 现在 state 里的 memtable 都是用过了的，所以要新建一个。
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }

            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
            manifest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        if let Some(v) = snapshot
            .memtable
            .get(KeySlice::from_slice(_key, TS_DEFAULT))
        {
            if !v.is_empty() {
                return Ok(Some(v));
            } else {
                return Ok(None);
            }
        }
        for old_memtable in snapshot.imm_memtables.iter() {
            if let Some(v) = old_memtable.get(KeySlice::from_slice(_key, TS_DEFAULT)) {
                if !v.is_empty() {
                    return Ok(Some(v));
                } else {
                    return Ok(None);
                }
            }
        }
        let keep_table = |key: &[u8], table: &SsTable| {
            if Self::key_within(key, table.first_key(), table.last_key()) {
                if let Some(bloom) = &table.bloom {
                    if bloom.may_contain(farmhash::fingerprint32(key)) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        };

        let mut sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_table in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables.get(sst_table).unwrap().clone();
            if keep_table(_key, &sst) {
                let sst_iterator = SsTableIterator::create_and_seek_to_key(
                    sst,
                    KeySlice::from_slice(_key, TS_DEFAULT),
                )?;
                sst_iters.push(Box::new(sst_iterator));
            }
        }
        let l0_iterator = MergeIterator::create(sst_iters);
        let mut levels_concat_iters = Vec::new();
        for (_, level_tables) in snapshot.levels.iter() {
            let mut ssts = Vec::new();
            for sst_id in level_tables {
                let table = snapshot.sstables.get(sst_id).unwrap().clone();
                if keep_table(_key, &table) {
                    ssts.push(table);
                }
            }
            let iter = SstConcatIterator::create_and_seek_to_key(
                ssts,
                KeySlice::from_slice(_key, TS_DEFAULT),
            )?;
            levels_concat_iters.push(Box::new(iter));
        }

        let iter =
            TwoMergeIterator::create(l0_iterator, MergeIterator::create(levels_concat_iters))?;

        // FIXME:  目前只匹配了 key ， 没匹配 timestap
        if iter.is_valid() && iter.key().key_ref() == _key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in _batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let _key = key.as_ref();
                    let _value = value.as_ref();
                    let size = {
                        let state = self.state.read();
                        state
                            .memtable
                            .put(KeySlice::from_slice(_key, TS_DEFAULT), _value)?;
                        state.memtable.approximate_size()
                    };
                    if size > self.options.target_sst_size {
                        let lock = self.state_lock.lock();
                        let state = self.state.read();
                        if state.memtable.approximate_size() > self.options.target_sst_size {
                            drop(state);
                            self.force_freeze_memtable(&lock)?;
                        }
                    }
                }
                WriteBatchRecord::Del(key) => {
                    let _key = key.as_ref();
                    let state = self.state.read();
                    state
                        .memtable
                        .put(KeySlice::from_slice(_key, TS_DEFAULT), b"")?;
                    if state.memtable.approximate_size() > self.options.target_sst_size {
                        let lock = self.state_lock.lock();
                        if state.memtable.approximate_size() > self.options.target_sst_size {
                            drop(state);
                            self.force_freeze_memtable(&lock)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let record = WriteBatchRecord::Put(_key, _value);
        self.write_batch(&[record])?;
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let record = WriteBatchRecord::Del(_key);
        self.write_batch(&[record])?;
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }
    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        // Swap the current memtable with a new one.
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // Add the memtable to the immutable memtables.
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        let snapshot_id = snapshot
            .imm_memtables
            .iter()
            .map(|x| x.id())
            .collect::<Vec<_>>();
        println!(
            "after freezing mem table ,snapshot imm_memtables {:?}",
            snapshot_id
        );
        // Update the snapshot.
        *guard = Arc::new(snapshot);

        drop(guard);
        println!("freezing memtable to wal with id {}", old_memtable.id());
        old_memtable.sync_wal()?;

        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        self.freeze_memtable_with_memtable(memtable)?;

        // NOTE:
        self.manifest.as_ref().unwrap().add_record(
            _state_lock_observer,
            ManifestRecord::NewMemtable(memtable_id),
        )?;
        self.sync_dir()?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        let imm_memtable = {
            let guard = self.state.read();
            guard
                .imm_memtables
                .last()
                .expect("no imm_memtables")
                .clone()
        };
        let mut sst_table_builder = SsTableBuilder::new(self.options.block_size);
        imm_memtable.flush(&mut sst_table_builder)?;

        let sst_id = imm_memtable.id();
        let sst = Arc::new(sst_table_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot.imm_memtables.pop();
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            println!("flushed {}.sst with size={}", sst_id, sst.table_size());
            snapshot.sstables.insert(sst_id, sst);
            *guard = Arc::new(snapshot);
        }
        if self.options.enable_wal {
            std::fs::remove_file(self.path_of_wal(sst_id))?;
        }

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&_state_lock, ManifestRecord::Flush(sst_id))?;
        self.sync_dir()?;
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }
    fn key_within(user_key: &[u8], table_begin: &KeyBytes, table_end: &KeyBytes) -> bool {
        table_begin.key_ref() <= user_key && user_key <= table_end.key_ref()
    }

    fn range_overlap(
        user_begin: Bound<&[u8]>,
        user_end: Bound<&[u8]>,
        table_begin: KeySlice,
        table_end: KeySlice,
    ) -> bool {
        match user_end {
            Bound::Excluded(key) if key <= table_begin.key_ref() => {
                return false;
            }
            Bound::Included(key) if key < table_begin.key_ref() => {
                return false;
            }
            _ => {}
        }
        match user_begin {
            Bound::Excluded(key) if key >= table_end.key_ref() => {
                return false;
            }
            Bound::Included(key) if key > table_end.key_ref() => {
                return false;
            }
            _ => {}
        }
        true
    }
    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut memtable_iters: Vec<Box<MemTableIterator>> =
            Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        let upper = map_key_bound_plus_ts(_upper, TS_DEFAULT);
        let lower = map_key_bound_plus_ts(_lower, TS_DEFAULT);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));

        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }

        let mem_iterator = MergeIterator::create(memtable_iters);

        let mut l0_sstable_iters: Vec<Box<SsTableIterator>> =
            Vec::with_capacity(snapshot.l0_sstables.len());
        for &sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables.get(&sst_id).unwrap().clone();
            if Self::range_overlap(
                _lower,
                _upper,
                sst.first_key().as_key_slice(),
                sst.last_key().as_key_slice(),
            ) {
                let sst_iter = match _lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        sst,
                        KeySlice::from_slice(key, TS_DEFAULT),
                    )?,
                    Bound::Excluded(key) => {
                        let k = KeySlice::from_slice(key, TS_DEFAULT);
                        let mut iter = SsTableIterator::create_and_seek_to_key(sst, k)?;
                        if iter.is_valid() && iter.key() == k {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst)?,
                };

                l0_sstable_iters.push(Box::new(sst_iter));
            }
        }

        let l0_sstable_iter = MergeIterator::create(l0_sstable_iters);
        let mem_l0_iter = TwoMergeIterator::create(mem_iterator, l0_sstable_iter)?;

        let mut levels_concat_iters = Vec::with_capacity(snapshot.levels.len());
        for level in snapshot.levels.iter() {
            let mut ssts = Vec::with_capacity(level.1.len());
            for sst_id in level.1.iter() {
                let table = snapshot.sstables.get(sst_id).unwrap().clone();
                if Self::range_overlap(
                    _lower,
                    _upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    ssts.push(table);
                }
            }
            let level_iter = match _lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    ssts,
                    KeySlice::from_slice(key, TS_DEFAULT),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        ssts,
                        KeySlice::from_slice(key, TS_DEFAULT),
                    )?;
                    if iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts)?,
            };
            levels_concat_iters.push(Box::new(level_iter));
        }

        let lsm_iter_inner =
            TwoMergeIterator::create(mem_l0_iter, MergeIterator::create(levels_concat_iters))?;
        let lsm_iterator = LsmIterator::new(lsm_iter_inner, map_bound(_upper))?;

        let fused_iterator = FusedIterator::new(lsm_iterator);

        Ok(fused_iterator)
    }
}
