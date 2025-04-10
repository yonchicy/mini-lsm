#![allow(dead_code)]
// REMOVE THIS LINE after fully implementing this functionality
// Copyright (c) 2022-2025 Alex Chi Z //
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

use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(_path)
                    .context("failed to create wal file")?,
            ))),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = _path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from wal file")?;
        let mut buf = Vec::new();

        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();
        while buf_ptr.has_remaining() {
            let mut hasher = crc32fast::Hasher::new();
            let batch_size = buf_ptr.get_u32();
            hasher.write(&(batch_size).to_be_bytes());
            for _ in 0..batch_size {
                let key_len = buf_ptr.get_u16() as usize;
                hasher.write(&(key_len as u16).to_be_bytes());

                let key = Bytes::copy_from_slice(&buf_ptr[..key_len]);
                hasher.write(&key);
                buf_ptr.advance(key_len);

                let ts = buf_ptr.get_u64();
                hasher.write(&(ts).to_be_bytes());

                let val_len = buf_ptr.get_u16() as usize;
                hasher.write(&(val_len as u16).to_be_bytes());
                let val = Bytes::copy_from_slice(&buf_ptr[..val_len]);
                hasher.write(&val);
                buf_ptr.advance(val_len);
                _skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), val);
            }
            let checksum = buf_ptr.get_u32();
            if checksum != hasher.finalize() {
                bail!("wal file checksum mismatch");
            }
            // println!(
            //     "recoverying memtable {:?} with kv ({:?},{:?})",
            //     _path.as_ref(),
            //     key,
            //     val
            // );
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        self.put_batch(&[(_key, _value)])
    }
    // |   HEADER   |                          BODY                                      |  FOOTER  |
    // |     u32    |   u16   | var | u64 |    u16    |  var  |           ...            |    u32   |
    // | batch_size | key_len | key | ts  | value_len | value | more key-value pairs ... | checksum |

    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        let batch_size = _data.len() as u32;
        let mut buf = Vec::<u8>::new();
        buf.put_u32(batch_size);
        for (_key, _value) in _data {
            let key_len = _key.key_len();
            let val_len = _value.len();
            buf.put_u16(key_len as u16);
            buf.put_slice(_key.key_ref());
            buf.put_u64(_key.ts());
            buf.put_u16(val_len as u16);
            buf.put_slice(_value);
        }
        let mut file = self.file.lock();
        file.write_all(&buf)?;
        file.write_all(&crc32fast::hash(&buf).to_be_bytes())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
