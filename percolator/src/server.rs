use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        Ok(TimestampResponse {
            start_ts: current_time(),
        })
    }
}

fn current_time() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards");
    since_the_epoch.as_nanos() as u64
}

// Key is a tuple (raw.key.clone(), timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

#[derive(Debug, Clone)]
pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default)]
pub struct KvTable {
    write: BTreeMap<Key, Value>,
    data: BTreeMap<Key, Value>,
    lock: BTreeMap<Key, Value>,
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.

        let col = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };

        let ts_start = ts_start_inclusive.unwrap_or(0);
        let ts_end = ts_end_inclusive.unwrap_or(u64::MAX);

        col.range((key.clone(), ts_start)..=(key, ts_end)).last()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let col = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };

        col.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        let col = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        col.remove(&(key, commit_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        loop {
            let kv = self.data.lock().unwrap();
            // Check for locks that signal concurrent writes.
            let key = req.key.clone();
            let start_ts = req.start_ts;

            if kv
                .read(key.clone(), Column::Lock, None, Some(start_ts))
                .is_some()
            {
                // There is a pending lock; try to clean it and wait
                std::mem::drop(kv);
                self.back_off_maybe_clean_up_lock(start_ts, key);
                continue;
            }

            // Find the latest write below our start timestamp
            let opt = kv.read(key.clone(), Column::Write, None, Some(start_ts));

            if opt.is_none() {
                return Ok(GetResponse { value: vec![] });
            }

            let data_ts = match opt {
                Some((_, Value::Timestamp(ts))) => ts.to_owned(),
                _ => unreachable!(),
            };

            let (_, val) = kv
                .read(key, Column::Data, Some(data_ts), Some(data_ts))
                .expect("missing data");
            let value = match val {
                Value::Vector(v) => v.to_owned(),
                _ => unreachable!(),
            };

            break Ok(GetResponse { value });
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        let w = req.write.unwrap();
        let primary = req.primary.unwrap();
        let start_ts = req.start_ts;
        let mut kv = self.data.lock().unwrap();

        // Abort on writes after our start timestamp ...
        if kv
            .read(w.key.clone(), Column::Write, Some(start_ts), None)
            .is_some()
        {
            return Ok(PrewriteResponse { success: false });
        }
        // ... or locks at any timestamp.
        if kv.read(w.key.clone(), Column::Lock, None, None).is_some() {
            return Ok(PrewriteResponse { success: false });
        }

        kv.write(
            w.key.clone(),
            Column::Data,
            start_ts,
            Value::Vector(w.value),
        );
        // The primary’s location.
        kv.write(w.key, Column::Lock, start_ts, Value::Vector(primary.key));

        Ok(PrewriteResponse { success: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        let w = req.write.unwrap();
        let start_ts = req.start_ts;
        let commit_ts = req.commit_ts;
        let is_primary = req.is_primary;
        let mut kv = self.data.lock().unwrap();
        if is_primary
            && kv
                .read(w.key.clone(), Column::Lock, Some(start_ts), Some(start_ts))
                .is_none()
        {
            // aborted while working
            return Ok(CommitResponse { success: false });
        }

        // write out write records for secondary cells.

        // Pointer to data written at start ts
        kv.write(
            w.key.clone(),
            Column::Write,
            commit_ts,
            Value::Timestamp(start_ts),
        );
        // commit point
        kv.erase(w.key, Column::Lock, start_ts);

        Ok(CommitResponse { success: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        let mut kv = self.data.lock().unwrap();
        let opt = kv.read(key.clone(), Column::Lock, None, Some(start_ts));
        if opt.is_none() {
            return;
        }

        let (lock_ts, primary_key) = match opt {
            Some(((_, ts), Value::Vector(p_key))) => (ts.to_owned(), p_key.to_owned()),
            _ => unreachable!(),
        };

        // back off if the key's lifetime not exceeds TTL.
        if current_time() - lock_ts < TTL {
            return;
        }

        // if key itself is primary and lock is present, erase the lock and data.
        if primary_key == key {
            kv.erase(primary_key.clone(), Column::Lock, lock_ts);
            kv.erase(primary_key, Column::Data, lock_ts);
            return;
        }

        // if primary lock is present, erase both primary and current lock and data.
        // transaction that encounters the lock should rollback.
        if kv
            .read(
                primary_key.clone(),
                Column::Lock,
                Some(lock_ts),
                Some(lock_ts),
            )
            .is_some()
        {
            kv.erase(primary_key.clone(), Column::Lock, lock_ts);
            kv.erase(primary_key, Column::Data, lock_ts);
            kv.erase(key.clone(), Column::Lock, lock_ts);
            kv.erase(key, Column::Data, lock_ts);
            return;
        }

        // if primary lock is disappeared, check data @ primary.lock_ts to decide roll forward or roll back.
        if kv
            .read(primary_key, Column::Data, Some(lock_ts), Some(lock_ts))
            .is_some()
        {
            // roll forward
            kv.write(
                key.clone(),
                Column::Write,
                lock_ts,
                Value::Timestamp(lock_ts),
            );
            kv.erase(key, Column::Lock, lock_ts);
        } else {
            // roll back
            kv.erase(key.clone(), Column::Lock, lock_ts);
            kv.erase(key, Column::Data, lock_ts);
        }
    }
}
