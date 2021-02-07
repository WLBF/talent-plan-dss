use labrpc::*;

use crate::msg;
use crate::service::{TSOClient, TransactionClient};
use std::sync::mpsc::channel;
use std::thread::sleep;
use std::time::Duration;

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    start_ts: u64,
    writes: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            start_ts: 0,
            writes: vec![],
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        let mut i = 1;
        let mut mul = 1;

        loop {
            let args = msg::TimestampRequest {};
            let cli = self.tso_client.clone();
            let (tx, rx) = channel();
            self.tso_client.spawn(async move {
                let res = cli.get_timestamp(&args).await;
                // ignore tx send error
                tx.send(res).unwrap_or(());
            });

            let res = rx.recv().expect("call get timestamp rx");

            if i < RETRY_TIMES && res.is_err() {
                sleep(Duration::from_millis(mul * BACKOFF_TIME_MS));
                i += 1;
                mul *= 2;
                continue;
            }

            break res.map(|reply| reply.start_ts);
        }
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        self.start_ts = self.get_timestamp().expect("get timestamp failed");
        self.writes.clear();
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        let mut i = 1;
        let mut mul = 1;

        loop {
            let args = msg::GetRequest {
                key: key.clone(),
                start_ts: self.start_ts,
            };

            let cli = self.txn_client.clone();
            let (tx, rx) = channel();
            self.txn_client.spawn(async move {
                let res = cli.get(&args).await;
                // ignore tx send error
                tx.send(res).unwrap_or(());
            });

            let res = rx.recv().expect("call get rx");

            if i < RETRY_TIMES && res.is_err() {
                sleep(Duration::from_millis(mul * BACKOFF_TIME_MS));
                i += 1;
                mul *= 2;
                continue;
            }

            break res.map(|reply| reply.value);
        }
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.writes.push((key, value));
    }

    fn call_prewrite(&self, w: (Vec<u8>, Vec<u8>), primary: (Vec<u8>, Vec<u8>)) -> Result<bool> {
        let mut i = 1;
        let mut mul = 1;
        loop {
            let (w_key, w_val) = w.clone();
            let (p_key, p_val) = primary.clone();

            let args = msg::PrewriteRequest {
                write: Some(msg::Write {
                    key: w_key,
                    value: w_val,
                }),
                primary: Some(msg::Write {
                    key: p_key,
                    value: p_val,
                }),
                start_ts: self.start_ts,
            };

            let cli = self.txn_client.clone();
            let (tx, rx) = channel();
            self.txn_client.spawn(async move {
                let res = cli.prewrite(&args).await;
                // ignore tx send error
                tx.send(res).unwrap_or(());
            });

            let res = rx.recv().expect("call prewrite rx");

            if i < RETRY_TIMES && res.is_err() {
                sleep(Duration::from_millis(mul * BACKOFF_TIME_MS));
                i += 1;
                mul *= 2;
                continue;
            }

            break res.map(|reply| reply.success);
        }
    }

    fn call_commit(&self, commit_ts: u64, is_primary: bool, w: (Vec<u8>, Vec<u8>)) -> Result<bool> {
        let mut i = 1;
        let mut mul = 1;
        loop {
            let (w_key, w_val) = w.clone();
            let args = msg::CommitRequest {
                start_ts: self.start_ts,
                commit_ts,
                is_primary,
                write: Some(msg::Write {
                    key: w_key,
                    value: w_val,
                }),
            };
            let cli = self.txn_client.clone();
            let (tx, rx) = channel();
            self.txn_client.spawn(async move {
                let res = cli.commit(&args).await;
                // ignore tx send error
                tx.send(res).unwrap_or(());
            });

            let res = rx.recv().expect("call commit rx");

            if i < RETRY_TIMES && res.is_err() {
                sleep(Duration::from_millis(mul * BACKOFF_TIME_MS));
                i += 1;
                mul *= 2;
                continue;
            }

            break res.map(|reply| reply.success);
        }
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        let mut it = self.writes.iter();
        let primary = it.next().expect("none write");
        let secondary = it
            .map(|x| x.to_owned())
            .collect::<Vec<(Vec<u8>, Vec<u8>)>>();

        if !self.call_prewrite(primary.clone(), primary.clone())? {
            return Ok(false);
        }

        for w in secondary.iter() {
            if !self.call_prewrite(w.clone(), primary.clone())? {
                return Ok(false);
            }
        }

        // Commit primary first.
        let commit_ts = self.get_timestamp()?;
        let res = self.call_commit(commit_ts, true, primary.clone());
        if res == Err(Error::Other("reqhook".to_owned())) || !res? {
            return Ok(false);
        }

        // Second phase: write out write records for secondary cells.
        for w in secondary.iter() {
            let _ = self.call_commit(commit_ts, false, w.clone());
        }

        Ok(true)
    }
}
