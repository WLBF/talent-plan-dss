use std::fmt;

use crate::proto::kvraftpb::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::channel;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    seq: AtomicU64,
    leader: AtomicUsize,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk {
            name,
            servers,
            seq: AtomicU64::new(1),
            leader: AtomicUsize::new(0),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        let args = GetRequest {
            key,
            name: self.name.clone(),
            seq: self.seq.fetch_add(1, Ordering::SeqCst),
        };

        let mut i = self.leader.load(Ordering::Relaxed);
        loop {
            let server = &self.servers[i];
            let server_clone = server.clone();
            let (tx, rx) = channel();
            let args_clone = args.clone();
            server.spawn(async move {
                let res = server_clone.get(&args_clone).await;
                let _ = tx.send(res);
            });

            if let Ok(Ok(reply)) = rx.recv() {
                if reply.wrong_leader {
                    i = (i + 1) % self.servers.len();
                } else if reply.err.is_empty() {
                    self.leader.store(i, Ordering::Relaxed);
                    return reply.value;
                }
            }
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        let (op, key, value) = match op {
            Op::Put(key, value) => (1, key, value),
            Op::Append(key, value) => (2, key, value),
        };

        let args = PutAppendRequest {
            key,
            value,
            op,
            name: self.name.clone(),
            seq: self.seq.fetch_add(1, Ordering::SeqCst),
        };

        let mut i = self.leader.load(Ordering::Relaxed);
        loop {
            let server = &self.servers[i];
            let server_clone = server.clone();
            let (tx, rx) = channel();
            let args_clone = args.clone();
            server.spawn(async move {
                let res = server_clone.put_append(&args_clone).await;
                let _ = tx.send(res);
            });

            if let Ok(Ok(reply)) = rx.recv() {
                if reply.wrong_leader {
                    i = (i + 1) % self.servers.len();
                } else if reply.err.is_empty() {
                    self.leader.store(i, Ordering::Relaxed);
                    return;
                }
            }
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
