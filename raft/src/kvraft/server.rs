use futures::channel::mpsc::{unbounded, UnboundedReceiver};
use futures::channel::oneshot;
use futures::channel::oneshot::Sender;
use futures::executor::ThreadPool;

use crate::proto::kvraftpb::*;
use crate::raft;
use futures::task::SpawnExt;
use futures::StreamExt;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    kv: BTreeMap<String, String>,
    seq_tbl: HashMap<String, u64>,
    send_tbl: HashMap<u64, Sender<(u64, Option<String>)>>,
    apply_rx: Option<UnboundedReceiver<raft::ApplyMsg>>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, rx) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        KvServer {
            rf: raft::Node::new(rf),
            me,
            maxraftstate,
            kv: BTreeMap::new(),
            seq_tbl: HashMap::new(),
            send_tbl: HashMap::new(),
            apply_rx: Some(rx),
        }
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    server: Arc<Mutex<KvServer>>,
    pool: Arc<ThreadPool>,
    stop: Arc<AtomicBool>,
}

impl Node {
    pub fn new(mut kv: KvServer) -> Node {
        let apply_rx = kv.apply_rx.take().unwrap();
        let pool = Arc::new(ThreadPool::new().unwrap());
        let server = Arc::new(Mutex::new(kv));
        let stop = Arc::new(AtomicBool::new(false));

        let server_1 = server.clone();
        let stop_1 = stop.clone();

        pool.spawn(apply_loop(server_1, stop_1, apply_rx)).unwrap();
        Node { server, pool, stop }
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
        let sv = self.server.lock().unwrap();
        sv.rf.kill();
        self.stop.store(true, Ordering::Relaxed);
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        let sv = self.server.lock().unwrap();
        sv.rf.get_state()
    }
}

async fn apply_loop(
    server: Arc<Mutex<KvServer>>,
    stop: Arc<AtomicBool>,
    mut rx: UnboundedReceiver<raft::ApplyMsg>,
) {
    while !stop.load(Ordering::Relaxed) {
        if let Some(msg) = rx.next().await {
            let Command {
                op,
                key,
                value,
                name,
                seq,
            } = labcodec::decode::<Command>(&msg.command).unwrap();

            let (sender, val) = {
                let mut sv = server.lock().unwrap();

                let last_seq = sv.seq_tbl.get(&name).map(|x| x.to_owned()).unwrap_or(0);

                let val = match (op, seq > last_seq) {
                    (1, true) => {
                        sv.kv.insert(key, value);
                        sv.seq_tbl.insert(name.clone(), seq);
                        None
                    }
                    (2, true) => {
                        sv.kv
                            .entry(key)
                            .and_modify(|v| v.push_str(&value))
                            .or_insert(value);
                        sv.seq_tbl.insert(name.clone(), seq);
                        None
                    }
                    (3, _) => sv.kv.get(&key).map(|x| x.to_owned()),
                    _ => None,
                };

                (sv.send_tbl.remove(&msg.command_index), val)
            };

            if let Some(tx) = sender {
                tx.send((msg.command_term, val)).unwrap();
            }
        }
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        let server = self.server.clone();
        let handle = self.pool.spawn_with_handle(async move {
            let GetRequest { key, name, seq } = arg;
            let (tx, rx) = oneshot::channel();
            let term = {
                let mut sv = server.lock().unwrap();

                let cmd = Command {
                    op: 3,
                    key,
                    value: "".to_owned(),
                    name,
                    seq,
                };

                let res = sv.rf.start(&cmd);

                if res.is_err() {
                    return Ok(GetReply {
                        wrong_leader: true,
                        err: "".to_owned(),
                        value: "".to_owned(),
                    });
                }

                let (index, term) = res.unwrap();

                sv.send_tbl.insert(index, tx);
                term
            };

            let (cmd_term, val) = rx.await.map_err(labrpc::Error::Recv)?;

            if cmd_term != term {
                return Ok(GetReply {
                    wrong_leader: false,
                    err: "reply miss match arg".to_owned(),
                    value: "".to_owned(),
                });
            }

            Ok(GetReply {
                wrong_leader: false,
                err: "".to_owned(),
                value: val.unwrap_or_else(|| "".to_owned()),
            })
        });
        handle.unwrap().await
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        let server = self.server.clone();
        let handle = self.pool.spawn_with_handle(async move {
            let PutAppendRequest {
                op,
                key,
                value,
                name,
                seq,
            } = arg;
            let (tx, rx) = oneshot::channel();
            let term = {
                let mut sv = server.lock().unwrap();

                let cmd = Command {
                    op,
                    key,
                    value,
                    name,
                    seq,
                };

                let res = sv.rf.start(&cmd);

                if res.is_err() {
                    return Ok(PutAppendReply {
                        wrong_leader: true,
                        err: "".to_owned(),
                    });
                }

                let (index, term) = res.unwrap();

                sv.send_tbl.insert(index, tx);
                term
            };

            let (cmd_term, _) = rx.await.map_err(labrpc::Error::Recv)?;

            if cmd_term != term {
                return Ok(PutAppendReply {
                    wrong_leader: false,
                    err: "reply miss match arg".to_owned(),
                });
            }

            Ok(PutAppendReply {
                wrong_leader: false,
                err: "".to_owned(),
            })
        });
        handle.unwrap().await
    }
}
