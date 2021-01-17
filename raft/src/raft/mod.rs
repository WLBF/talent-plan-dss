use futures::channel::mpsc::UnboundedSender;
use rand::prelude::*;
use rand::rngs::StdRng;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time;
use std::time::Instant;
use tokio::runtime;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use futures::FutureExt;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

/// Role of a raft peer.
#[derive(Debug, Eq, PartialEq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    current_term: u64,
    vote_for: Option<usize>,
    role: Role,
    last_heartbeat: Instant,
    apply_ch: UnboundedSender<ApplyMsg>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            vote_for: None,
            current_term: 0,
            role: Role::Follower,
            last_heartbeat: Instant::now(),
            apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel(1);
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            tx.send(res).await.unwrap_or(()); // ignore sendError
        });
        rx
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel(1);
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            tx.send(res).await.unwrap_or(()); // ignore sendError
        });
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    // request vote rpc handler
    fn request_vote(&mut self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        if args.term > self.current_term {
            self.convert_to_follower(args.term);
        }

        let mut vote_granted = true;
        if args.term < self.current_term {
            vote_granted = false;
        }

        if self.vote_for.is_some() {
            vote_granted = false;
        }

        if vote_granted {
            self.vote_for.replace(args.candidate_id as usize);
        }

        Ok(RequestVoteReply {
            term: self.current_term,
            vote_granted,
        })
    }

    // append entries rpc handler
    fn append_entries(&mut self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        if args.term < self.current_term {
            return Ok(AppendEntriesReply {
                term: self.current_term,
                success: false,
            });
        }

        if args.term > self.current_term {
            self.convert_to_follower(args.term);
        }

        self.last_heartbeat = Instant::now();

        if self.role == Role::Candidate {
            self.convert_to_follower(args.term);
        }

        Ok(AppendEntriesReply {
            term: self.current_term,
            success: true,
        })
    }

    fn convert_to_candidate(&mut self) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.vote_for.replace(self.me);
        info!(
            "[{}] convert to candidate, term: {}",
            self.me, self.current_term
        );
    }

    fn convert_to_follower(&mut self, term: u64) {
        self.role = Role::Follower;
        self.current_term = term;
        self.vote_for.take();
        info!(
            "[{}] convert to follower, term: {}",
            self.me, self.current_term
        );
    }

    fn convert_to_leader(&mut self) {
        self.role = Role::Leader;
        info!(
            "[{}] convert to leader, term: {}",
            self.me, self.current_term
        );
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
        let _ = &self.apply_ch;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    rt: Arc<runtime::Runtime>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (tx, rx) = channel(1);
        let raft = Arc::new(Mutex::new(raft));

        let raft_1 = raft.clone();
        let raft_2 = raft.clone();

        let rt = runtime::Runtime::new().unwrap();
        rt.spawn(election_loop(raft_1, tx));
        rt.spawn(heartbeat_loop(raft_2, rx));

        Node {
            raft,
            rt: Arc::new(rt),
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        let rf = self.raft.lock().unwrap();
        rf.start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        let rf = self.raft.lock().unwrap();
        rf.current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        let rf = self.raft.lock().unwrap();
        rf.role == Role::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        let rf = self.raft.lock().unwrap();
        State {
            term: rf.current_term,
            is_leader: rf.role == Role::Leader,
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }
}

async fn election_loop(raft: Arc<Mutex<Raft>>, heartbeat_tx: Sender<i32>) {
    let mut rng = StdRng::from_entropy();
    let mut last_election = Instant::now();
    loop {
        let num = rng.gen_range(250, 400);
        let timeout = time::Duration::from_millis(num);

        let raft_1 = raft.clone();
        let heartbeat_tx_1 = heartbeat_tx.clone();

        // critical section
        {
            let rf = raft.lock().unwrap();
            let now = Instant::now();
            if rf.role != Role::Leader
                && now.duration_since(rf.last_heartbeat) > timeout
                && now.duration_since(last_election) > timeout
            {
                tokio::spawn(try_election(raft_1, heartbeat_tx_1));
                last_election = now;
            }
        }

        tokio::time::sleep(time::Duration::from_millis(10)).await;
    }
}

async fn try_election(raft: Arc<Mutex<Raft>>, heartbeat_tx: Sender<i32>) {
    let mut rxs = VecDeque::new();
    let peer_num;
    let mut args = RequestVoteArgs {
        term: 0,
        candidate_id: 0,
    };

    // critical section
    {
        let mut rf = raft.lock().unwrap();
        rf.convert_to_candidate();

        peer_num = rf.peers.len();
        args.term = rf.current_term;
        args.candidate_id = rf.me as u32;

        for i in 0..peer_num {
            if i != rf.me {
                info!("[{}] -> [{}] {:?}", rf.me, i, args);
                let rx = rf.send_request_vote(i, args.clone());
                rxs.push_back((i, rx));
            }
        }
    }

    let mut cnt = 0;
    let mut done = 0;

    while let Some((i, mut rx)) = rxs.pop_front() {
        let empty = match rx.recv().now_or_never() {
            None => true,
            Some(None) => false,
            Some(Some(res)) => {

                // critical section
                if let Ok(reply) = res {
                    let mut rf = raft.lock().unwrap();
                    info!("[{}] <- [{}] {:?}", rf.me, i, reply);
                    if reply.term > rf.current_term {
                        rf.convert_to_follower(reply.term);
                    }

                    if reply.term == args.term && reply.vote_granted {
                        cnt += 1;
                    }
                }
                false
            }
        };

        if empty {
            rxs.push_back((i, rx));
        } else {
            done += 1;
        }

        if cnt >= peer_num / 2 {
            let valid;

            // critical section
            {
                let mut rf = raft.lock().unwrap();
                valid = args.term == rf.current_term && rf.role == Role::Candidate;
                if valid {
                    rf.convert_to_leader();
                }
            };

            if valid {
                heartbeat_tx.send(1).await.unwrap();
            }
            return;
        }

        if peer_num - done + cnt < peer_num / 2 {
            return;
        }
    }
}

async fn heartbeat_loop(raft: Arc<Mutex<Raft>>, mut heartbeat_rx: Receiver<i32>) {
    loop {
        heartbeat_rx.recv().await;
        loop {
            let mut pause = false;

            // critical section
            {
                let rf = raft.lock().unwrap();
                if rf.role != Role::Leader {
                    pause = true;
                }

                let raft_1 = raft.clone();
                tokio::spawn(heartbeat(raft_1));
            };

            if pause {
                break;
            }
            tokio::time::sleep(time::Duration::from_millis(100)).await;
        }
    }
}

async fn heartbeat(raft: Arc<Mutex<Raft>>) {
    let mut rxs = VecDeque::new();
    let mut args = AppendEntriesArgs { term: 0 };

    // critical section
    {
        let rf = raft.lock().unwrap();
        if rf.role != Role::Leader {
            return;
        }
        args.term = rf.current_term;

        for i in 0..rf.peers.len() {
            if i != rf.me {
                let rx = rf.send_append_entries(i, args.clone());
                rxs.push_back((i, rx));
                info!("[{}] -> [{}] {:?}", rf.me, i, args);
            }
        }
    }

    while let Some((i, mut rx)) = rxs.pop_front() {
        let empty = match rx.recv().now_or_never() {
            None => true,
            Some(None) => false,
            Some(Some(res)) => {

                // critical section
                if let Ok(reply) = res {
                    let mut rf = raft.lock().unwrap();
                    info!("[{}] <- [{}] {:?}", rf.me, i, reply);
                    if reply.term > rf.current_term {
                        rf.convert_to_follower(reply.term);
                        return;
                    }
                }
                false
            }
        };

        if empty {
            rxs.push_back((i, rx));
        }
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let raft = self.raft.clone();
        self.rt
            .spawn(async move {
                let mut rf = raft.lock().unwrap();
                rf.request_vote(args)
            })
            .await
            .unwrap()
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let raft = self.raft.clone();
        self.rt
            .spawn(async move {
                let mut rf = raft.lock().unwrap();
                rf.append_entries(args)
            })
            .await
            .unwrap()
    }
}
