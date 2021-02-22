use futures::channel::mpsc::UnboundedSender;
use futures::SinkExt;
use rand::prelude::*;
use std::cmp::{max, min};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
    pub command_term: u64,
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
    last_receive: Instant,
    apply_ch: UnboundedSender<ApplyMsg>,
    commit_index: u64,
    last_applied: u64,
    next_index: Vec<u64>,
    match_index: Vec<u64>,
    log: Vec<LogEntry>,
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
        let len = peers.len();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            vote_for: None,
            current_term: 0,
            role: Role::Follower,
            last_receive: Instant::now(),
            apply_ch,
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; len],
            match_index: vec![0; len],
            log: vec![],
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        let mut data = vec![];
        let state = PersistentState {
            current_term: self.current_term,
            vote_for: self.vote_for.map(|x| x as i32).unwrap_or(-1),
            log: self.log.clone(),
        };

        labcodec::encode(&state, &mut data).unwrap();
        self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }

        match labcodec::decode::<PersistentState>(data) {
            Ok(o) => {
                self.current_term = o.current_term;
                self.log = o.log;
                if o.vote_for >= 0 {
                    self.vote_for.replace(o.vote_for as usize);
                }
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
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

    fn make_append_entries(&self, peer: usize) -> AppendEntriesArgs {
        let mut args = AppendEntriesArgs {
            term: self.current_term,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: self.commit_index,
        };

        let next_index = self.next_index[peer];
        for idx in next_index..=self.last_log_index() {
            args.entries.push(self.get_entry(idx).unwrap());
        }

        assert!(next_index > 0);
        if let Some(prev_log) = self.get_entry(next_index - 1) {
            args.prev_log_index = prev_log.index;
            args.prev_log_term = prev_log.term;
        }
        args
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

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.role == Role::Leader {
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            // Your code here (2B).
            let index = self.last_log_index() + 1;
            let term = self.current_term;

            let entry = LogEntry {
                index,
                term,
                command: buf,
            };

            info!("[{}] start {:?}", self.me, entry);
            self.log.push(entry);
            self.match_index[self.me] = index;
            self.persist();
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn is_candidate_up_to_date(&self, index: u64, term: u64) -> bool {
        let last_term = self.last_log_term();
        let last_index = self.last_log_index();

        if term == last_term {
            index >= last_index
        } else {
            term > last_term
        }
    }

    // request vote rpc handler
    fn request_vote(&mut self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        if args.term > self.current_term {
            self.convert_to_follower(args.term);
        }

        let mut reply = RequestVoteReply {
            term: self.current_term,
            vote_granted: false,
        };

        if args.term < self.current_term {
            return Ok(reply);
        }

        if self.vote_for.is_some() {
            return Ok(reply);
        }

        if self.is_candidate_up_to_date(args.last_log_index, args.last_log_term) {
            reply.vote_granted = true;
            self.vote_for.replace(args.candidate_id as usize);
            self.last_receive = Instant::now();
        }

        self.persist();
        Ok(reply)
    }

    // append entries rpc handler
    fn append_entries(&mut self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut reply = AppendEntriesReply {
            term: self.current_term,
            success: false,
            conflict_index: 0,
            conflict_term: 0,
        };

        if args.term < self.current_term {
            return Ok(reply);
        }

        self.last_receive = Instant::now();

        if args.term > self.current_term {
            self.convert_to_follower(args.term);
        }

        if self.role == Role::Candidate {
            self.convert_to_follower(args.term);
        }

        if args.prev_log_index > 0 {
            let opt = self.get_entry(args.prev_log_index);
            if opt.is_none() {
                reply.conflict_index = self.last_log_index();
                return Ok(reply);
            }

            let prev_log = opt.unwrap();
            if prev_log.term != args.prev_log_term {
                reply.conflict_term = prev_log.term;
                reply.conflict_index = self
                    .log
                    .iter()
                    .find(|&x| x.term == prev_log.term)
                    .map(|x| x.index)
                    .unwrap();
                self.truncate_log(prev_log.index);
                return Ok(reply);
            }
        }

        for log in args.entries.iter() {
            if let Some(local) = self.get_entry(log.index) {
                if local.term != log.term {
                    self.truncate_log(local.index);
                    self.log.push(log.to_owned());
                }
            } else {
                self.log.push(log.to_owned());
            }
        }

        if args.leader_commit > self.commit_index {
            self.commit_index = min(args.leader_commit, self.last_log_index());
        }

        reply.success = true;

        self.persist();
        Ok(reply)
    }

    fn check_committed(&self, index: u64) -> bool {
        if index <= self.commit_index || self.role != Role::Leader {
            return false;
        }

        let cnt = self
            .match_index
            .iter()
            .fold(0, |acc, &x| acc + (x >= index) as usize);
        if cnt <= self.peers.len() / 2 {
            return false;
        }

        let log = self.get_entry(index).expect("check committed entry none");
        log.term == self.current_term
    }

    fn check_apply(&self) -> bool {
        self.commit_index > self.last_applied
    }

    fn convert_to_candidate(&mut self) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.vote_for.replace(self.me);
        self.persist();
        info!(
            "[{}] convert to candidate, term: {}",
            self.me, self.current_term
        );
    }

    fn convert_to_follower(&mut self, term: u64) {
        self.role = Role::Follower;
        self.current_term = term;
        self.vote_for.take();
        self.persist();
        info!(
            "[{}] convert to follower, term: {}",
            self.me, self.current_term
        );
    }

    fn convert_to_leader(&mut self) {
        self.role = Role::Leader;

        for i in 0..self.next_index.len() {
            self.next_index[i] = self.last_log_index() + 1;
        }

        info!(
            "[{}] convert to leader, term: {}",
            self.me, self.current_term
        );
    }

    fn last_log_index(&self) -> u64 {
        self.log.last().map(|l| l.index).unwrap_or(0)
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map(|l| l.term).unwrap_or(0)
    }

    fn get_entry(&self, index: u64) -> Option<LogEntry> {
        match index {
            0 => None,
            idx => self.log.get(idx as usize - 1).map(|x| x.to_owned()),
        }
    }

    fn truncate_log(&mut self, index: u64) {
        while let Some(entry) = self.log.last() {
            if entry.index < index {
                break;
            }
            self.log.pop();
        }
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
    stop: Arc<AtomicBool>,
    apply_tx: Sender<i32>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (heartbeat_tx, heartbeat_rx) = channel(3);
        let (apply_tx, apply_rx) = channel(3);
        let raft = Arc::new(Mutex::new(raft));

        let raft_1 = raft.clone();
        let raft_2 = raft.clone();
        let raft_3 = raft.clone();

        let stop = Arc::new(AtomicBool::new(false));
        let stop_1 = stop.clone();
        let stop_2 = stop.clone();
        let stop_3 = stop.clone();

        let rt = runtime::Runtime::new().unwrap();
        rt.spawn(election_loop(stop_1, raft_1, heartbeat_tx));
        let apply_tx_1 = apply_tx.clone();
        rt.spawn(heartbeat_loop(stop_2, raft_2, heartbeat_rx, apply_tx_1));
        rt.spawn(apply_loop(stop_3, raft_3, apply_rx));

        Node {
            raft,
            rt: Arc::new(rt),
            stop,
            apply_tx,
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
        let mut rf = self.raft.lock().unwrap();
        rf.start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        let rf = self.raft.lock().unwrap();
        rf.current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
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
        self.stop.store(true, Ordering::Relaxed);
    }
}

async fn election_loop(stop: Arc<AtomicBool>, raft: Arc<Mutex<Raft>>, heartbeat_tx: Sender<i32>) {
    while !stop.load(Ordering::Relaxed) {
        let num = thread_rng().gen_range(350, 500);
        let timeout = Duration::from_millis(num);

        let raft_1 = raft.clone();
        let heartbeat_tx_1 = heartbeat_tx.clone();

        // critical section
        {
            let mut rf = raft.lock().unwrap();
            let now = Instant::now();
            if rf.role != Role::Leader && now.duration_since(rf.last_receive) > timeout {
                rf.last_receive = now;
                tokio::spawn(try_election(raft_1, heartbeat_tx_1));
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn try_election(raft: Arc<Mutex<Raft>>, heartbeat_tx: Sender<i32>) {
    let mut rxs = Vec::new();
    let peer_num;
    let args_term;

    // critical section
    {
        let mut rf = raft.lock().unwrap();
        rf.convert_to_candidate();

        peer_num = rf.peers.len();
        args_term = rf.current_term;

        let args = RequestVoteArgs {
            term: rf.current_term,
            candidate_id: rf.me as u32,
            last_log_index: rf.last_log_index(),
            last_log_term: rf.last_log_term(),
        };

        for i in 0..peer_num {
            if i != rf.me {
                info!("[{}] -> [{}] {:?}", rf.me, i, args);
                let rx = rf.send_request_vote(i, args.clone());
                rxs.push((i, rx));
            }
        }
    }

    let cnt = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));

    for (i, mut rx) in rxs {
        let raft_1 = raft.clone();
        let cnt_1 = cnt.clone();
        let done_1 = done.clone();
        tokio::spawn(async move {
            if let Some(Ok(reply)) = rx.recv().await {
                let mut rf = raft_1.lock().unwrap();
                info!("[{}] <- [{}] {:?}", rf.me, i, reply);
                if reply.term > rf.current_term {
                    rf.convert_to_follower(reply.term);
                }

                if reply.term == args_term && reply.vote_granted {
                    cnt_1.fetch_add(1, Ordering::SeqCst);
                }
            }
            done_1.fetch_add(1, Ordering::SeqCst);
        });
    }

    while done.load(Ordering::SeqCst) < peer_num {
        if cnt.load(Ordering::SeqCst) >= peer_num / 2 {
            let mut rf = raft.lock().unwrap();
            if args_term == rf.current_term && rf.role == Role::Candidate {
                rf.convert_to_leader();
                let _ = heartbeat_tx.try_send(1);
            }
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn heartbeat_loop(
    stop: Arc<AtomicBool>,
    raft: Arc<Mutex<Raft>>,
    mut heartbeat_rx: Receiver<i32>,
    apply_tx: Sender<i32>,
) {
    while !stop.load(Ordering::Relaxed) {
        heartbeat_rx.recv().await;
        loop {
            {
                let rf = raft.lock().unwrap();
                if rf.role != Role::Leader {
                    break;
                }

                let raft_1 = raft.clone();
                let tx_1 = apply_tx.clone();
                tokio::spawn(heartbeat(raft_1, tx_1));
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn heartbeat(raft: Arc<Mutex<Raft>>, apply_tx: Sender<i32>) {
    let mut rxs = Vec::new();

    // critical section
    {
        let rf = raft.lock().unwrap();
        if rf.role != Role::Leader {
            return;
        }

        for i in 0..rf.peers.len() {
            if i != rf.me {
                let args = rf.make_append_entries(i);
                let rx = rf.send_append_entries(i, args.clone());
                info!("[{}] -> [{}] {:?}", rf.me, i, args);
                rxs.push((i, args, rx));
            }
        }
    }

    for (i, args, mut rx) in rxs {
        let raft_1 = raft.clone();
        let tx_1 = apply_tx.clone();
        tokio::spawn(async move {
            if let Some(Ok(reply)) = rx.recv().await {
                let mut rf = raft_1.lock().unwrap();
                info!("[{}] <- [{}] {:?}", rf.me, i, reply);
                if reply.term > rf.current_term {
                    rf.convert_to_follower(reply.term);
                    return;
                }

                if rf.role == Role::Leader && args.term == rf.current_term {
                    if reply.success {
                        if !args.entries.is_empty() {
                            let last = args.prev_log_index + args.entries.len() as u64;
                            rf.match_index[i] = max(rf.match_index[i], last);
                            rf.next_index[i] = max(rf.next_index[i], last + 1);

                            if rf.check_committed(rf.match_index[i]) {
                                rf.commit_index = rf.match_index[i];
                                info!("[{}] commit {}", rf.me, rf.commit_index);
                                let _ = tx_1.try_send(1);
                            }
                        }
                    } else {
                        let next_idx = match rf
                            .log
                            .iter()
                            .rev()
                            .find(|&x| x.term == reply.conflict_term)
                            .map(|x| x.index)
                        {
                            Some(idx) => idx + 1,
                            None => reply.conflict_index,
                        };

                        rf.next_index[i] = min(rf.next_index[i], next_idx);
                        // make sure next_index[i] always larger than 0
                        rf.next_index[i] = max(rf.next_index[i], 1);
                    }
                }
            }
        });
    }
}

async fn apply_loop(stop: Arc<AtomicBool>, raft: Arc<Mutex<Raft>>, mut apply_rx: Receiver<i32>) {
    let mut apply_ch = {
        let rf = raft.lock().unwrap();
        rf.apply_ch.clone()
    };

    while !stop.load(Ordering::Relaxed) {
        apply_rx.recv().await;
        let mut logs = vec![];

        {
            let mut rf = raft.lock().unwrap();
            for idx in rf.last_applied + 1..=rf.commit_index {
                let log = rf.get_entry(idx).expect("apply entry missing");
                logs.push(log);
            }

            info!("[{}] apply {:?}", rf.me, logs);
            rf.last_applied = rf.commit_index;
        }

        for log in logs {
            let msg = ApplyMsg {
                command_valid: true,
                command: log.command,
                command_index: log.index,
                command_term: log.term,
            };

            apply_ch.send(msg).await.unwrap();
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
        let tx = self.apply_tx.clone();
        self.rt
            .spawn(async move {
                let mut rf = raft.lock().unwrap();
                let reply = rf.append_entries(args);
                if rf.check_apply() {
                    let _ = tx.try_send(1);
                }
                reply
            })
            .await
            .unwrap()
    }
}
