// RUST 

// original post: https://stackoverflow.com/questions/184618/what-is-the-best-comment-in-source-code-you-have-ever-encountered
// 
// Dear maintainer:
// 
// Once you are done trying to 'optimize' this routine,
// and have realized what a terrible mistake that was,
// please increment the following counter as a warning
// to the next guy:
// 
// total_hours_wasted_here = 42
// 

use core::sync;
use core::time;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::default;
// use std::fmt::Write;
use std::fs;
use std::hash::Hash;
use std::hash::Hasher;
use std::collections::HashMap;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use chrono::DateTime;
use chrono::Utc;
use fnv::FnvHasher;
use std::time::{Instant, SystemTime};
use std::net::UdpSocket;
use chrono::Local;
use serde_with::serde_as;
use serde_with::TimestampSeconds;
#[macro_use]
extern crate serde_derive;


// #######################################################################
//                                  FILE 
// #######################################################################

// Log stores a chunk of data in a file
// A file consists of a collection of logs, together they represent the whole file
// If the content size is small, we store the content in memory, otherwise we just store in local filesystem
struct Log {
    seq_num: u64,        // the sequence number of this log
    log_size: u64,       // the size of the content
    source_host: String, // the source host that appended/created this log
    source_req_id: i64,  // the source request id to create this log

    content: Option<Vec<u8>>, // the in-memory content if the size is small
    content_filename: String, // the file that contains the content of this log
}

impl Log {
    // Constructor for creating a new log
    fn new(source_host: String, source_req_id: i64, seq_num: u64, content: Vec<u8>, content_dir: &str, dfs_file: &str) -> Option<Log> {
        let log_size = content.len() as u64;
        let mut content_filename = String::new();
        let content_option;

        if log_size <= parameter::MAX_LOG_SIZE_IN_MEMORY {
            // small enough to store in memory
            content_option = Some(content);
        } else {
            // need to store as a file
            content_filename = format!("{}/{}-{}.txt", content_dir, dfs_file, seq_num);

            if let Err(err) = fs::write(&content_filename, &content) {
                eprintln!("Error in writing log content into {}: {}", content_filename, err);
                return None;
            }

            content_option = None;
        }

        Some(Log {
            seq_num,
            log_size,
            source_host,
            source_req_id,
            content: content_option,
            content_filename,
        })
    }

    // Retrieve the content from a log
    // Return (content, successful)
    fn get_content(&self) -> Result<String, io::Error> {
        // Check if we can straight return from memory
        if let Some(ref content) = self.content {
            return Ok(String::from_utf8_lossy(content).to_string());
        }

        // Otherwise, we have to read from the replicated file
        let content = fs::read_to_string(&self.content_filename)?;
        Ok(content)
    }
}

mod parameter {
    pub const MAX_LOG_SIZE_IN_MEMORY: u64 = 1024; // maximum size for in-memory content
    pub const REPLICATE_NUM: usize = 3;           // replication factor 
}

#[derive(Clone)]
struct File {
    filename: String,
    filesize: u64,
    logs: Vec<Arc<Log>>,
    rw_lock: Arc<RwLock<()>>
}



impl File {
    fn new(dfs_filename: String) -> File {
        File {
            filename: dfs_filename,
            filesize: 0,
            logs: Vec::new(),
            rw_lock: Arc::new(RwLock::new(())),
        }
    }

    fn add_log(&mut self, log: Log) {
        let _write_lock = self.rw_lock.write().unwrap();
        self.filesize += log.log_size;
        self.logs.push(Arc::new(log));
    }

    // Add a given log to the end of the file 
    // seq_num is used to do a sanity check so that it is intended to be appended at the end
    // return whether the operation is successful
    fn append_file_with_log(&mut self, log: Arc<Log>, seq_num: u64) -> bool {
        let _write_lock = self.rw_lock.write().unwrap();

        if seq_num != self.logs.len() as u64 {
            eprintln!(
                "Failed to append log with seq_num {} at position {}, {} has {} logs",
                log.seq_num, seq_num, self.filename, self.logs.len()
            );
            return false;
        }

        self.filesize += log.log_size;
        self.logs.push(log);
        true
    }
}

// #######################################################################
//                                  Helper 
// #######################################################################

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
enum State {
    Alive, 
    Suspect,
    Dead,
}


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Member {
    // [TODO] 
    // [RESOLVED]
    hostname: String,
    port: i32,
    version: i64,
}

impl Member {
    pub fn to_string(&self) -> String {
        format!("{}:{}:{}", self.hostname, self.port, self.version)
    }
}
#[derive(Debug, Serialize, Deserialize, Clone)]
enum InfoType {
    Join = 0,
    Alive = 1,
    Fail = 2, 
    Leave = 3,
    Suspect = 4,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
struct MemberShipInfo {
    info_type: InfoType,
    member: Member,
    incarnation: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum MessageType {
    Ping = 0,
    Ack = 1,
    Join = 2,
    Accept = 3,
    Leave = 4,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct  Message {
    msg_type: MessageType,
    sender: Member,
    infos: Vec<MemberShipInfo>,
    known_members: Vec<Member>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct InfoItem {
    info: MemberShipInfo, // [TODO] 
    count: i64,
}



// A custom logger that prints out timming accurate unitl ms
#[derive(Clone)]
struct Logger {
    logger: Arc<Mutex<BufWriter<Box<dyn Write + Send>>>>,
}


impl Logger {
    // new logger with an output writer
    pub fn new(out: Box<dyn Write + Send>) -> Self {
        let writer = BufWriter::new(out);
        Logger {
            logger: Arc::new(Mutex::new(writer)),
        }
    }

    // Printf writes a log message with a timestamp
    pub fn printlog(&self, format: &str, args: std::fmt::Arguments) {
        let timestamp = Local::now().format("%H:%M:%S%.3f");
        let log_message = format!("{} {}", timestamp, format);
        if let Ok(mut logger) = self.logger.lock() {
            writeln!(logger, "{}", log_message).unwrap();
            writeln!(logger, "{}", args).unwrap();
            logger.flush().unwrap();
        }
    }

    // fatalf writes a log message with a timestamp and exits the program
    pub fn fatalf(&self, format: &str, args: std::fmt::Arguments) -> ! {
        let timestamp = Local::now().format("%H:%M:%S%.3f");
        let log_message = format!("{} {}", timestamp, format);
        if let Ok(mut logger) = self.logger.lock() {
            writeln!(logger, "{}", log_message).unwrap();
            writeln!(logger, "{}", args).unwrap();
            logger.flush().unwrap();
        }
        std::process::exit(1);
    }
}


// #######################################################################
//                                  Swim
// #######################################################################

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
struct MemberWithState {
    member: Member,
    state: State,
    #[serde_as(as = "TimestampSeconds<i64>")]
    suspect_time: SystemTime,
    incarnation: i32,
}


pub struct Swim {
    mu: Mutex<()>,
    me : Member, 
    members: Mutex<Vec<MemberWithState>>, // [TODO] implement ( MemberWithState )
    introducers: Mutex<Vec<Member>>,
    drop_rate: Mutex<i32>,
    is_sus: Mutex<bool>,
    incarnation: Mutex<i32>,

    //Related to ping routine
    next_idx: Mutex<usize>,
    pending_members: Mutex<HashMap<String, Instant>>,

    log: Arc<Logger>, // [TODO] implement ( Logger )
    
    dead: AtomicI32,
    conn: Option<UdpSocket>,

    buffers: Mutex<HashMap<InfoType, Vec<InfoItem>>>, // [TODO] implement ( InfoType ) and ( InfoItem )

    // channels to communicate join/leave to the upper layer 
    join_ch: Sender<Member>,
    gone_ch: Sender<Member>,
}   

impl Swim {
    // [TODO] Fix the constructor to operate correctly --or-- add another constructor 
    pub fn new(me: Member, join_ch: Sender<Member>, gone_ch: Sender<Member>) -> Self {
        Swim {
            mu: Mutex::new(()),
            me,
            members: Mutex::new(Vec::new()), 
            introducers: Mutex::new(Vec::new()),
            drop_rate: Mutex::new(0), 
            is_sus: Mutex::new(false),
            incarnation: Mutex::new(0),
            next_idx: Mutex::new(0),
            pending_members: Mutex::new(HashMap::new()),
            log: Arc::new(Logger::new(Box::new(io::stdout()))),
            dead: AtomicI32::new(0),
            conn: None, // [TODO] Not declared yet: maybe choice the coordinator to be vm1
            buffers: Mutex::new(HashMap::new()),
            join_ch, 
            gone_ch,
        }
    }

    pub fn get_members(&self) ->  Vec<Member> {
        let members = self.members.lock().unwrap();
        members.iter().map(|mws| mws.member.clone()).collect()
    }

    // Mimic a crash state
   pub fn kill(&self)  {
        self.dead.store(1, Ordering::SeqCst);
        if let Some(conn) = &self.conn {
            conn.set_nonblocking(true).ok();
        }
    }

    // check if the node is killed
   pub fn killed(&self) -> bool {
        self.dead.load(Ordering::SeqCst) == 1
    }
    
    // Enable suspicion (use Pingack + S)
    pub fn enable_suspicion(&self) {
        let _lock = self.mu.lock().unwrap();
        let mut is_sus = self.is_sus.lock().unwrap();
        if !*is_sus {
            *is_sus = true;
            self.log.printlog("[{}]: Enabled suspicion.", format_args!("{}", self.me.hostname));
        }
    }

    // Disable suspicion (use only Pingack)
    pub fn disable_suspicion(&self) {
        let _lock = self.mu.lock().unwrap();
        let mut is_sus = self.is_sus.lock().unwrap();
        if *is_sus {
            *is_sus = false;
            self.log.printlog("[{}]: Disabled suspicion.", format_args!("{}", self.me.hostname));
        }
    }

    // check if a given member exists in the member list
    pub fn has_member_exists(&self, mem: &Member) -> bool {
        let members = self.members.lock().unwrap();
        members.iter().any(|mws| mws.member == *mem)
    }
}


// #######################################################################
//                                  Hash Ring 
// #######################################################################
struct HashRing {
    keys: Vec<u32>,
    hashmap: BTreeMap<u32, Member>, // <- change to Member not String [FIXED]
    ring_mutex: Mutex<()>
}

impl HashRing {
    // constructor for creating a new hash ring 
    fn new() -> HashRing {
        HashRing {
            keys: Vec::new(),
            hashmap: BTreeMap::new(),
            ring_mutex: Mutex::new(()),  
        }
    }

    // Hash a key using FNV (Fowler-Noll-Vo hash function)
    fn hash(&self, key: &str) -> u32 {
        let mut hasher = FnvHasher::default();
        hasher.write(key.as_bytes());
        hasher.finish() as u32
    }

    // Add a node into known members
    fn add_node(&mut self, node: Member) { // <- Chage node to be (node: Member) NOT String [FIXED]
        let _lock = self.ring_mutex.lock().unwrap();

        // The key for a server is {hostname:port:version}
        let node_hash = self.hash(&node.to_string());

        // add this node into the node list
        self.keys.push(node_hash);
        self.hashmap.insert(node_hash, node.clone());

        // soert the kets for successor retrieval in log(n) time
        self.keys.sort();
        println!("{} joined at location {}", node.to_string(), node_hash);
    }

    pub fn remove_node(&mut self, node: Member) {
        let node_hash = self.hash(&node.to_string());

        let _lock = self.ring_mutex.lock().unwrap();

        if let Some(node_idx) = self.keys.iter().position(|&key| key == node_hash) {
            self.keys.remove(node_idx);
            if self.hashmap.remove(&node_hash).is_some() {
                println!("{} left at location {}", node.to_string(), node_hash);
            } else {
                println!("Node {} not found in the hash map", node.to_string());
            }
        } else {
            println!("Node {} not found in the keys vector", node.to_string());
        }
    }

    // Given a key, the filename 
    // return a list of nodes that are responsible for the file
    // the first node will be the primary node and the rest are backup nodes
    fn get_node_by_key(&self, key: &str) -> Vec<Member> {
        let key_hash = self.hash(key);
        let _lock = self.ring_mutex.lock().unwrap();

        // successor  
        let node_idx = self.search_key_idx(key_hash);

        let mut nodes = Vec::new();

        for i in 0..std::cmp::min(parameter::REPLICATE_NUM, self.keys.len()) {
            let idx = (node_idx + i) % self.keys.len();
            if let Some(node) = self.hashmap.get(&self.keys[idx]) {
                nodes.push(node.clone());
            }
        }
        nodes
    }

    fn search_key_idx(&self, key_hash: u32) -> usize {
        match self.keys.binary_search(&key_hash) {
            Ok(idx) => idx, 
            Err(idx) => idx % self.keys.len(),
        }
    }
}

// #######################################################################
//                                  DFS Server
// #######################################################################

struct CachedContent {
    content: Vec<u8>,
    seq_num: i32

}

struct DfsServer {
    sw: Arc<Swim>,                              // The underlying membership monitor daemon 
    join_receiver: Arc<Mutex<mpsc::Receiver<Member>>>,      // Channel to receive join members from sw
    gone_receiver: Arc<Mutex<mpsc::Receiver<Member>>>,      // Channel to receive left members from sw

    // Hash ring: Combine with membership protocol to know how to route files
    hr: Arc<HashRing>,                                      // Hash ring taht stores the state of the system this server percieves

    // RWMutex for concurrency
    rwlock: Arc<RwLock<()>>,                                // RWLock to control access to shared resources
    me: Member,                                             // My info (hostname, port, version)
    append_req_id: AtomicI64,                              // The request ID, increments every time we make an append request
    me_str: String,                                      // String representation of this server

    // To mantain when left or "crashed" in unit test
    dead: AtomicI64,                                     // Atomic integer representing if the server is dead

    // C
    log: Arc<Logger>,                                   // A custom logger that outputs until ms

    // Store files
    replica_dir: String,                                // Directory where all log content will be stored
    primary_files: Arc<Mutex<HashMap<String, File>>>,    // Primary files
    backup_files: Arc<Mutex<HashMap<String, File>>>,     // Backup files

    // Coordinator
    primary_files_lock: HashMap<String, Arc<Mutex<()>>>, // To serialize append for a single file

    // DfsServer can act as a client too
    latest_write_seq: Arc<Mutex<HashMap<String, u64>>>,             // Maps filename -> latest append done by this client
    cached_contents: Arc<Mutex<HashMap<String, CachedContent>>>, // [TODO]      // Stores the cached content for a specific file
    cached_list: Arc<Mutex<VecDeque<String>>>,                          // The most frequent accessed file is at the back of the list

}

impl DfsServer {
    fn new(hostname: String, port: i32, version: i64) -> Arc<Self> {
        // Create logger
        let logger = Arc::new(Logger::new(Box::new(io::stdout())));

        // Use a buffer channel with N capacity for non-blocking send/recv
        let (join_sender, join_receiver) = mpsc::channel();
        let (gone_sender, gone_receiver) = mpsc::channel();

        // Swim instance
        let sw = Arc::new(Swim::new(
            Member{
                hostname: hostname.clone(),
                port,
                version,
            }, 
            join_sender.clone(), 
            gone_sender.clone(),
        ));

        // Create hash ring and add itself to the ring
        let mut hr = HashRing::new();
        hr.add_node(sw.me.clone());

        // set up directory for logs
        let replica_dir = format!("./tmp_logs_{}", version);
        if let Err(err) = fs::remove_dir_all(&replica_dir) {
            eprint!(
                "[{}]: Error in removing {}: {}",
                sw.me.to_string(),
                replica_dir,
                err
            );
        }

        if let Err(err) = fs::create_dir(&replica_dir) {
            eprint!(
                "[{}]: Error creating directory {}: {}",
                sw.me.to_string(),
                replica_dir,
                err
            );
        }

        let dfs_server = Arc::new(DfsServer {
            sw: sw.clone(),
            join_receiver: Arc::new(Mutex::new(join_receiver)),
            gone_receiver: Arc::new(Mutex::new(gone_receiver)), 
            hr: Arc::new(hr),
            rwlock: Arc::new(RwLock::new(())),
            me: sw.me.clone(),
            append_req_id: AtomicI64::new(0),
            me_str: sw.me.to_string(),
            dead: AtomicI64::new(0),
            log: logger.clone(),
            replica_dir,
            primary_files: Arc::new(Mutex::new(HashMap::new())),
            backup_files: Arc::new(Mutex::new(HashMap::new())),
            primary_files_lock: HashMap::new(),
            latest_write_seq: Arc::new(Mutex::new(HashMap::new())),
            cached_contents: Arc::new(Mutex::new(HashMap::new())),
            cached_list: Arc::new(Mutex::new(VecDeque::new())),
        });

        // Start thread for asynchronous operations
        {
            let dfs_server = dfs_server.clone();
            std::thread::spawn({
                let dfs_server = dfs_server.clone();
                move || {
                    dfs_server.poll_channel_from_swim();
                }
            });
        }

        dfs_server
    }
  
    // Periodically poll join_receiver and gone_receiver for membership updates 
    // Redistribute the files when there is membership update 
    pub fn poll_channel_from_swim(&self) {
            let join_node = self.join_receiver.lock().unwrap().try_recv();
            let gone_node = self.gone_receiver.lock().unwrap().try_recv();

            match join_node {
                Ok(join_node) => {
                    self.handle_node_join(join_node);
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    // No new join event, continue
                }
                Err(_) => {
                    // [TODO] Handle error (channel disconnection)
                    eprint!("Join channel disconnected unexpectedly.");
                    return;
                }
            }

            match gone_node {
                Ok(gone_node) => {
                    self.handle_node_leave(gone_node);
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    // No new gone node event, continue
                }
                Err(_) => {
                     // [TODO] Handle error (channel disconnection)
                     eprint!("Join channel disconnected unexpectedly.");
                     return;
                }
            }

            std::thread::sleep(std::time::Duration::from_millis(parameter::MEMBERSHIP_INTERVAL));
    }

    pub fn killed(&self) -> bool {
        self.dead.load(Ordering::SeqCst) == 1
    }

    // Periodically updates our own primary and backup files when a node fails
    // Promote some backup replicas to primary replicas
    pub fn redistribute_files_fail(&self) {

    }

    // Periodically update primary and backup files when a node joins
    // Move some primary replicas to backup replicas
    // Remove some backup replicas
    pub fn redistribute_files_join(&self) {
        let _lock = self.rwlock.write().unwrap();
        {
            let mut primary_files = self.primary_files.lock().unwrap();
            let mut backup_files = self.backup_files.lock().unwrap();

            let mut files_to_move: Vec<&File> = Vec::new();
            for (primary_filename, primary_file) in primary_files.iter() {
                let replicas = self.hr.get_node_by_key(&primary_filename);

                if !replicas.is_empty() && replicas[0].to_string() != self.me_str {
                    // we are no longer responsible for this file, move to backup
                    self.log.printlog(
                        "[{}]: Moved {} from primary to backup\n",
                        format_args!("{}: {}", self.me_str, primary_filename),
                    );

                    files_to_move.push(primary_file);
                    backup_files.insert(primary_filename.clone(), primary_file.clone());
                }
            }

            for primary_filename in files_to_move {
                primary_files.remove(&primary_filename.filename);
            }
        }

        {
            let mut backup_files = self.backup_files.lock().unwrap();

            let mut files_to_move: Vec<&File> = Vec::new();
            for (backup_filename, _) in backup_files.iter() {
                let replicas = self.hr.get_node_by_key(&backup_filename);
                let is_me_one_of_replica = replicas
                    .iter()
                    .any(|replica| replica.to_string() == self.me_str);

                if !is_me_one_of_replica {
                    self.log.printlog( 
                        "[{}]: No longer storing {}\n",
                        format_args!("{}: {}", self.me_str, backup_filename),
                    );

                    files_to_move.push(backup_files.get(backup_filename).unwrap());
                }
            }

            for backup_filename in files_to_move {
                backup_files.remove(&backup_filename.filename);
            }
        }
    }

    pub fn handle_node_join(&self, node: Member) {
        let _lock = self.hr.ring_mutex.lock().unwrap();
        self.hr.add_node(node);
        self.redistribute_files_join();
    }

    pub fn handle_node_leave(&self, node: Member) {
        let _lock = self.hr.ring_mutex.lock().unwrap();
        self.hr.remove_node(node);
        self.redistribute_files_fail();
    }




        
}


// #######################################################################
//                                  MAIN 
// #######################################################################
fn main() {
    // Example usage
    let log = Log::new(
        "localhost".to_string(),
        42,
        1,
        b"This is an example stream of characters".to_vec(),
        "./content_dir",
        "dfs_file",
    );

    match log {
        Some(log) => match log.get_content() {
            Ok(content) => println!("Content retrieved successfully: {}", content),
            Err(e) => eprintln!("Error reading content: {}", e),
        },
        None => eprintln!("Failed to create log"),
    }


}