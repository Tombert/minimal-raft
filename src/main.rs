use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    response::IntoResponse, routing::{get, post}, Json, Router,  extract::State,
};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::{Receiver, Sender}, Mutex, OnceCell};

#[derive(PartialEq ,Debug,Clone)]
enum Roles {
    Candidate,
    Follower,
    Leader
}

#[derive(Debug,Clone, Serialize, Deserialize)]
struct Entry {
    key: String, 
    value: String,
    term: u64
}


#[derive(Debug,Clone)]
struct Node {
    node_id: String, 
    ip_address: String,
}

struct AppState  {
    commit_index: usize,
   term: u64,
   last_log_index: usize,
   last_log_term: u64,
   entries : Vec<Entry>,
   last_voted_for: Option<String>,
   current_leader : String,
   role: Roles,
   nodes: HashMap<String, Node>,
   next_index: HashMap<String, usize>,
   match_index: HashMap<String, usize>,
}


#[derive(Debug, Serialize, Deserialize)]
struct AppendEntryRequest {
    term: u64, 
    node_id:String, 
    prev_log_index: usize, 
    prev_log_term:u64, 
    entries: Vec<Entry>, 
    leader_commit: usize,

}

#[derive(Debug, Serialize, Deserialize)]
struct AppendEntryResponse { 
    term: u64,
    node_id: String, 
    response: bool

}

#[derive(Debug, Serialize, Deserialize)]
struct VoteRequest {
    node_id : String, 
    term: u64,
    last_log_index: usize,
    last_log_term: u64
}

#[derive(Debug, Serialize, Deserialize)]
struct VoteResponse {
    node_id: String, 
    term: u64,
    response: bool
}
const NODE_ID : &str= "A";

static GLOBAL_STORE: OnceCell<Mutex<HashMap<String, String>>> = OnceCell::const_new();
static TEMP_GLOBAL_STORE: OnceCell<Mutex<HashMap<String, String>>> = OnceCell::const_new();

async fn init_global_stores() {
    GLOBAL_STORE
        .set(Mutex::new(HashMap::new()))
        .expect("GLOBAL_STORE already initialized");
    TEMP_GLOBAL_STORE
        .set(Mutex::new(HashMap::new()))
        .expect("GLOBAL_STORE already initialized");
}

async fn insert_into_store(key: String, value: String) {
    let mut store = GLOBAL_STORE
        .get()
        .expect("GLOBAL_STORE not initialized")
        .lock().await;
    let mut temp_store = GLOBAL_STORE.get().expect("TEMP not inited").lock().await; 
    temp_store.remove(&key);
    store.insert(key, value);
}

async fn apply_fn(key: String, value: String) {
    insert_into_store(key, value).await;

}

async fn insert_into_temp_store(key: String, value: String) {
    let mut temp_store = TEMP_GLOBAL_STORE.get().expect("NOT INIT").lock().await;
    temp_store.insert(key, value);
}

async fn get_from_stores(key: &String) -> Option<String> {
    let temp_store = TEMP_GLOBAL_STORE.get().expect("NOT INIT").lock().await;

    if let Some(val) = temp_store.get(key) {
        return Some(val.to_string());
    } 

    let store = GLOBAL_STORE
        .get()
        .expect("GLOBAL_STORE not initialized")
        .lock().await;

    if let Some(val) = store.get(key){
        return Some(val.to_string());
    }

    None
}


async fn update_commit_index(state: &mut AppState) {
    let mut match_indexes: Vec<usize> = state.match_index.values().copied().collect();
    match_indexes.push(state.last_log_index); // Leader's own index

    match_indexes.sort_unstable_by(|a, b| b.cmp(a));

    let majority = (state.nodes.len() + 1) / 2; // +1 for self
    let new_commit_index = match_indexes[majority];

    if let Some(entry) = state.entries.get(new_commit_index) {
        if entry.term == state.term && new_commit_index > state.commit_index {
            println!("Committing up to index {}", new_commit_index);
            state.commit_index = new_commit_index;

            apply_fn(entry.key.clone(), entry.value.clone()).await;
        }
    }
}

//type SharedState = Arc< Mutex<AppState>>;
async fn handle_request_vote(State(state) : State<Arc<Mutex<AppState>>> , Json(req) : Json<VoteRequest>) -> impl IntoResponse {

    let mut x = state.lock().await; 
    
    if req.term > x.term {
        x.term = req.term;
        x.last_voted_for = None;
    }

    let mut vote_granted = false; 

    let up_to_date = 
        req.last_log_term > x.last_log_term 
        || (req.last_log_term == x.last_log_term && req.last_log_index > x.last_log_index);

    if req.term == x.term && (x.last_voted_for.is_none() || x.last_voted_for == Some(req.node_id.clone()) ) && up_to_date {
        vote_granted = true; 
        x.last_voted_for = Some(req.node_id.clone());
    }

    let response = VoteResponse {
        node_id: NODE_ID.to_string(),
        term: x.term,
        response: vote_granted,
    };    


    axum::Json(response)
}

async fn handle_append_entry(State(state) : State<Arc<Mutex<AppState>>> , Json(req) : Json<AppendEntryRequest>, heartbeat_chan: Sender<bool>) -> impl IntoResponse {
    let _ = heartbeat_chan.send(true).await;
    let mut s = state.lock().await; 
    if req.term > s.term {
        s.term = req.term;
        s.last_voted_for = None;
    }
    let resp = if req.term >= s.term  &&
        s.entries.get(req.prev_log_index)
        .map(|entry| entry.term == req.prev_log_term)
        .unwrap_or(false)
    {
        s.term = req.term; 
        
        if let Some(last) = req.entries.last() {
            s.last_log_term = last.term;
        }
        if !req.entries.is_empty() {
            s.entries.truncate(req.prev_log_index + 1 );
        }

        for entry in req.entries {
            s.entries.push(entry.clone());
        }
        if req.leader_commit > s.commit_index {
            let new_commit_index = std::cmp::min(req.leader_commit, s.last_log_index);
            for i in (s.commit_index + 1)..=new_commit_index {
                if let Some(entry) = s.entries.get(i) {
                    apply_fn(entry.key.clone(), entry.value.clone()).await;
                }
            }
            s.commit_index = new_commit_index;
        }
        s.last_log_index = s.entries.len().saturating_sub(1);
        s.current_leader = req.node_id; 
        s.role = Roles::Follower;
        AppendEntryResponse{
            term: s.term,
            node_id : NODE_ID.to_string(),
            response: true,
        }

    } else {
        AppendEntryResponse {
            term: s.term,
            node_id: NODE_ID.to_string(),
            response : false
        }


    };

    axum::Json(resp)
}

fn send_append_logs(state: Arc<Mutex<AppState>>) {
    tokio::task::spawn(async move {
        loop {
            let (_term, _entries, peers, role) = {
                let s = state.lock().await;
                (
                    s.term,
                    s.entries.clone(),
                    s.nodes.clone(),
                    s.role.clone(),
                )
            };

            if role == Roles::Leader {
                let client = reqwest::Client::new();
                let mut tasks = vec![];

                for (peer_id, peer) in peers.iter() {
                    let state = Arc::clone(&state);
                    let client = client.clone();
                    let peer_id = peer_id.clone();
                    let peer_ip = peer.ip_address.clone();

                    let task = tokio::spawn(async move {
                        let (term, next_idx, log_entries, prev_log_term) = {
                            let s = state.lock().await;
                            let next_index = *s.next_index.get(&peer_id).unwrap_or(&0);
                            let prev_log_index = next_index.saturating_sub(1);
                            let prev_log_term = s.entries.get(prev_log_index).map(|e| e.term).unwrap_or(0);
                            (
                                s.term,
                                next_index,
                                s.entries[next_index..].to_vec(),
                                prev_log_term,
                            )
                        };

                        let req = AppendEntryRequest {
                            term,
                            node_id: NODE_ID.to_string(),
                            prev_log_index: next_idx.saturating_sub(1),
                            prev_log_term,
                            entries: log_entries,
                            leader_commit: state.lock().await.commit_index,
                        };

                        match client
                            .post(format!("http://{}/append-entries", peer_ip))
                            .json(&req)
                            .send()
                            .await
                        {
                            Ok(resp) => match resp.json::<AppendEntryResponse>().await {
                                Ok(resp_data) => {
                                    let mut s = state.lock().await;
                                    if resp_data.term > s.term {
                                        s.term = resp_data.term;
                                        s.role = Roles::Follower;
                                        return;
                                    }

                                    if resp_data.response {
                                        let new_index = s.entries.len();
                                        s.match_index.insert(peer_id.clone(), new_index);
                                        s.next_index.insert(peer_id.clone(), new_index);
                                    } else {
                                        let next = s.next_index.get_mut(&peer_id);
                                        if let Some(n) = next {
                                            *n = n.saturating_sub(1);
                                        }
                                    }
                                }
                                Err(e) => eprintln!("{}", e)
                            },
                            Err(e) => eprintln!("{}", e)
                        }
                    });
                    tasks.push(task);
                }

                futures::future::join_all(tasks).await;
                let mut s = state.lock().await;
                update_commit_index(&mut s).await;
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

#[derive(Debug, Serialize, Deserialize)]
struct AddValueResponse {
    node_id : String,
    success : bool,
    leader : Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AddValueRequest {
    key: String, 
    value: String
}

#[derive(Debug, Serialize, Deserialize)]
struct GetValueRequest {
    key : String
}

#[derive(Debug, Serialize, Deserialize)]
struct GetValueResponse {
    node_id : String,
    key : String, 
    value : Option<String>,
    was_found: bool
}

async fn handle_get_value(State(state) : State<Arc<Mutex<AppState>>> , Json(req) : Json<GetValueRequest>) -> impl IntoResponse {

  let value = get_from_stores(&req.key).await; 
  let key = req.key.clone();

  let resp = GetValueResponse {
      was_found : value.clone().is_some(),
      value, 
      key, 
      node_id : NODE_ID.to_string()
  };

  axum::Json(resp)

}
async fn handle_add_value(State(state) : State<Arc<Mutex<AppState>>> , Json(req) : Json<AddValueRequest>) -> impl IntoResponse {

    let mut s = state.lock().await; 
    let resp = if let Roles::Leader =  s.role {
        let key = req.key; 
        let value = req.value; 
        let term = s.term; 
        let temp_key = key.clone();
        let temp_value = key.clone(); 
        let in_obj = Entry {
            key,
            value,
            term
        };

        s.entries.push(in_obj);
        s.last_log_term = term; 
        s.last_log_index = s.entries.len().saturating_sub(1);
        insert_into_temp_store(temp_key, temp_value).await;
        AddValueResponse {
            node_id : NODE_ID.to_string(),
            success : true,
            leader : None

        }
    } else {
        AddValueResponse {
            node_id : NODE_ID.to_string(),
            success : false,
            leader : Some(s.current_leader.clone())
        }
    };

    axum::Json(resp)
}

fn heartbeat_checker(state: Arc<Mutex<AppState>> , mut in_chan : Receiver<bool>) {
    let nodes : Vec<String> = Vec::new();
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _x = in_chan.recv() => (),
                _ = tokio::time::sleep(Duration::from_millis(50))  => {
                    let mut s = state.lock().await;
                    match s.role {
                        Roles::Follower => {
                            let client = reqwest::Client::new();
                            let mut futs = Vec::new();
                            s.term += 1;
                            s.role = Roles::Candidate; 

                            for node in &nodes {
                                let vote_request = VoteRequest {
                                    node_id: NODE_ID.to_string(),
                                    term: s.term,
                                    last_log_index: s.last_log_index,
                                    last_log_term: s.last_log_term,
                                };
                                s.last_voted_for = Some(NODE_ID.to_string());

                                let url = format!("http://{}/request-vote", node);
                                let fut = {
                                    let client = client.clone(); // Clone client for each task
                                    async move {
                                        client
                                            .post(url)
                                            .json(&vote_request)
                                            .send()
                                            .await?
                                            .json::<VoteResponse>()
                                            .await
                                    }
                                };

                                futs.push(fut);
                            }

                            let results: Vec<Result<VoteResponse, reqwest::Error>> = futures::future::join_all(futs).await;
                            let mut votes = Some(1); 
                            for result in results {
                                if let Ok(res) = result {
                                    if res.term > s.term {
                                        s.role = Roles::Follower; 
                                        s.term = res.term;
                                        s.last_voted_for = None; 
                                        votes = None; 
                                        break; 
                                    }
                                    if res.response {
                                        votes = votes.map(|f| f+1);
                                    }
                                }
                            }

                            if let Some(votes) = votes {
                                if votes >= (s.nodes.len() + 1) / 2 + 1 {
                                    s.role = Roles::Leader; 
                                }
                            } 
                        }
                        _ => ()

                    }
                }
            }
            // So we're not constantly spammng in the case it was successful. 
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
}


#[tokio::main]
async fn main() {
    let state = 
        Arc::new(
            Mutex::new(
                AppState { 
                    commit_index: 0,
                    match_index : HashMap::from([]),
                    next_index : HashMap::from([]),
                    nodes: HashMap::from([]),
                    term: 0, 
                    last_log_index: 0, 
                    last_log_term: 0, 
                    entries: [].to_vec(), 
                    last_voted_for : None, 
                    current_leader: String::from(""), 
                    role: Roles::Follower}));

    init_global_stores().await;
    send_append_logs(state.clone());
    let ( heartbeat_sender, heartbeat_receiver) = tokio::sync::mpsc::channel::<bool>(5);
    heartbeat_checker(state.clone(), heartbeat_receiver ) ;
    let app = 
        Router::new()
        .route("/append-entries", post(|state, req| handle_append_entry(state, req, heartbeat_sender)))
        .route("/request-vote", post(handle_request_vote))
        .route("/add-value", post(handle_add_value))
        .route("/get-value", post(handle_get_value))
        .with_state(state) ;

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
