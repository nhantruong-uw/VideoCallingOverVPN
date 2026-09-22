use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;
use std::sync::atomic::{AtomicU64, Ordering};
use axum::extract::State;
use futures_util::{SinkExt, StreamExt};
use serde::{Serialize, Deserialize};
use tower_http::services::ServeDir;
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

type ClientId = u64;

struct ClientInfo {
    name: String,
    tx: Option<UnboundedSender<Message>>,
}

type SharedState = Arc<Mutex<HashMap<ClientId, ClientInfo>>>;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
enum MessageType {
    Offer,
    Answer,
    IceCandidate,
    Identify,
    Roster,
}

#[derive(Serialize, Deserialize)]
struct SignalMessage {
    #[serde(rename = "type")]
    msg_type: MessageType,
    sender: ClientId,
    target: ClientId,
    payload: serde_json::Value,
}

#[derive(Serialize, Deserialize)]
struct IdentifyPayload {
    name: String,
}

#[derive(Serialize)]
struct RosterEntry {
    id: ClientId,
    name: String,
    online: bool,
}

fn broadcast_roster(state: &SharedState) {
    let map = state.lock().unwrap();

    let peers: Vec<RosterEntry> = map
        .iter()
        .map(|(&id, info)| RosterEntry {
            id,
            name: info.name.clone(),
            online: info.tx.is_some(),
        })
        .collect();

    let payload = serde_json::to_value(&peers).unwrap();

    for info in map.values() {
        if let Some(tx) = &info.tx {
            let msg = SignalMessage {
                msg_type: MessageType::Roster,
                sender: 0,
                target: 0,
                payload: payload.clone(),
            };
            let text = serde_json::to_string(&msg).unwrap();
            let _ = tx.send(Message::Text(text.into()));
        }
    }
}

#[tokio::main]
async fn main() {
    let state: SharedState = Arc::new(Mutex::new(HashMap::new()));

    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state)
        .fallback_service(ServeDir::new("web"));

    let tls_config = RustlsConfig::from_pem_file("certs/cert.pem", "certs/key.pem")
        .await
        .unwrap();

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("signaling server listening on https://{addr}");

    axum_server::bind_rustls(addr, tls_config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn ws_handler(State(state): State<SharedState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: SharedState) {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    state.lock().unwrap().insert(
        id,
        ClientInfo {
            name: format!("Peer {id}"),
            tx: Some(tx),
        },
    );
    tracing::info!("client connected: id={id}");
    broadcast_roster(&state);

    let (mut sender, mut receiver) = socket.split();

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    while let Some(Ok(msg)) = receiver.next().await {
        if let Message::Text(text) = &msg {
            match serde_json::from_str::<SignalMessage>(text) {
                Ok(mut signal) => {
                    signal.sender = id;

                    if let MessageType::Identify = signal.msg_type {
                        match serde_json::from_value::<IdentifyPayload>(signal.payload) {
                            Ok(identify) => {
                                if let Some(info) = state.lock().unwrap().get_mut(&id) {
                                    info.name = identify.name;
                                }
                                broadcast_roster(&state);
                            }
                            Err(e) => {
                                tracing::warn!("failed to parse identify payload: {e}");
                            }
                        }
                        continue;
                    }

                    tracing::info!("relaying {:?} from {} to {}",
                        signal.msg_type, id, signal.target);
                    let pkg = Message::Text(serde_json::to_string(&signal).unwrap().into());
                    let map = state.lock().unwrap();
                    let other_tx = map.get(&signal.target).and_then(|info| info.tx.as_ref());
                    match other_tx {
                        Some(tx) => {
                            if tx.send(pkg).is_err() {
                                tracing::warn!("failed to deliver to {}", signal.target);
                            }
                        }
                        None => {
                            tracing::warn!("target {} not connected", signal.target);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("failed to parse message: {e}");
                    continue;
                }
            }
        }
    }

    if let Some(info) = state.lock().unwrap().get_mut(&id) {
        info.tx = None;
    }
    tracing::info!("client disconnected: id={id}");
    broadcast_roster(&state);
}
