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

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

type ClientId = u64;
type SharedState = Arc<Mutex<HashMap<ClientId, UnboundedSender<Message>>>>;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
enum MessageType {
    Offer,
    Answer,
    IceCandidate,
}

#[derive(Serialize, Deserialize)]
struct SignalMessage {
    #[serde(rename = "type")]
    msg_type: MessageType,
    sender: ClientId,
    target: ClientId,
    payload: serde_json::Value,
}

#[tokio::main]
async fn main() {
    let state: SharedState = Arc::new(Mutex::new(HashMap::new()));

    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state)
        .fallback_service(ServeDir::new("web"));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .unwrap();
    tracing::info!("signaling server listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(State(state): State<SharedState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: SharedState) {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    // let tx_clone = tx.clone();
    state.lock().unwrap().insert(id, tx);
    tracing::info!("client connected");

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
                    // good signal
                    signal.sender = id;
                    tracing::info!("relaying {:?} from {} to {}", 
                        signal.msg_type, id, signal.target);
                    let pkg = Message::Text(serde_json::to_string(&signal).unwrap().into());
                    let map = state.lock().unwrap();
                    let other_tx = map.get(&signal.target);
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

    state.lock().unwrap().remove(&id);
    tracing::info!("client disconnected");
}
