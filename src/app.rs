use std::sync::Arc;

use tokio::sync::{RwLock, mpsc};

use crate::{
    client::KafkaClient, components::stateful_list::StatefulList, models::topic::Topic,
    repository::topic_repository::TopicRepository, state::filter::FilterState,
};

pub enum AppMessage {
    TopicsLoaded(Vec<Topic>),
    TopicSelected(String),
    // MessagesLoaded(Vec<Message>),
    Error(String),
}

pub enum Screen {
    Main,
    MessageDetail,
    TopicDetail(String),
}

pub struct App {
    pub screen: Screen,
    pub rx: mpsc::UnboundedReceiver<AppMessage>,
    tx: mpsc::UnboundedSender<AppMessage>,
    pub state: AppState,
    repository: TopicRepository,
}

pub struct AppState {
    pub topics: StatefulList<Topic>,
    // pub messages: StatefulList<Message>,
    pub filter: FilterState,
    // pub broker_stats: BrokerStats,
    pub breadcrumb: Vec<String>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            topics: StatefulList::with_items(vec![]),
            filter: Default::default(),
            breadcrumb: Default::default(),
        }
    }
}

impl App {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        Self {
            screen: Screen::Main,
            state: AppState::default(),
            rx,
            tx,
            repository: TopicRepository::new(kafka_client),
        }
    }

    pub fn load_topics(&self) {
        let tx = self.tx.clone();
        let repo = self.repository.clone();

        tokio::spawn(async move {
            match repo.fetch_all().await {
                Ok(topics) => {
                    let _ = tx.send(AppMessage::TopicsLoaded(topics));
                }
                Err(e) => {
                    let _ = tx.send(AppMessage::Error(e.to_string()));
                }
            }
        });
    }
}
