use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{RwLock, mpsc};

use crate::models::broker_stats::{BrokerStats, ConnectionStatus};

use crate::protocol::fetch::{FetchPartition, FetchTopic};
use crate::protocol::record::Record;
use crate::{
    client::KafkaClient, components::stateful_list::StatefulList, error::KafkaError,
    models::topic::{Topic, TopicConfig}, repository::RepositoryManager, state::filter::FilterState,
};

pub enum AppMessage {
    TopicsLoaded(Vec<Topic>),
    TopicSelected(String),
    StatsUpdated(BrokerStats),
    MessagesLoaded(Vec<Record>),
    TopicConfigLoaded(String, TopicConfig),
    Error(String),
}

pub enum Screen {
    Main,
    MessageDetail,
    TopicDetail(String),
}

pub enum InputMode {
    Normal,
    SearchTopic,
}

pub struct App {
    pub screen: Screen,
    pub input_mode: InputMode,
    pub rx: mpsc::UnboundedReceiver<AppMessage>,
    tx: mpsc::UnboundedSender<AppMessage>,
    pub state: AppState,
    repositories: RepositoryManager,
}

pub struct AppState {
    pub topics: StatefulList<Topic>,
    pub filtered_topic: StatefulList<Topic>,
    pub records: StatefulList<Record>,
    // pub messages: StatefulList<Message>,
    pub filter: FilterState,
    pub broker_stats: BrokerStats,
    pub breadcrumb: Vec<String>,
    pub error: Option<KafkaError>,
    pub topic_search_query: String,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            topics: StatefulList::with_items(vec![]),
            filtered_topic: StatefulList::with_items(vec![]),
            records: StatefulList::with_items(vec![]),
            filter: Default::default(),
            breadcrumb: Default::default(),
            broker_stats: BrokerStats {
                bytes_sent: 0,
                bytes_received: 0,
                status: ConnectionStatus::default(),
                broker: "".to_string(),
            },
            error: None,
            topic_search_query: String::new(),
        }
    }
}

impl App {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        Self {
            screen: Screen::Main,
            input_mode: InputMode::Normal,
            state: AppState::default(),
            rx,
            tx,
            repositories: RepositoryManager::new(kafka_client),
        }
    }

    pub fn on_up(&mut self) {
        self.state.filtered_topic.previous()
    }

    pub fn on_down(&mut self) {
        self.state.filtered_topic.next()
    }

    pub fn on_enter(&mut self) {
        let selected_topic = self.state.filtered_topic.selected();

        if let Some(topic) = selected_topic {
            self.select_topic(topic.name.clone());
        }
    }
    
    pub fn show_topic_info(&mut self) {
        let selected_topic = self.state.filtered_topic.selected();

        if let Some(topic) = selected_topic {
            self.screen = Screen::TopicDetail(topic.name.clone());
            self.load_topic_config(topic.name.clone());
        } else {
            self.screen = Screen::Main;
        }
    }
    
    pub fn close_popup(&mut self) {
        self.screen = Screen::Main;
    }

    pub fn toggle_search_mode(&mut self) {
        self.input_mode = match self.input_mode {
            InputMode::Normal => InputMode::SearchTopic,
            InputMode::SearchTopic => InputMode::Normal,
        };
    }

    pub fn on_search_char(&mut self, c: char) {
        self.state.topic_search_query.push(c);
        self.update_filters();
    }

    pub fn on_search_backspace(&mut self) {
        self.state.topic_search_query.pop();
        self.update_filters();
    }

    pub fn clear_search(&mut self) {
        self.state.topic_search_query.clear();
        self.state.filtered_topic.update_items(self.state.topics.items.clone());
    }

    pub fn update_filters(&mut self) {
        self.state.filtered_topic.update_items(
            self.state.topics.items.iter().filter(|topic| topic.name.contains(&self.state.topic_search_query)).cloned().collect()
        );
    }

    pub fn start_stats_polling(&self) {
        let tx = self.tx.clone();
        let client = self.repositories.kafka_client.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;

                // Read lock is fine since get_stats() only reads
                let stats = client.read().await.get_stats();
                let _ = tx.send(AppMessage::StatsUpdated(stats));
            }
        });
    }

    pub fn load_topics(&self) {
        let tx = self.tx.clone();
        let repo = self.repositories.topics().clone();

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

    pub fn select_topic(&self, topic: String) {
        self.tx.send(AppMessage::TopicSelected(topic)).unwrap();
    }

    pub fn load_messages_for_topic(&self, topic: String) {
        let tx = self.tx.clone();
        let repo = self.repositories.messages().clone();

        tokio::spawn(async move {
            match repo.fetch_by_topic(topic).await {
                Ok(records) => {
                    let _ = tx.send(AppMessage::MessagesLoaded(records));
                }
                Err(e) => {
                    let _ = tx.send(AppMessage::Error(e.to_string()));
                }
            }
        });
    }

    pub fn load_topic_config(&self, topic_name: String) {
        let tx = self.tx.clone();
        let client = self.repositories.kafka_client.clone();

        tokio::spawn(async move {
            let mut client = client.write().await;
            match client.fetch_topic_configs(topic_name.clone()).await {
                Ok(response) => {
                    if let Some(resource) = response.resources.first() {
                        let config = TopicConfig::from(resource);
                        let _ = tx.send(AppMessage::TopicConfigLoaded(topic_name, config));
                    }
                }
                Err(e) => {
                    let _ = tx.send(AppMessage::Error(e.to_string()));
                }
            }
        });
    }

    pub fn handle_messages(&mut self) {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                AppMessage::TopicsLoaded(topics) => {
                    self.state.topics = StatefulList::with_items(topics);
                    self.state.filtered_topic = StatefulList::with_items(self.state.topics.items.clone());
                    if !self.state.topics.items.is_empty() {
                        self.state.topics.state.select(Some(0));
                    }
                }
                AppMessage::TopicSelected(name) => {
                    self.load_messages_for_topic(name);
                }
                AppMessage::StatsUpdated(stats) => {
                    self.state.broker_stats = stats;
                }
                AppMessage::Error(err) => {
                    // self.state.error = Some(err);
                }
                AppMessage::MessagesLoaded(records) => {
                    self.state.records = StatefulList::with_items(records);
                }
                AppMessage::TopicConfigLoaded(topic_name, config) => {
                    // Update the topic in the topics list
                    if let Some(topic) = self.state.topics.items.iter_mut().find(|t| t.name == topic_name) {
                        topic.config = Some(config.clone());
                    }

                    // Update the topic in the filtered list
                    if let Some(topic) = self.state.filtered_topic.items.iter_mut().find(|t| t.name == topic_name) {
                        topic.config = Some(config);
                    }
                }
            }
        }
    }
}
