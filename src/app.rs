use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{RwLock, mpsc};

use crate::models::broker_stats::{BrokerStats, ConnectionStatus};

use crate::{
    client::KafkaClient, components::stateful_list::StatefulList, error::KafkaError,
    models::topic::Topic, repository::RepositoryManager, state::filter::FilterState,
};
use crate::protocol::fetch::{FetchPartition, FetchTopic};
use crate::protocol::record::Record;

pub enum AppMessage {
    TopicsLoaded(Vec<Topic>),
    TopicSelected(String),
    StatsUpdated(BrokerStats),
    MessagesLoaded(Vec<Record>),
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
    repositories: RepositoryManager,
}

pub struct AppState {
    pub topics: StatefulList<Topic>,
    pub records: Vec<Record>,
    // pub messages: StatefulList<Message>,
    pub filter: FilterState,
    pub broker_stats: BrokerStats,
    pub breadcrumb: Vec<String>,
    pub error: Option<KafkaError>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            topics: StatefulList::with_items(vec![]),
            records: vec![],
            filter: Default::default(),
            breadcrumb: Default::default(),
            broker_stats: BrokerStats {
                bytes_sent: 0,
                bytes_received: 0,
                status: ConnectionStatus::default(),
                broker: "".to_string(),
            },
            error: None,
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
            repositories: RepositoryManager::new(kafka_client),
        }
    }

    pub fn on_up(&mut self) {
       self.state.topics.previous()
    }

    pub fn on_down(&mut self) {
        self.state.topics.next()
    }

    pub fn on_enter(&mut self) {
        let selected_topic = self.state.topics.selected();

        if let Some(topic) = selected_topic {
            self.select_topic(topic.name.clone());
        }
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
        let client = self.repositories.kafka_client.clone();

        let topics = vec![FetchTopic {
            topic,
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 30,
                partition_max_bytes: 1_048_576,
            }],
        }];

        tokio::spawn(async move {
            let mut client =  client.write().await;
                
            let response = client.fetch_messages(topics).await;

            match response {
                Ok(r) => {
                    let mut records = Vec::new();
                    for topic_r in r.responses {
                        for p in topic_r.paritions {
                            for batch in p.records {
                                for r in batch.records {
                                    records.push(r);
                                }
                            }
                        }
                    }

                    let _ = tx.send(AppMessage::MessagesLoaded(records));
                }
                Err(err) => {
                    println!("error {}", err)
                }
            }
        });
    }

    pub fn handle_messages(&mut self) {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                AppMessage::TopicsLoaded(topics) => {
                    self.state.topics = StatefulList::with_items(topics);
                    if !self.state.topics.items.is_empty() {
                        self.state.topics.state.select(Some(0));
                    }

                    // self.select_topic(String::from("EmploymentHero.User"));
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
                    self.state.records = records;
                }
            }
        }
    }
}
