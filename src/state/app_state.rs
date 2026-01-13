use std::sync::Arc;
use crate::components::stateful_list::StatefulList;
use crate::error::KafkaError;
use crate::models::broker_stats::{BrokerStats, ConnectionStatus};
use crate::models::topic::{Topic, TopicConfig};
use crate::protocol::record::Record;
use crate::state::filter::FilterState;
use ratatui::widgets::{ScrollbarState, TableState};
use tokio::sync::{mpsc, RwLock};
use crate::client::KafkaClient;
use crate::repository::RepositoryManager;

pub struct TopicListState {
    pub topics: StatefulList<Topic>,
    pub vertical_scroll_state: ScrollbarState,
    pub vertical_scroll: usize,
}

pub struct TopicInfoState {
    pub table_state: TableState,
    pub scroll_state: ScrollbarState,
}

pub enum AppMessage {
    TopicsLoaded(Vec<Topic>),
    TopicSelected(String),
    StatsUpdated(BrokerStats),
    MessagesLoaded(Vec<Record>),
    TopicConfigLoaded(String, TopicConfig),
    TopicDeleted(String),
    Error(String),
}

pub struct AppState {
    pub topics: StatefulList<Topic>,
    pub filtered_topic_state: TopicListState,
    pub topic_info_state: TopicInfoState,
    pub records: StatefulList<Record>,
    // pub messages: StatefulList<Message>,
    pub filter: FilterState,
    pub broker_stats: BrokerStats,
    pub breadcrumb: Vec<String>,
    pub error: Option<KafkaError>,
    pub topic_search_query: String,
    pub rx: mpsc::UnboundedReceiver<AppMessage>,
    pub tx: mpsc::UnboundedSender<AppMessage>,
    pub repository_manager: RepositoryManager
}

impl AppState {
    pub fn default(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let repository_manager = RepositoryManager::new(kafka_client);

        Self {
            topics: StatefulList::with_items(vec![]),
            filtered_topic_state: TopicListState {
                topics: StatefulList::with_items(vec![]),
                vertical_scroll_state: ScrollbarState::default(),
                vertical_scroll: 0,
            },
            topic_info_state: TopicInfoState {
                table_state: TableState::default().with_selected(0),
                scroll_state: ScrollbarState::default(),
            },
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
            tx,
            rx,
            repository_manager
        }
    }
}
