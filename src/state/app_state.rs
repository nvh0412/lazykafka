use crate::client::KafkaClient;
use crate::components::stateful_list::StatefulList;
use crate::error::KafkaError;
use crate::models::broker_stats::{BrokerStats, ConnectionStatus};
use crate::models::consumer_group::{ConsumerGroupDetail, ConsumerGroupSummary};
use crate::models::topic::{Topic, TopicConfig};
use crate::protocol::record::Record;
use crate::repository::RepositoryManager;
use crate::state::filter::FilterState;
use ratatui::widgets::{ScrollbarState, TableState};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

pub struct TopicListState {
    pub topics: StatefulList<Topic>,
    pub vertical_scroll_state: ScrollbarState,
    pub vertical_scroll: usize,
}

pub struct TopicInfoState {
    pub table_state: TableState,
    pub scroll_state: ScrollbarState,
}

// Consumer Group State
pub struct ConsumerGroupListState {
    pub items: Vec<ConsumerGroupSummary>,
    pub groups: StatefulList<ConsumerGroupSummary>,
    pub vertical_scroll_state: ScrollbarState,
    pub vertical_scroll: usize,
}

impl Default for ConsumerGroupListState {
    fn default() -> Self {
        Self {
            items: vec![],
            groups: StatefulList::with_items(vec![]),
            vertical_scroll_state: ScrollbarState::default(),
            vertical_scroll: 0,
        }
    }
}

pub struct ConsumerGroupDetailState {
    pub detail: Option<ConsumerGroupDetail>,
    pub members_table_state: TableState,
    pub offsets_table_state: TableState,
    pub active_tab: ConsumerGroupTab,
}

impl Default for ConsumerGroupDetailState {
    fn default() -> Self {
        Self {
            detail: None,
            members_table_state: TableState::default(),
            offsets_table_state: TableState::default(),
            active_tab: ConsumerGroupTab::Overview,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsumerGroupTab {
    #[default]
    Overview,
    Members,
    Offsets,
}

pub enum AppMessage {
    TopicsLoaded(Vec<Topic>),
    StatsUpdated(BrokerStats),
    MessagesLoaded(Vec<Record>),
    TopicConfigLoaded(String, TopicConfig),
    TopicDeleted(String),
    Error(String),
    // Consumer group messages
    ConsumerGroupsLoaded(Vec<ConsumerGroupSummary>),
    ConsumerGroupDetailLoaded(ConsumerGroupDetail),
    ConsumerGroupError(String),
}

#[allow(dead_code)]
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
    pub repository_manager: RepositoryManager,
    // Consumer group state
    pub consumer_groups: StatefulList<ConsumerGroupSummary>,
    pub filtered_consumer_groups_state: ConsumerGroupListState,
    pub consumer_group_detail_state: ConsumerGroupDetailState,
    pub consumer_group_search_query: String,
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
            repository_manager,
            // Consumer group state
            consumer_groups: StatefulList::with_items(vec![]),
            filtered_consumer_groups_state: ConsumerGroupListState::default(),
            consumer_group_detail_state: ConsumerGroupDetailState::default(),
            consumer_group_search_query: String::new(),
        }
    }
}
