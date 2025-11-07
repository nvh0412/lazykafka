use kafkatui_rs::client::KafkaClient;

use crate::{
    components::stateful_list::StatefulList, models::topic::Topic, state::filter::FilterState,
};

pub enum Screen {
    Main,
    MessageDetail,
    TopicDetail(String),
}

pub struct App {
    pub screen: Screen,
    pub client: KafkaClient,
    pub state: AppState,
}

pub struct AppState {
    pub topics: StatefulList<Topic>,
    // pub messages: StatefulList<Message>,
    pub filter: FilterState,
    // pub broker_stats: BrokerStats,
    pub breadcrumb: Vec<String>,
}
