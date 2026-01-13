use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::state::app_state::{AppMessage, AppState, InputMode};
use crate::ui::screen::{AppAction, EventResult};
use crate::ui::screen_manager::ScreenManager;
use crate::ui::screen_type::ScreenType;
use crate::{
    client::KafkaClient,
    components::stateful_list::StatefulList
    ,
    models::topic::TopicConfig,
    repository::RepositoryManager
    ,
};

pub enum Screen {
    Main,
    MessageDetail,
    TopicDetail(String),
}

pub struct App {
    pub screen: Screen,
    pub screen_manager: ScreenManager,
    pub state: AppState,
    repositories: RepositoryManager,
}

impl App {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        Self {
            screen: Screen::Main,
            state: AppState::default(),
            screen_manager: ScreenManager::default(),
            repositories: RepositoryManager::new(kafka_client),
        }
    }

    pub fn handle_event_result(&mut self, event_result: &mut EventResult) -> bool {
        match event_result {
            EventResult::Action(action) => match action {
                AppAction::LoadMessages(topic) => {
                    self.load_messages_for_topic(topic.clone());
                    true
                }
                AppAction::LoadTopics => {
                    self.load_topics();
                    true
                }
                AppAction::LoadTopicConfig(topic) => {
                    self.load_topic_config(topic.clone());
                    true
                }
                AppAction::ClosePopup => {
                    self.close_popup();
                    true
                }
                AppAction::ToggleSearchMode => {
                    self.toggle_search_mode();
                    true
                }
                AppAction::SearchTopic(c) => {
                    self.on_search_char(c.clone());
                    true
                }
                AppAction::SearchTopicBackspace => {
                    self.on_search_backspace();
                    true
                }
            },
            EventResult::Push(screen_type) => {
                match screen_type {
                    ScreenType::TopicDetail(topic_name) => {
                        self.load_topic_config(topic_name.clone());
                    }
                    _ => {}
                }
                self.screen_manager.push(screen_type.clone());
                true
            }
            EventResult::Quit => false,
            _ => true,
        }
    }

    pub fn close_popup(&mut self) {
        self.screen_manager.pop();
        self.state.input_mode = InputMode::Normal;
    }

    pub fn toggle_search_mode(&mut self) {
        self.state.input_mode = match self.state.input_mode {
            InputMode::Normal => InputMode::SearchTopic,
            InputMode::SearchTopic => InputMode::Normal,
            _ => InputMode::Normal,
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

    pub fn update_filters(&mut self) {
        self.state.filtered_topic_state.topics.update_items(
            self.state
                .topics
                .items
                .iter()
                .filter(|topic| topic.name.contains(&self.state.topic_search_query))
                .cloned()
                .collect(),
        );
    }

    pub fn start_stats_polling(&self) {
        let tx = self.state.tx.clone();
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
        let tx = self.state.tx.clone();
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

    pub fn load_messages_for_topic(&self, topic: String) {
        let tx = self.state.tx.clone();
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
        let tx = self.state.tx.clone();
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
        while let Ok(msg) = self.state.rx.try_recv() {
            match msg {
                AppMessage::TopicsLoaded(topics) => {
                    self.state.topics = StatefulList::with_items(topics);
                    self.state.filtered_topic_state.topics =
                        StatefulList::with_items(self.state.topics.items.clone());
                    if !self.state.topics.items.is_empty() {
                        self.state.topics.state.select(Some(0));
                        self.state.filtered_topic_state.topics.state.select(Some(0));
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
                    if let Some(topic) = self
                        .state
                        .topics
                        .items
                        .iter_mut()
                        .find(|t| t.name == topic_name)
                    {
                        topic.config = Some(config.clone());
                    }
                }
            }
        }
    }
}
