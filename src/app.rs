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
};

pub struct App {
    pub screen_manager: ScreenManager,
    pub state: AppState,
}

impl App {
    pub fn new(kafka_client: Arc<RwLock<KafkaClient>>) -> Self {
        let mut state = AppState::default(kafka_client);
        let screen_manager = ScreenManager::default(&mut state);
        Self {
            state,
            screen_manager,
        }
    }

    pub fn handle_event_result(&mut self, event_result: &mut EventResult) -> bool {
        match event_result {
            EventResult::Action(action) => match action {
                AppAction::LoadMessages(topic) => {
                    self.load_messages_for_topic(topic.clone());
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
                self.screen_manager.push(screen_type.clone(), &mut self.state);
                true
            }
            EventResult::Quit => false,
            _ => true,
        }
    }

    pub fn close_popup(&mut self) {
        self.screen_manager.pop(&mut self.state);
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

    pub fn load_messages_for_topic(&self, topic: String) {
        let tx = self.state.tx.clone();
        let repo = self.state.repository_manager.messages().clone();

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
