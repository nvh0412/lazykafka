use std::sync::Arc;
use tokio::sync::RwLock;

use crate::state::app_state::{AppMessage, AppState};
use crate::ui::screen::{AppAction, ConfirmAction, EventResult};
use crate::ui::screen_manager::ScreenManager;
use crate::ui::screen_type::ScreenType;
use crate::{client::KafkaClient, components::stateful_list::StatefulList};

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
                AppAction::ClosePopup => {
                    self.close_popup();
                    true
                }
                AppAction::ConfirmExitApp => {
                    self.screen_manager.push(
                        ScreenType::ConfirmDialog {
                            title: "Leave app?".to_string(),
                            message: "Are you sure you want to exit?".to_string(),
                        },
                        &mut self.state,
                    );
                    true
                },
                AppAction::DeletedTopic(t) =>  {
                    self.screen_manager.pop(&mut self.state);
                    true
                }
            },
            EventResult::Push(screen_type) => {
                self.screen_manager
                    .push(screen_type.clone(), &mut self.state);
                true
            }
            EventResult::Quit(confirm_action) => match confirm_action {
                ConfirmAction::Accept => false,
                ConfirmAction::Decline => {
                    self.screen_manager.pop(&mut self.state);
                    true
                }
            },
            _ => true,
        }
    }

    pub fn close_popup(&mut self) {
        self.screen_manager.pop(&mut self.state);
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
                },
                AppMessage::TopicDeleted(t) => {
                    self
                        .state
                        .topics
                        .items
                        .retain_mut(|topic| topic.name != t);
                    self.state.filtered_topic_state.topics.update_items(self.state.topics.items.clone());
                }
            }
        }
    }
}
