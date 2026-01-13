use std::fmt::format;
use ratatui::crossterm::event::KeyCode;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use crate::models::topic::TopicConfig;
use crate::state::app_state::{AppMessage, AppState};
use crate::ui::screen::{AppAction, ConfirmAction, EventResult, Screen};

pub struct ConfirmDeleteTopicDialogModal {
    topic_name: String,
}

impl ConfirmDeleteTopicDialogModal {
    pub fn new(topic_name: String) -> Self {
        Self { topic_name }
    }

    fn centered_rect(&self, percent_x: u16, percent_y: u16, r: Rect) -> Rect {
        // Cut the given rectangle into three vertical pieces
        let popup_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage((100 - percent_y) / 2),
                Constraint::Percentage(percent_y),
                Constraint::Percentage((100 - percent_y) / 2),
            ])
            .split(r);

        // Then cut the middle vertical piece into three width-wise pieces
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage((100 - percent_x) / 2),
                Constraint::Percentage(percent_x),
                Constraint::Percentage((100 - percent_x) / 2),
            ])
            .split(popup_layout[1])[1] // Return the middle chunk
    }

    fn delete_topics(&self, app_state: &mut AppState) {
        let tx = app_state.tx.clone();
        let client = app_state.repository_manager.kafka_client.clone();

        let topic_name = self.topic_name.clone();

        tokio::spawn(async move {
            let mut client = client.write().await;
            match client.delete_topics(vec![topic_name.clone()]).await {
                Ok(response) => {
                    let error_codes = response.topic_error_codes;

                    for error_code in error_codes {
                        if error_code.error_code == 0 {
                            let _ = tx.send(AppMessage::TopicDeleted(topic_name.clone()));
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(AppMessage::Error(e.to_string()));
                }
            }
        });
    }
}

impl Screen for ConfirmDeleteTopicDialogModal {
    fn render(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let popup_block = Block::default()
            .title("Delete topic?".to_string())
            .title_style(ratatui::style::Style::default().fg(ratatui::style::Color::Red))
            .borders(Borders::ALL)
            .border_style(ratatui::style::Style::default().fg(ratatui::style::Color::Red))
            .title_bottom(Line::from("Yes (Y/y)").right_aligned())
            .title_bottom(Line::from("No (N/n)").right_aligned());

        let area = self.centered_rect(30, 20, frame.area());
        frame.render_widget(Clear, area);

        let content = Paragraph::new(format!("Are you sure you want to delete {} topic?", self.topic_name))
            .wrap(Wrap { trim: true })
            .block(popup_block);

        frame.render_widget(content, area);
    }

    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        match key {
            KeyCode::Char('y') | KeyCode::Char('Y') => {
                self.delete_topics(app_state);
                EventResult::Action(AppAction::DeletedTopic(self.topic_name.clone()))
            },
            KeyCode::Char('n') | KeyCode::Char('N') => EventResult::Quit(ConfirmAction::Decline),
            _ => EventResult::Handled
        }
    }

    fn is_modal(&self) -> bool { true }
}
