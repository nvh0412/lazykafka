use crate::models::topic::{Topic, TopicConfig};
use crate::state::app_state::{AppMessage, AppState, TopicInfoState};
use crate::ui::screen::AppAction::ClosePopup;
use crate::ui::screen::{EventResult, Screen};
use ratatui::Frame;
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Margin, Rect};
use ratatui::prelude::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Clear, Row, Scrollbar, ScrollbarOrientation, Table};

pub struct TopicDetailModal {
    topic_name: String,
    scroll_offset: usize,
    config_count: usize,
    visible_height: u16,
}

impl TopicDetailModal {
    pub fn new(topic_name: String) -> Self {
        Self {
            topic_name,
            scroll_offset: 0,
            config_count: 0,
            visible_height: 10,
        }
    }

    fn render_topic_popup(
        &mut self,
        frame: &mut Frame,
        topic: &Topic,
        topic_info_state: &mut TopicInfoState,
    ) {
        let popup_block = Block::default().borders(Borders::NONE);

        let area = self.centered_rect(60, 60, frame.area());
        frame.render_widget(Clear, area);
        frame.render_widget(popup_block, area);

        let topic_name = topic.name.clone();

        // Calculate visible height (area height - borders - header - header margin)
        self.visible_height = area.height.saturating_sub(4);

        let content = if let Some(config) = &topic.config {
            // Sort config entries by key name for easier reading
            let mut sorted_configs: Vec<_> = config.configs.iter().collect();
            sorted_configs.sort_by(|a, b| a.0.cmp(b.0));

            // Track config count for scroll calculation
            self.config_count = sorted_configs.len();

            // Create table rows from sorted config
            let rows: Vec<Row> = sorted_configs
                .iter()
                .map(|(key, value)| {
                    let value_str = value.value.as_deref().unwrap_or("<null>");

                    Row::new(vec![Cell::from(key.as_str()), Cell::from(value_str)])
                })
                .collect();

            // Build scroll indicator for title
            let scroll_info = if self.config_count > 0 {
                format!(
                    " [{}/{}]",
                    (self.scroll_offset + 1).min(self.config_count),
                    self.config_count
                )
            } else {
                String::new()
            };

            // Update table state selection to current scroll position
            topic_info_state
                .table_state
                .select(Some(self.scroll_offset));

            // Update scrollbar state
            topic_info_state.scroll_state = topic_info_state
                .scroll_state
                .content_length(self.config_count)
                .position(self.scroll_offset);

            Table::new(
                rows,
                [Constraint::Percentage(50), Constraint::Percentage(50)],
            )
            .header(
                Row::new(vec![
                    Cell::from("Config Key").style(Style::default().add_modifier(Modifier::BOLD)),
                    Cell::from("Value").style(Style::default().add_modifier(Modifier::BOLD)),
                ])
                .style(Style::default().fg(Color::Yellow))
                .bottom_margin(1),
            )
            .row_highlight_style(Style::default().bg(Color::DarkGray))
            .block(
                Block::default()
                    .title(format!(
                        "Topic Configuration: {}{} (↑↓ scroll, Esc close)",
                        topic_name, scroll_info
                    ))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Cyan)),
            )
            .style(Style::default().fg(Color::White))
        } else {
            // Config is loading
            self.config_count = 0;
            Table::new(
                vec![Row::new(vec![Cell::from("Loading configuration...")])],
                [Constraint::Percentage(100)],
            )
            .block(
                Block::default()
                    .title(format!(
                        "Topic Configuration: {} (↑↓ scroll, Esc close)",
                        topic_name
                    ))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Cyan)),
            )
        };

        frame.render_stateful_widget(content, area, &mut topic_info_state.table_state);
        frame.render_stateful_widget(
            Scrollbar::default()
                .orientation(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None),
            area.inner(Margin {
                vertical: 1,
                horizontal: 1,
            }),
            &mut topic_info_state.scroll_state,
        );
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

    fn load_topic_config(&mut self, app_state: &mut AppState, topic_name: String) {
        let tx = app_state.tx.clone();
        let client = app_state.repository_manager.kafka_client.clone();

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
}

impl Screen for TopicDetailModal {
    fn render(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        if let Some(topic) = app_state
            .topics
            .items
            .iter()
            .find(|topic| topic.name == self.topic_name)
        {
            self.render_topic_popup(frame, topic, &mut app_state.topic_info_state)
        } else {
            let popup_area = self.centered_rect(80, 80, area);
            frame.render_widget(Clear, popup_area);
            let not_found_block = Block::default()
                .title(format!("Topic '{}' not found", self.topic_name))
                .borders(Borders::ALL);
            frame.render_widget(not_found_block, popup_area);
        }
    }

    fn handle_key(&mut self, key: KeyCode, _app_state: &mut AppState) -> EventResult {
        let max_scroll = self.config_count.saturating_sub(1);

        match key {
            KeyCode::Esc => EventResult::Action(ClosePopup),
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
                EventResult::Handled
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll_offset = (self.scroll_offset + 1).min(max_scroll);
                EventResult::Handled
            }
            KeyCode::PageUp => {
                self.scroll_offset = self.scroll_offset.saturating_sub(10);
                EventResult::Handled
            }
            KeyCode::PageDown => {
                self.scroll_offset = (self.scroll_offset + 10).min(max_scroll);
                EventResult::Handled
            }
            KeyCode::Home | KeyCode::Char('g') => {
                self.scroll_offset = 0;
                EventResult::Handled
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.scroll_offset = max_scroll;
                EventResult::Handled
            }
            _ => EventResult::Handled,
        }
    }

    fn on_mount(&mut self, app_state: &mut AppState) {
        self.load_topic_config(app_state, self.topic_name.clone());
    }
}
