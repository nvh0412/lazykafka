use ratatui::crossterm::event::KeyCode;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Margin, Rect};
use ratatui::prelude::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Clear, Row, Scrollbar, ScrollbarOrientation, Table};
use crate::models::topic::Topic;
use crate::state::app_state::{AppState, TopicInfoState};
use crate::ui::screen::{EventResult, Screen};
use crate::ui::screen::AppAction::ClosePopup;

pub struct TopicDetailModal {
    topic_name: String,
}

impl TopicDetailModal {
    pub fn new(topic_name: String) -> Self {
        Self {
            topic_name
        }
    }

    fn render_topic_popup(&self, frame: &mut Frame, topic: &Topic, topic_info_state: &mut TopicInfoState) {
        let popup_block = Block::default().borders(Borders::NONE);

        let area = self.centered_rect(60, 60, frame.area());
        frame.render_widget(Clear, area);
        frame.render_widget(popup_block, area);

        // Create inner area for the content
        let inner_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(0)])
            .split(area);

        let topic_name = topic.name.clone();

        // Find the topic in the app state

        let content = if let Some(config) = &topic.config {
            // Sort config entries by key name for easier reading
            let mut sorted_configs: Vec<_> = config.configs.iter().collect();
            sorted_configs.sort_by(|a, b| a.0.cmp(b.0));

            // Create table rows from sorted config
            let rows: Vec<Row> = sorted_configs
                .iter()
                .map(|(key, value)| {
                    let value_str = value.value.as_deref().unwrap_or("<null>");

                    Row::new(vec![Cell::from(key.as_str()), Cell::from(value_str)])
                })
                .collect();

            topic_info_state.scroll_state = topic_info_state.scroll_state.position(10);

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
                .block(
                    Block::default()
                        .title(format!(
                            "Topic Configuration: {} (Press Esc to close)",
                            topic_name
                        ))
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Cyan)),
                )
                .style(Style::default().fg(Color::White))
        } else {
            // Config is loading
            Table::new(
                vec![Row::new(vec![Cell::from("Loading configuration...")])],
                [Constraint::Percentage(100)],
            )
                .block(
                    Block::default()
                        .title(format!(
                            "Topic Configuration: {} (Press Esc to close)",
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
}

impl Screen for TopicDetailModal {
    fn render(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        if let Some(topic) = app_state.topics.items.iter().find(|topic| topic.name == self.topic_name) {
            self.render_topic_popup(frame, topic, &mut app_state.topic_info_state)
        }
    }

    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        match key {
            KeyCode::Esc => EventResult::Action(ClosePopup),
            _ => EventResult::Handled
        }
    }
}
