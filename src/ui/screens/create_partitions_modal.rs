use crate::state::app_state::{AppMessage, AppState};
use crate::ui::screen::{AppAction, EventResult, Screen};
use ratatui::Frame;
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::{Color, Line, Modifier, Span, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

pub struct CreatePartitionsModal {
    topic_name: String,
    current_partitions: i32,
    new_partition_count: String,
    error_message: Option<String>,
}

impl CreatePartitionsModal {
    pub fn new(topic_name: String, current_partitions: i32) -> Self {
        Self {
            topic_name,
            current_partitions,
            new_partition_count: String::new(),
            error_message: None,
        }
    }

    fn centered_rect(&self, percent_x: u16, percent_y: u16, r: Rect) -> Rect {
        let popup_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage((100 - percent_y) / 2),
                Constraint::Percentage(percent_y),
                Constraint::Percentage((100 - percent_y) / 2),
            ])
            .split(r);

        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage((100 - percent_x) / 2),
                Constraint::Percentage(percent_x),
                Constraint::Percentage((100 - percent_x) / 2),
            ])
            .split(popup_layout[1])[1]
    }

    fn validate_input(&self) -> Result<i32, String> {
        if self.new_partition_count.is_empty() {
            return Err("Please enter a partition count".to_string());
        }

        let count: i32 = self
            .new_partition_count
            .parse()
            .map_err(|_| "Invalid number".to_string())?;

        if count <= self.current_partitions {
            return Err(format!(
                "New count must be greater than {}",
                self.current_partitions
            ));
        }

        Ok(count)
    }

    fn create_partitions(&self, app_state: &mut AppState, new_count: i32) {
        let tx = app_state.tx.clone();
        let client = app_state.repository_manager.kafka_client.clone();
        let topic_name = self.topic_name.clone();

        tokio::spawn(async move {
            let mut client = client.write().await;
            match client
                .create_partitions(topic_name.clone(), new_count, 30000)
                .await
            {
                Ok(response) => {
                    if let Some(error) = response.first_error() {
                        let error_msg = error
                            .error_description()
                            .unwrap_or_else(|| "Unknown error".to_string());
                        let _ = tx.send(AppMessage::CreatePartitionsError(error_msg));
                    } else {
                        let _ = tx.send(AppMessage::PartitionsCreated);
                    }
                }
                Err(e) => {
                    let _ = tx.send(AppMessage::CreatePartitionsError(e.to_string()));
                }
            }
        });
    }
}

impl Screen for CreatePartitionsModal {
    fn render(&mut self, frame: &mut Frame, _area: Rect, _app_state: &mut AppState) {
        let area = self.centered_rect(40, 30, frame.area());
        frame.render_widget(Clear, area);

        let popup_block = Block::default()
            .title(" Add Partitions ")
            .title_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));

        // Split the modal into sections
        let inner = popup_block.inner(area);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(2), // Topic name
                Constraint::Length(2), // Current partitions
                Constraint::Length(3), // Input field
                Constraint::Length(2), // Error/hint message
                Constraint::Min(0),    // Spacer
                Constraint::Length(1), // Footer
            ])
            .split(inner);

        frame.render_widget(popup_block, area);

        // Topic name
        let topic_line = Line::from(vec![
            Span::styled("Topic: ", Style::default().fg(Color::Gray)),
            Span::styled(&self.topic_name, Style::default().fg(Color::White)),
        ]);
        frame.render_widget(Paragraph::new(topic_line), chunks[0]);

        // Current partitions
        let current_line = Line::from(vec![
            Span::styled("Current partitions: ", Style::default().fg(Color::Gray)),
            Span::styled(
                self.current_partitions.to_string(),
                Style::default().fg(Color::Yellow),
            ),
        ]);
        frame.render_widget(Paragraph::new(current_line), chunks[1]);

        // Input field
        let input_display = if self.new_partition_count.is_empty() {
            format!("[{}]", "_".repeat(10))
        } else {
            format!("[{}{}]", self.new_partition_count, "_")
        };

        let input_line = Line::from(vec![
            Span::styled("New partition count: ", Style::default().fg(Color::Gray)),
            Span::styled(input_display, Style::default().fg(Color::Green)),
        ]);
        frame.render_widget(Paragraph::new(input_line), chunks[2]);

        // Error or hint message
        let message = if let Some(ref error) = self.error_message {
            Line::from(Span::styled(error, Style::default().fg(Color::Red)))
        } else {
            Line::from(Span::styled(
                "Must be greater than current count",
                Style::default().fg(Color::DarkGray),
            ))
        };
        frame.render_widget(Paragraph::new(message), chunks[3]);

        // Footer with keybindings
        let footer = Line::from(vec![
            Span::styled("[Enter]", Style::default().fg(Color::Green)),
            Span::raw(" Confirm  "),
            Span::styled("[Esc]", Style::default().fg(Color::Yellow)),
            Span::raw(" Cancel"),
        ]);
        frame.render_widget(Paragraph::new(footer), chunks[5]);
    }

    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        match key {
            KeyCode::Char(c) if c.is_ascii_digit() => {
                // Limit input length
                if self.new_partition_count.len() < 10 {
                    self.new_partition_count.push(c);
                    self.error_message = None;
                }
                EventResult::Handled
            }
            KeyCode::Backspace => {
                self.new_partition_count.pop();
                self.error_message = None;
                EventResult::Handled
            }
            KeyCode::Enter => match self.validate_input() {
                Ok(new_count) => {
                    self.create_partitions(app_state, new_count);
                    EventResult::Action(AppAction::PartitionsCreated {
                        topic: self.topic_name.clone(),
                        new_count,
                    })
                }
                Err(msg) => {
                    self.error_message = Some(msg);
                    EventResult::Handled
                }
            },
            KeyCode::Esc => EventResult::Pop,
            _ => EventResult::Handled,
        }
    }

    fn is_modal(&self) -> bool {
        true
    }
}
