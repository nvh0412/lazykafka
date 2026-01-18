use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::{Color, Line, Modifier, Span, Style, Stylize};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};
use ratatui::Frame;

use crate::models::protocol_log::{ApiType, LogDirection, LogStatus, ProtocolLogEntry, ProtocolLogFilter};
use crate::state::app_state::AppState;

use super::PanelResult;

/// Panel displaying protocol logs at the bottom of the screen
pub struct LogsPanel {
    /// Whether this panel currently has focus
    pub is_focused: bool,
    /// Visible height for scroll calculations
    visible_height: usize,
}

impl LogsPanel {
    pub fn new() -> Self {
        Self {
            is_focused: false,
            visible_height: 5,
        }
    }

    /// Toggle focus state
    pub fn toggle_focus(&mut self) {
        self.is_focused = !self.is_focused;
    }

    /// Render the logs panel
    pub fn render(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        // Store visible height for scroll calculations
        self.visible_height = area.height.saturating_sub(3) as usize; // Subtract header and borders

        // Layout: Header (1 line) + Log entries
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // Header
                Constraint::Min(0),    // Log entries
            ])
            .split(area);

        // Render header
        self.render_header(frame, chunks[0], app_state);

        // Render log entries
        self.render_log_entries(frame, chunks[1], app_state);
    }

    fn render_header(&self, frame: &mut Frame, area: Rect, app_state: &AppState) {
        let log_state = &app_state.protocol_log_state;

        let filter_text = format!("Filter: {}", log_state.filter.display_name());
        let pause_text = if log_state.is_paused { "[P]aused" } else { "[P]ause" };
        let count_text = format!("{} logs", log_state.total_count());

        let header_style = if self.is_focused {
            Style::default().fg(Color::LightGreen)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let header = Line::from(vec![
            Span::styled("Protocol Logs", header_style.add_modifier(Modifier::BOLD)),
            Span::raw(" │ "),
            Span::styled(filter_text, Style::default().fg(Color::Cyan)),
            Span::raw(" │ "),
            Span::styled(pause_text, if log_state.is_paused {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default().fg(Color::DarkGray)
            }),
            Span::raw(" │ [C]lear │ "),
            Span::styled(count_text, Style::default().fg(Color::DarkGray)),
        ]);

        frame.render_widget(Paragraph::new(header), area);
    }

    fn render_log_entries(&self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let log_state = &app_state.protocol_log_state;
        let filtered: Vec<&ProtocolLogEntry> = log_state.filtered_entries();

        // Calculate scroll position
        let visible_height = area.height.saturating_sub(2) as usize; // Account for borders
        let total_entries = filtered.len();
        let scroll_offset = if log_state.scroll_offset == usize::MAX {
            // Auto-scroll to bottom
            total_entries.saturating_sub(visible_height)
        } else {
            log_state.scroll_offset.min(total_entries.saturating_sub(visible_height))
        };

        // Get visible entries
        let visible_entries: Vec<ListItem> = filtered
            .iter()
            .skip(scroll_offset)
            .take(visible_height)
            .map(|entry| self.render_log_entry(entry))
            .collect();

        let border_style = if self.is_focused {
            Style::default().fg(Color::LightGreen)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let list = List::new(visible_entries)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(border_style),
            );

        frame.render_widget(list, area);
    }

    fn render_log_entry(&self, entry: &ProtocolLogEntry) -> ListItem<'static> {
        // Format timestamp
        let time_str = if let Ok(duration) = entry.timestamp.elapsed() {
            let secs = duration.as_secs();
            if secs < 60 {
                format!("{}s ago", secs)
            } else if secs < 3600 {
                format!("{}m ago", secs / 60)
            } else {
                format!("{}h ago", secs / 3600)
            }
        } else {
            "now".to_string()
        };

        // Direction arrow
        let (direction_str, direction_style) = match entry.direction {
            LogDirection::Request => ("→", Style::default().fg(Color::Cyan)),
            LogDirection::Response => match entry.status {
                LogStatus::Success => ("←", Style::default().fg(Color::Green)),
                LogStatus::Error => ("←", Style::default().fg(Color::Red)),
                LogStatus::Pending => ("←", Style::default().fg(Color::Yellow)),
            },
        };

        // API type with color
        let api_style = match entry.api_type {
            ApiType::Fetch | ApiType::ListOffset => Style::default().fg(Color::Cyan),
            ApiType::Metadata => Style::default().fg(Color::Yellow),
            ApiType::ListGroups | ApiType::DescribeGroups | ApiType::FindCoordinator | ApiType::OffsetFetch => {
                Style::default().fg(Color::Magenta)
            }
            ApiType::DeleteTopics | ApiType::CreatePartitions => Style::default().fg(Color::Red),
            _ => Style::default().fg(Color::White),
        };

        // Duration or status
        let status_str = if let Some(duration) = entry.duration {
            let ms = duration.as_millis();
            if ms < 1000 {
                format!("{}ms", ms)
            } else {
                format!("{:.1}s", duration.as_secs_f64())
            }
        } else {
            "...".to_string()
        };

        let status_style = match entry.status {
            LogStatus::Pending => Style::default().fg(Color::Yellow),
            LogStatus::Success => Style::default().fg(Color::Green),
            LogStatus::Error => Style::default().fg(Color::Red),
        };

        // Format bytes
        let bytes_str = format_bytes(entry.bytes);

        let line = Line::from(vec![
            Span::styled(format!("{:>7}", time_str), Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled(direction_str, direction_style),
            Span::raw("  "),
            Span::styled(format!("{:<12}", entry.api_type.display_name()), api_style),
            Span::raw(" "),
            Span::styled(format!("v{}", entry.api_version), Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled(format!("{:>6}", status_str), status_style),
            Span::raw("  "),
            Span::styled(format!("{:>8}", bytes_str), Style::default().fg(Color::DarkGray)),
            Span::raw("  "),
            Span::styled(entry.summary.clone(), Style::default().fg(Color::White)),
        ]);

        ListItem::new(line)
    }

    /// Handle keyboard input when focused
    pub fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> PanelResult {
        let visible_height = self.visible_height;

        match key {
            KeyCode::Char('j') | KeyCode::Down => {
                app_state.protocol_log_state.scroll_down(visible_height);
                PanelResult::Handled
            }
            KeyCode::Char('k') | KeyCode::Up => {
                app_state.protocol_log_state.scroll_up();
                PanelResult::Handled
            }
            KeyCode::Char('g') => {
                app_state.protocol_log_state.scroll_to_top();
                PanelResult::Handled
            }
            KeyCode::Char('G') => {
                app_state.protocol_log_state.scroll_to_bottom();
                PanelResult::Handled
            }
            KeyCode::Char('p') => {
                app_state.protocol_log_state.toggle_pause();
                PanelResult::Handled
            }
            KeyCode::Char('c') => {
                app_state.protocol_log_state.clear();
                PanelResult::Handled
            }
            KeyCode::Char('f') => {
                app_state.protocol_log_state.cycle_filter();
                PanelResult::Handled
            }
            KeyCode::Esc | KeyCode::Char('l') => {
                self.is_focused = false;
                PanelResult::Handled
            }
            _ => PanelResult::Handled,
        }
    }
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}
