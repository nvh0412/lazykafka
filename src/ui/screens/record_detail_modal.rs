use crate::protocol::record::Record;
use crate::state::app_state::AppState;
use crate::ui::screen::AppAction::ClosePopup;
use crate::ui::screen::{EventResult, Screen};
use ratatui::Frame;
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
};

pub struct RecordDetailModal {
    record: Record,
    scroll_offset: u16,
    formatted_lines: Vec<Line<'static>>,
    is_json: bool,
    visible_height: u16,
}

impl RecordDetailModal {
    pub fn new(record: Record) -> Self {
        let (is_json, formatted_lines) = Self::format_value(&record.value);
        Self {
            record,
            scroll_offset: 0,
            formatted_lines,
            is_json,
            visible_height: 10,
        }
    }

    fn format_value(value: &[u8]) -> (bool, Vec<Line<'static>>) {
        // Try to parse as UTF-8
        let text = match String::from_utf8(value.to_vec()) {
            Ok(s) => s,
            Err(_) => return (false, Self::format_hex(value)),
        };

        // Try to parse as JSON
        match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(json) => {
                let formatted = serde_json::to_string_pretty(&json).unwrap_or(text.clone());
                (true, Self::highlight_json(&formatted))
            }
            Err(_) => (false, Self::format_plain_text(&text)),
        }
    }

    fn format_hex(value: &[u8]) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        for chunk in value.chunks(16) {
            let hex: Vec<String> = chunk.iter().map(|b| format!("{:02x}", b)).collect();
            let ascii: String = chunk
                .iter()
                .map(|&b| {
                    if (32..127).contains(&b) {
                        b as char
                    } else {
                        '.'
                    }
                })
                .collect();
            lines.push(Line::from(vec![
                Span::styled(hex.join(" "), Style::default().fg(Color::Yellow)),
                Span::raw("  "),
                Span::styled(ascii, Style::default().fg(Color::DarkGray)),
            ]));
        }
        if lines.is_empty() {
            lines.push(Line::from(Span::styled(
                "<empty>",
                Style::default().fg(Color::DarkGray),
            )));
        }
        lines
    }

    fn format_plain_text(text: &str) -> Vec<Line<'static>> {
        text.lines()
            .map(|line| Line::from(Span::raw(line.to_string())))
            .collect()
    }

    fn highlight_json(json_str: &str) -> Vec<Line<'static>> {
        json_str.lines().map(Self::highlight_json_line).collect()
    }

    fn highlight_json_line(line: &str) -> Line<'static> {
        let mut spans: Vec<Span<'static>> = Vec::new();
        let chars: Vec<char> = line.chars().collect();
        let mut i = 0;

        while i < chars.len() {
            let c = chars[i];

            match c {
                // Whitespace - preserve indentation
                ' ' | '\t' => {
                    let mut ws = String::new();
                    while i < chars.len() && (chars[i] == ' ' || chars[i] == '\t') {
                        ws.push(chars[i]);
                        i += 1;
                    }
                    spans.push(Span::raw(ws));
                }
                // String (could be key or value)
                '"' => {
                    let start = i;
                    i += 1;
                    while i < chars.len() && chars[i] != '"' {
                        if chars[i] == '\\' && i + 1 < chars.len() {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    if i < chars.len() {
                        i += 1; // Include closing quote
                    }
                    let string_content: String = chars[start..i].iter().collect();

                    // Check if this is a key (followed by colon)
                    let mut j = i;
                    while j < chars.len() && (chars[j] == ' ' || chars[j] == '\t') {
                        j += 1;
                    }
                    let is_key = j < chars.len() && chars[j] == ':';

                    if is_key {
                        spans.push(Span::styled(
                            string_content,
                            Style::default()
                                .fg(Color::Cyan)
                                .add_modifier(Modifier::BOLD),
                        ));
                    } else {
                        spans.push(Span::styled(
                            string_content,
                            Style::default().fg(Color::Green),
                        ));
                    }
                }
                // Punctuation
                ':' | ',' | '{' | '}' | '[' | ']' => {
                    spans.push(Span::styled(
                        c.to_string(),
                        Style::default().fg(Color::DarkGray),
                    ));
                    i += 1;
                }
                // Numbers, booleans, null
                _ if c.is_ascii_digit() || c == '-' => {
                    let start = i;
                    while i < chars.len()
                        && (chars[i].is_ascii_digit()
                            || chars[i] == '.'
                            || chars[i] == '-'
                            || chars[i] == 'e'
                            || chars[i] == 'E'
                            || chars[i] == '+')
                    {
                        i += 1;
                    }
                    let num: String = chars[start..i].iter().collect();
                    spans.push(Span::styled(num, Style::default().fg(Color::Yellow)));
                }
                't' | 'f' => {
                    // true or false
                    let remaining: String = chars[i..].iter().collect();
                    if remaining.starts_with("true") {
                        spans.push(Span::styled(
                            "true".to_string(),
                            Style::default().fg(Color::Magenta),
                        ));
                        i += 4;
                    } else if remaining.starts_with("false") {
                        spans.push(Span::styled(
                            "false".to_string(),
                            Style::default().fg(Color::Magenta),
                        ));
                        i += 5;
                    } else {
                        spans.push(Span::raw(c.to_string()));
                        i += 1;
                    }
                }
                'n' => {
                    // null
                    let remaining: String = chars[i..].iter().collect();
                    if remaining.starts_with("null") {
                        spans.push(Span::styled(
                            "null".to_string(),
                            Style::default().fg(Color::Red),
                        ));
                        i += 4;
                    } else {
                        spans.push(Span::raw(c.to_string()));
                        i += 1;
                    }
                }
                _ => {
                    spans.push(Span::raw(c.to_string()));
                    i += 1;
                }
            }
        }

        Line::from(spans)
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

    fn render_metadata(&self, frame: &mut Frame, area: Rect) {
        let metadata_block = Block::default()
            .title("Metadata")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));

        let inner = metadata_block.inner(area);
        frame.render_widget(metadata_block, area);

        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(inner);

        let key_str = self
            .record
            .key
            .as_ref()
            .and_then(|k| String::from_utf8(k.clone()).ok())
            .unwrap_or_else(|| "<null>".to_string());

        let key_display = if key_str.len() > 25 {
            format!("{}...", &key_str[..22])
        } else {
            key_str
        };

        let left_text = vec![
            Line::from(vec![
                Span::styled(
                    "Key: ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(key_display),
            ]),
            Line::from(vec![
                Span::styled(
                    "Offset: ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(self.record.offset_delta.to_string()),
            ]),
            Line::from(vec![
                Span::styled(
                    "Attributes: ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!("{:#04x}", self.record.attributes)),
            ]),
        ];

        let right_text = vec![
            Line::from(vec![
                Span::styled(
                    "Partition: ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(
                    self.record
                        .partition
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "<unknown>".to_string()),
                ),
            ]),
            Line::from(vec![
                Span::styled(
                    "Timestamp: ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(self.record.timestamp_delta.to_string()),
            ]),
            Line::from(vec![
                Span::styled(
                    "Headers: ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(self.record.headers.len().to_string()),
            ]),
        ];

        frame.render_widget(Paragraph::new(left_text), cols[0]);
        frame.render_widget(Paragraph::new(right_text), cols[1]);
    }

    fn render_value(&mut self, frame: &mut Frame, area: Rect) {
        let title = if self.is_json {
            "Value (JSON)"
        } else {
            "Value (Raw)"
        };

        let content_len = self.formatted_lines.len();
        let scroll_info = if content_len > 0 {
            format!(
                " [{}/{}]",
                (self.scroll_offset as usize + 1).min(content_len),
                content_len
            )
        } else {
            String::new()
        };

        let value_block = Block::default()
            .title(format!("{}{}", title, scroll_info))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::LightGreen));

        let inner = value_block.inner(area);
        self.visible_height = inner.height;

        // Get visible portion of content
        let visible_lines: Vec<Line> = self
            .formatted_lines
            .iter()
            .skip(self.scroll_offset as usize)
            .take(inner.height as usize)
            .cloned()
            .collect();

        let content = Paragraph::new(visible_lines).block(value_block);

        frame.render_widget(content, area);

        // Render scrollbar if content exceeds visible area
        if content_len > inner.height as usize {
            let mut scroll_state = ScrollbarState::default()
                .content_length(content_len)
                .position(self.scroll_offset as usize);

            frame.render_stateful_widget(
                Scrollbar::default()
                    .orientation(ScrollbarOrientation::VerticalRight)
                    .begin_symbol(None)
                    .end_symbol(None),
                area.inner(ratatui::layout::Margin {
                    vertical: 1,
                    horizontal: 0,
                }),
                &mut scroll_state,
            );
        }
    }

    fn render_headers(&self, frame: &mut Frame, area: Rect) {
        let headers_block = Block::default()
            .title(format!("Headers ({})", self.record.headers.len()))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));

        if self.record.headers.is_empty() {
            let content = Paragraph::new(Line::from(Span::styled(
                "No headers",
                Style::default().fg(Color::DarkGray),
            )))
            .block(headers_block);
            frame.render_widget(content, area);
            return;
        }

        let inner = headers_block.inner(area);
        let max_headers = inner.height as usize;

        let header_lines: Vec<Line> = self
            .record
            .headers
            .iter()
            .take(max_headers)
            .map(|h| {
                let value_str = h
                    .value
                    .as_ref()
                    .and_then(|v| String::from_utf8(v.clone()).ok())
                    .unwrap_or_else(|| "<binary>".to_string());

                let value_display = if value_str.len() > 50 {
                    format!("{}...", &value_str[..47])
                } else {
                    value_str
                };

                Line::from(vec![
                    Span::styled(
                        h.key.clone(),
                        Style::default()
                            .fg(Color::Yellow)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(": "),
                    Span::styled(value_display, Style::default().fg(Color::White)),
                ])
            })
            .collect();

        let content = Paragraph::new(header_lines).block(headers_block);
        frame.render_widget(content, area);
    }
}

impl Screen for RecordDetailModal {
    fn render(&mut self, frame: &mut Frame, _area: Rect, _app_state: &mut AppState) {
        let area = self.centered_rect(80, 80, frame.area());
        frame.render_widget(Clear, area);

        // Main block with title
        let main_block = Block::default()
            .title("Record Detail (↑↓ scroll, Esc close)")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::LightGreen));

        let inner = main_block.inner(area);
        frame.render_widget(main_block, area);

        // Calculate header section height (2 lines + border)
        let headers_height = if self.record.headers.is_empty() {
            3 // Just "No headers" message
        } else {
            (self.record.headers.len().min(4) + 2) as u16 // Header lines + borders
        };

        // Split into three sections: metadata, value, headers
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5),              // Metadata
                Constraint::Min(5),                 // Value (fills remaining)
                Constraint::Length(headers_height), // Headers
            ])
            .split(inner);

        self.render_metadata(frame, chunks[0]);
        self.render_value(frame, chunks[1]);
        self.render_headers(frame, chunks[2]);
    }

    fn handle_key(&mut self, key: KeyCode, _app_state: &mut AppState) -> EventResult {
        let max_scroll = self
            .formatted_lines
            .len()
            .saturating_sub(self.visible_height as usize) as u16;

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
}
