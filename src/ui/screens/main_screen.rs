use crate::models::broker_stats::ConnectionStatus;
use crate::state::app_state::{AppMessage, AppState, InputMode, TopicListState};
use crate::ui::screen::{AppAction, EventResult, Screen};
use crate::ui::screen_type::ScreenType;
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::{Color, Line, Modifier, Span, Style};
use ratatui::widgets::{
    Block, Borders, List, ListItem, Paragraph, Scrollbar, ScrollbarOrientation,
};
use ratatui::Frame;

pub struct MainScreen {}

impl MainScreen {
    pub fn new() -> Self {
        Self {}
    }

    fn render_header(&self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let stats = &app_state.broker_stats;
        let breadcrumb = &app_state.breadcrumb;
        let header = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(25),
                Constraint::Min(20),
                Constraint::Length(40),
            ])
            .split(area);

        // Broker
        let broker = Paragraph::new(format!("Broker: {}", stats.broker));
        frame.render_widget(broker, header[0]);

        // Breadcrumb
        let breadcrumb_text = breadcrumb.join(" > ");
        let breadcrumb_widget = Paragraph::new(breadcrumb_text);
        frame.render_widget(breadcrumb_widget, header[1]);

        // Stats
        let status_icon = match stats.status {
            ConnectionStatus::Connected => "✓",
            ConnectionStatus::Connecting => "⋯",
            ConnectionStatus::Disconnected => "✗",
            ConnectionStatus::Failed(_) => "⚠",
        };

        let stats_text = format!(
            "{} Connected | ↓ {} | ↑ {}",
            status_icon,
            self.format_bytes(stats.bytes_received as u64),
            self.format_bytes(stats.bytes_sent as u64)
        );
        let stats_widget = Paragraph::new(stats_text);
        frame.render_widget(stats_widget, header[2]);
    }

    fn format_bytes(&self, bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = 1024 * KB;
        const GB: u64 = 1024 * MB;

        if bytes >= GB {
            format!("{:.2}GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2}MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2}KB", bytes as f64 / KB as f64)
        } else {
            format!("{}B", bytes)
        }
    }

    fn render_body(&self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(25), // Topics sidebar
                Constraint::Percentage(75), // Messages + filter
            ])
            .split(area);

        self.render_topics_list(
            frame,
            body[0],
            &mut app_state.filtered_topic_state,
            &app_state.topic_search_query,
            &app_state.input_mode,
        );
        self.render_messages_panel(frame, body[1], app_state);
    }

    fn render_topics_list(
        &self,
        frame: &mut Frame,
        area: Rect,
        topic_state: &mut TopicListState,
        search_query: &str,
        input_mode: &InputMode,
    ) {
        // Split the area into search box (3 lines) and topic list
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Search box
                Constraint::Min(0),    // Topic list
            ])
            .split(area);

        // Render search box
        let search_text = if search_query.is_empty() {
            "Type / to search topics...".to_string()
        } else {
            search_query.to_string()
        };

        let search_style = match input_mode {
            InputMode::SearchTopic => Style::default().fg(Color::Yellow),
            InputMode::Normal => Style::default().fg(Color::Gray),
            _ => Style::default().fg(Color::Gray),
        };

        let search_box = Paragraph::new(search_text)
            .block(
                Block::default()
                    .title("Search")
                    .borders(Borders::ALL)
                    .border_style(search_style),
            )
            .style(Style::default().fg(Color::White));

        frame.render_widget(search_box, chunks[0]);

        // Filter topics based on search query
        let filtered_items: Vec<ListItem> = topic_state
            .topics
            .items
            .iter()
            .enumerate()
            .map(|(_, topic)| {
                let line = Line::from(vec![
                    Span::raw(&topic.name),
                    Span::styled(
                        format!(" [{}p]", topic.partitions),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]);
                ListItem::new(line)
            })
            .collect();

        let list = List::new(filtered_items)
            .block(
                Block::default()
                    .title("Topics")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(Color::Gray)),
            )
            .highlight_style(
                Style::default()
                    .bg(Color::Blue)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("► ")
            .scroll_padding(5);

        topic_state.vertical_scroll_state = topic_state
            .vertical_scroll_state
            .content_length(topic_state.topics.items.len());

        frame.render_stateful_widget(list, chunks[1], &mut topic_state.topics.state);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(Some("↑"))
                .end_symbol(Some("↓")),
            chunks[1],
            &mut topic_state.vertical_scroll_state,
        );
    }

    fn render_messages_panel(&self, frame: &mut Frame, area: Rect, app_state: &AppState) {
        let items: Vec<ListItem> = app_state
            .records
            .items
            .iter()
            .map(|r| ListItem::new(format!("{}", String::from_utf8(r.value.clone()).unwrap())))
            .collect();

        let list = List::new(items).block(
            Block::default()
                .title("Records")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Gray)),
        );
        frame.render_widget(list, area);
    }

    fn render_footer(&self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {}
}

impl Screen for MainScreen {
    fn render(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // Header
                Constraint::Min(0),    // Body
                Constraint::Length(1), // Footer
            ])
            .split(frame.area());

        self.render_header(frame, chunks[0], app_state);
        self.render_body(frame, chunks[1], app_state);
        self.render_footer(frame, chunks[2], app_state);
    }

    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        match app_state.input_mode {
            InputMode::Normal => match key {
                KeyCode::Char('q') => EventResult::Quit,
                KeyCode::Char('/') => EventResult::Action(AppAction::ToggleSearchMode),
                KeyCode::Char('i') => {
                    if let Some(topic) = app_state.filtered_topic_state.topics.selected() {
                        EventResult::Push(ScreenType::TopicDetail(topic.name.clone()))
                    } else {
                        EventResult::Handled
                    }
                }
                KeyCode::Up => {
                    app_state.filtered_topic_state.vertical_scroll = app_state
                        .filtered_topic_state
                        .vertical_scroll
                        .saturating_sub(1);
                    app_state.filtered_topic_state.vertical_scroll_state = app_state
                        .filtered_topic_state
                        .vertical_scroll_state
                        .position(app_state.filtered_topic_state.vertical_scroll);
                    app_state.filtered_topic_state.topics.previous();
                    EventResult::Handled
                }
                KeyCode::Down => {
                    app_state.filtered_topic_state.vertical_scroll = app_state
                        .filtered_topic_state
                        .vertical_scroll
                        .saturating_add(1);
                    app_state.filtered_topic_state.vertical_scroll_state = app_state
                        .filtered_topic_state
                        .vertical_scroll_state
                        .position(app_state.filtered_topic_state.vertical_scroll);
                    app_state.filtered_topic_state.topics.next();
                    EventResult::Handled
                }
                KeyCode::Enter => {
                    if let Some(topic) = app_state.filtered_topic_state.topics.selected() {
                        app_state
                            .tx
                            .send(AppMessage::TopicSelected(topic.name.clone()))
                            .unwrap();
                        EventResult::Action(AppAction::LoadMessages(topic.name.clone()))
                    } else {
                        EventResult::Handled
                    }
                }
                _ => EventResult::Handled,
            },
            InputMode::SearchTopic => match key {
                KeyCode::Char(c) => EventResult::Action(AppAction::SearchTopic(c)),
                KeyCode::Backspace => EventResult::Action(AppAction::SearchTopicBackspace), 
                KeyCode::Esc => EventResult::Action(AppAction::ToggleSearchMode),
                KeyCode::Enter => EventResult::Action(AppAction::ToggleSearchMode),
                _ => EventResult::Handled,
            },
        }
    }
}
