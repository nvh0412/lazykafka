use crate::models::broker_stats::ConnectionStatus;
use crate::models::topic::Topic;
use crate::state::app_state::{AppMessage, AppState, TopicListState};
use crate::ui::screen::{AppAction, EventResult, Screen};
use crate::ui::screen_type::ScreenType;
use ratatui::Frame;
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::{Color, Line, Modifier, Span, Style};
use ratatui::text::Text;
use ratatui::widgets::{
    Block, Borders, HighlightSpacing, List, ListItem, Paragraph, Row, Scrollbar,
    ScrollbarOrientation, Table, TableState,
};
use std::time::Duration;

pub enum InputMode {
    SelectTopic,
    SearchTopic,
    ViewRecords,
}

pub struct MainScreen {
    input_mode: InputMode,
    records_table_state: TableState,
}

impl MainScreen {
    pub fn new() -> Self {
        Self {
            input_mode: InputMode::SelectTopic,
            records_table_state: TableState::default().with_selected(0),
        }
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

    fn render_body(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
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
        );
        self.render_messages_panel(frame, body[1], app_state);
    }

    fn render_topics_list(
        &self,
        frame: &mut Frame,
        area: Rect,
        topic_state: &mut TopicListState,
        search_query: &str,
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

        let search_style = match self.input_mode {
            InputMode::SearchTopic => Style::default().fg(Color::White),
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
            .map(|topic| {
                let line = Line::from(vec![
                    Span::raw(&topic.name),
                    Span::styled(
                        format!(" [{}p]", topic.partitions.len()),
                        Style::default().fg(Color::DarkGray),
                    ),
                ]);
                ListItem::new(line)
            })
            .collect();

        let list_style = match self.input_mode {
            InputMode::SelectTopic => Style::default().fg(Color::White),
            _ => Style::default().fg(Color::Gray),
        };

        let list = List::new(filtered_items)
            .block(
                Block::default()
                    .title("Topics")
                    .borders(Borders::ALL)
                    .border_style(list_style),
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
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            chunks[1],
            &mut topic_state.vertical_scroll_state,
        );
    }

    fn next_record_row(&mut self) {
        let i = self.records_table_state.selected().unwrap_or(0);
        self.records_table_state.select(Some(i.saturating_add(1)));
    }

    fn previous_record_row(&mut self) {
        let i = self.records_table_state.selected().unwrap_or(0);
        self.records_table_state.select(Some(i.saturating_sub(1)));
    }

    fn render_messages_panel(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let rows = app_state.records.items.iter().map(|data| {
            let item = data.ref_array();
            item.into_iter()
                .map(|content| ratatui::widgets::Cell::from(format!("\n{content}\n")))
                .collect::<Row>()
                .height(4)
        });

        let list_style = match self.input_mode {
            InputMode::ViewRecords => Style::default().fg(Color::White),
            _ => Style::default().fg(Color::Gray),
        };

        let header_style = Style::default().bg(Color::Cyan);

        let header = ["Key", "Partition", "Timestamp", "Offset", "Value"]
            .into_iter()
            .map(ratatui::widgets::Cell::from)
            .collect::<Row>()
            .style(header_style)
            .height(1);

        let selected_row_style = Style::default()
            .add_modifier(Modifier::REVERSED)
            .fg(Color::Black)
            .bg(Color::White);

        let bar = " █ ";

        let t = Table::new::<_, [Constraint; 0]>(rows, [])
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Records")
                    .border_style(list_style),
            )
            .header(header)
            .row_highlight_style(selected_row_style)
            .highlight_symbol(Text::from(vec![
                "".into(),
                bar.into(),
                bar.into(),
                "".into(),
            ]))
            .highlight_spacing(HighlightSpacing::Always);

        frame.render_stateful_widget(t, area, &mut self.records_table_state);
    }

    fn render_footer(&self, _frame: &mut Frame, _area: Rect, _app_state: &mut AppState) {}

    pub fn load_topics(&self, app_state: &mut AppState) {
        let tx = app_state.tx.clone();
        let repo = app_state.repository_manager.topics().clone();

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

    pub fn start_stats_polling(&self, app_state: &mut AppState) {
        let tx = app_state.tx.clone();
        let client = app_state.repository_manager.kafka_client.clone();

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

    pub fn switch_view_mode(&mut self, input_mode: InputMode) {
        self.input_mode = input_mode;
    }

    pub fn update_filters(&mut self, app_state: &mut AppState) {
        app_state.filtered_topic_state.topics.update_items(
            app_state
                .topics
                .items
                .iter()
                .filter(|topic| topic.name.contains(&app_state.topic_search_query))
                .cloned()
                .collect(),
        );
    }

    pub fn on_search_char(&mut self, c: char, app_state: &mut AppState) {
        app_state.topic_search_query.push(c);
        self.update_filters(app_state);
    }

    pub fn on_search_backspace(&mut self, app_state: &mut AppState) {
        app_state.topic_search_query.pop();
        self.update_filters(app_state);
    }

    pub fn on_reset_filters(&mut self, app_state: &mut AppState) {
        app_state.topic_search_query.clear();
        self.update_filters(app_state);
    }

    pub fn load_messages_for_topic(&self, topic: Topic, app_state: &mut AppState) {
        let tx = app_state.tx.clone();
        let repo = app_state.repository_manager.messages().clone();

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
}

impl Screen for MainScreen {
    fn render(&mut self, frame: &mut Frame, _area: Rect, app_state: &mut AppState) {
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

    fn on_mount(&mut self, app_state: &mut AppState) {
        self.load_topics(app_state);
        self.start_stats_polling(app_state);
    }

    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        match self.input_mode {
            InputMode::SelectTopic => match key {
                KeyCode::Char('q') => EventResult::Action(AppAction::ConfirmExitApp),
                KeyCode::Char('/') => {
                    self.switch_view_mode(InputMode::SearchTopic);
                    EventResult::Handled
                }
                KeyCode::Char('i') => {
                    if let Some(topic) = app_state.filtered_topic_state.topics.selected() {
                        EventResult::Push(ScreenType::TopicDetail(topic.name.clone()))
                    } else {
                        EventResult::Handled
                    }
                }
                KeyCode::Char('d') => {
                    if let Some(topic) = app_state.filtered_topic_state.topics.selected() {
                        EventResult::Push(ScreenType::ConfirmDeleteTopicDialog {
                            topic_name: topic.name.clone(),
                        })
                    } else {
                        EventResult::Handled
                    }
                }
                KeyCode::Char('g') => {
                    EventResult::Replace(ScreenType::ConsumerGroups)
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
                        self.load_messages_for_topic(topic.clone(), app_state);
                    }
                    EventResult::Handled
                }
                KeyCode::Tab => {
                    self.switch_view_mode(InputMode::ViewRecords);
                    EventResult::Handled
                }
                _ => EventResult::Handled,
            },
            InputMode::SearchTopic => match key {
                KeyCode::Char(c) => {
                    self.on_search_char(c, app_state);
                    EventResult::Handled
                }
                KeyCode::Backspace => {
                    self.on_search_backspace(app_state);
                    EventResult::Handled
                }
                KeyCode::Esc => {
                    self.on_reset_filters(app_state);
                    self.switch_view_mode(InputMode::SelectTopic);
                    EventResult::Handled
                }
                KeyCode::Enter => {
                    self.switch_view_mode(InputMode::SelectTopic);
                    EventResult::Handled
                }
                _ => EventResult::Handled,
            },
            InputMode::ViewRecords => match key {
                KeyCode::Esc | KeyCode::Tab => {
                    self.switch_view_mode(InputMode::SelectTopic);
                    EventResult::Handled
                }
                KeyCode::Up => {
                    self.previous_record_row();
                    EventResult::Handled
                }
                KeyCode::Down => {
                    self.next_record_row();
                    EventResult::Handled
                }
                KeyCode::Char('i') => {
                    if let Some(record_idx) = self.records_table_state.selected() {
                        let record = app_state.records.items[record_idx].clone();
                        EventResult::Push(ScreenType::MessageDetail(record))
                    } else {
                        EventResult::Handled
                    }
                }
                _ => EventResult::Handled,
            },
        }
    }
}
