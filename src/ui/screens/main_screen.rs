use crate::models::broker_stats::ConnectionStatus;
use crate::models::topic::Topic;
use crate::state::app_state::{AppMessage, AppState, ConsumerGroupListState, TopicListState};
use crate::ui::panels::{FocusTarget, GroupDetailPanel, LogsPanel, MainPanelType, PanelResult, RecordsPanel};
use crate::ui::screen::{AppAction, EventResult, Screen};
use crate::ui::screen_type::ScreenType;
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::{Color, Line, Modifier, Span, Style};
use ratatui::widgets::{
    Block, BorderType, Borders, List, ListItem, Paragraph, Scrollbar, ScrollbarOrientation,
};
use ratatui::Frame;
use std::time::Duration;

pub enum InputMode {
    SelectTopic,
    SelectGroup,
    SearchTopic,
    PanelFocused,
    LogsFocused,
}

pub struct MainScreen {
    input_mode: InputMode,
    current_panel: MainPanelType,
    records_panel: RecordsPanel,
    group_detail_panel: Option<GroupDetailPanel>,
    logs_panel: LogsPanel,
}

impl MainScreen {
    pub fn new() -> Self {
        Self {
            input_mode: InputMode::SelectTopic,
            current_panel: MainPanelType::default(),
            records_panel: RecordsPanel::new(),
            group_detail_panel: None,
            logs_panel: LogsPanel::new(),
        }
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
                Constraint::Percentage(75), // Main panel
            ])
            .split(area);

        let left_pillars = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Percentage(50),
                Constraint::Percentage(50),
                Constraint::Length(3),
            ])
            .split(body[0]);

        let broker_box = Paragraph::new(format!("{}", app_state.broker_stats.broker))
            .block(
                Block::default()
                    .title("Broker")
                    .borders(Borders::ALL)
                    .border_type(BorderType::Plain),
            )
            .style(Style::default().fg(Color::White));

        frame.render_widget(broker_box, left_pillars[0]);

        self.render_topics_list(
            frame,
            left_pillars[1],
            &mut app_state.filtered_topic_state,
            &app_state.topic_search_query,
        );

        self.render_groups_list(
            frame,
            left_pillars[2],
            &mut app_state.filtered_consumer_groups_state,
        );

        self.render_stats(frame, left_pillars[3], app_state);

        self.render_main_panel(frame, body[1], app_state);
    }

    fn render_main_panel(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let chunks = Layout::default().direction(Direction::Vertical).constraints(
            [
                Constraint::Min(0),
                Constraint::Percentage(20)
            ]
        ).split(area);

        self.logs_panel.is_focused = matches!(self.input_mode, InputMode::LogsFocused);
        self.logs_panel.render(frame, chunks[1], app_state);

        let is_focused = matches!(self.input_mode, InputMode::PanelFocused);

        match &self.current_panel {
            MainPanelType::Records => {
                self.records_panel.render(frame, chunks[0], app_state, is_focused);
            }
            MainPanelType::GroupDetail(_) => {
                if let Some(panel) = &mut self.group_detail_panel {
                    panel.render(frame, chunks[0], app_state, is_focused);
                }
            }
        }
    }

    fn render_stats(&self, frame: &mut Frame, area: Rect, app_state: &AppState) {
        let stats = &app_state.broker_stats;

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
        let stats_widget =
            Paragraph::new(stats_text).block(Block::default().borders(Borders::ALL).title("Stats"));
        frame.render_widget(stats_widget, area);
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
                Constraint::Length(3),
                Constraint::Min(0), // Topic list
            ])
            .split(area);

        // Render search box
        let search_text = if search_query.is_empty() {
            "Type / to search topics...".to_string()
        } else {
            search_query.to_string()
        };

        let search_style = match self.input_mode {
            InputMode::SearchTopic => Style::default().fg(Color::LightGreen),
            _ => Style::default().fg(Color::White),
        };

        let search_box = Paragraph::new(search_text)
            .block(
                Block::default()
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
            InputMode::SelectTopic => Style::default().fg(Color::LightGreen),
            _ => Style::default().fg(Color::White),
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

    fn render_groups_list(
        &self,
        frame: &mut Frame,
        area: Rect,
        state: &mut ConsumerGroupListState,
    ) {
        let list_style = match self.input_mode {
            InputMode::SelectGroup => Style::default().fg(Color::LightGreen),
            _ => Style::default().fg(Color::White),
        };

        // Filter topics based on search query
        let filtered_items: Vec<ListItem> = state
            .groups
            .items
            .iter()
            .map(|group| {
                let line = Line::from(vec![Span::raw(&group.group_id)]);
                ListItem::new(line)
            })
            .collect();

        let list = List::new(filtered_items)
            .block(
                Block::default()
                    .title("Groups")
                    .borders(Borders::ALL)
                    .border_style(list_style),
            )
            .highlight_style(
                Style::default()
                    .bg(Color::Blue)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )
            .scroll_padding(5);

        state.vertical_scroll_state = state
            .vertical_scroll_state
            .content_length(state.items.len());

        frame.render_stateful_widget(list, area, &mut state.groups.state);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            area,
            &mut state.vertical_scroll_state,
        );
    }

    fn render_footer(&self, frame: &mut Frame, area: Rect, _app_state: &mut AppState) {
        let hint_text = match self.input_mode {
            InputMode::SelectTopic => {
                "q: Quit | <tab>: Switch | /: Search | l: Logs | i: Info | p: Partitions | d: Delete | <enter>: View"
            }
            InputMode::SelectGroup => {
                "q: Quit | <tab>: Switch | l: Logs | r: Refresh | <enter>: View details"
            }
            InputMode::SearchTopic => "<esc>: Cancel | <enter>: Confirm search",
            InputMode::PanelFocused => match &self.current_panel {
                MainPanelType::Records => {
                    "<esc>/<tab>: Back | q: Quit | l: Logs | j/k: Navigate | i: View detail"
                }
                MainPanelType::GroupDetail(_) => {
                    "<esc>: Back | <tab>: Switch tab | l: Logs | r: Refresh | j/k: Navigate"
                }
            },
            InputMode::LogsFocused => {
                "<esc>/l: Exit logs | j/k: Scroll | g/G: Top/Bottom | p: Pause | c: Clear | f: Filter"
            }
        };

        let p = Paragraph::new(hint_text)
            .block(Block::default())
            .style(Style::default().fg(Color::LightBlue));

        frame.render_widget(p, area);
    }

    fn load_topics(&self, app_state: &mut AppState) {
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

    fn start_stats_polling(&self, app_state: &mut AppState) {
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

    fn switch_view_mode(&mut self, input_mode: InputMode, app_state: &mut AppState) {
        // Unselect previous list and select new list based on mode transition
        match (&self.input_mode, &input_mode) {
            (InputMode::SelectTopic, InputMode::SelectGroup) => {
                // Switching from Topics to Groups
                app_state.filtered_topic_state.topics.unselect();
                app_state.filtered_consumer_groups_state.groups.select_first();
            }
            (InputMode::SelectGroup, InputMode::SelectTopic) => {
                // Switching from Groups to Topics
                app_state.filtered_consumer_groups_state.groups.unselect();
                app_state.filtered_topic_state.topics.select_first();
            }
            (InputMode::PanelFocused, InputMode::SelectTopic) => {
                // Returning to Topics from panel
                app_state.filtered_consumer_groups_state.groups.unselect();
                app_state.filtered_topic_state.topics.select_first();
            }
            (InputMode::PanelFocused, InputMode::SelectGroup) => {
                // Returning to Groups from panel
                app_state.filtered_topic_state.topics.unselect();
                app_state.filtered_consumer_groups_state.groups.select_first();
            }
            _ => {}
        }
        self.input_mode = input_mode;
    }

    fn switch_panel(&mut self, panel_type: MainPanelType) {
        // Clean up previous panel if needed
        if !matches!(panel_type, MainPanelType::GroupDetail(_)) {
            self.group_detail_panel = None;
        }

        // Set up new panel
        match &panel_type {
            MainPanelType::Records => {
                // Records panel is always available
            }
            MainPanelType::GroupDetail(group_id) => {
                self.group_detail_panel = Some(GroupDetailPanel::new(group_id.clone()));
            }
        }

        self.current_panel = panel_type;
        self.input_mode = InputMode::PanelFocused;
    }

    fn update_filters(&mut self, app_state: &mut AppState) {
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

    fn on_search_char(&mut self, c: char, app_state: &mut AppState) {
        app_state.topic_search_query.push(c);
        self.update_filters(app_state);
    }

    fn on_search_backspace(&mut self, app_state: &mut AppState) {
        app_state.topic_search_query.pop();
        self.update_filters(app_state);
    }

    fn on_reset_filters(&mut self, app_state: &mut AppState) {
        app_state.topic_search_query.clear();
        self.update_filters(app_state);
    }

    fn load_messages_for_topic(&self, topic: Topic, app_state: &mut AppState) {
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

    fn load_consumer_groups(&self, app_state: &mut AppState) {
        let tx = app_state.tx.clone();
        let repo = app_state.repository_manager.consumer_groups().clone();

        tokio::spawn(async move {
            match repo.fetch_all().await {
                Ok(groups) => {
                    let _ = tx.send(AppMessage::ConsumerGroupsLoaded(groups));
                }
                Err(e) => {
                    let _ = tx.send(AppMessage::ConsumerGroupError(e.to_string()));
                }
            }
        });
    }

    fn handle_panel_result(&mut self, result: PanelResult, app_state: &mut AppState) -> EventResult {
        match result {
            PanelResult::Handled => EventResult::Handled,
            PanelResult::SwitchFocus(target) => {
                match target {
                    FocusTarget::Topics => {
                        self.switch_view_mode(InputMode::SelectTopic, app_state);
                        // Switch back to Records panel when returning to topics
                        self.current_panel = MainPanelType::Records;
                        self.group_detail_panel = None;
                    }
                    FocusTarget::Groups => {
                        self.switch_view_mode(InputMode::SelectGroup, app_state);
                    }
                }
                EventResult::Handled
            }
            PanelResult::ShowModal(screen_type) => EventResult::Push(screen_type),
        }
    }
}

impl Screen for MainScreen {
    fn render(&mut self, frame: &mut Frame, _area: Rect, app_state: &mut AppState) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Min(0),        // Body + Logs
                Constraint::Length(1),     // Footer
            ])
            .split(frame.area());

        self.render_body(frame, chunks[0], app_state);

        self.render_footer(frame, chunks[1], app_state);
    }

    fn on_mount(&mut self, app_state: &mut AppState) {
        self.load_topics(app_state);
        self.start_stats_polling(app_state);
        self.load_consumer_groups(app_state);
    }

    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        match self.input_mode {
            InputMode::SelectTopic => match key {
                KeyCode::Char('q') => EventResult::Action(AppAction::ConfirmExitApp),
                KeyCode::Char('l') => {
                    self.input_mode = InputMode::LogsFocused;
                    EventResult::Handled
                }
                KeyCode::Char('/') => {
                    self.switch_view_mode(InputMode::SearchTopic, app_state);
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
                KeyCode::Char('p') => {
                    if let Some(topic) = app_state.filtered_topic_state.topics.selected() {
                        EventResult::Push(ScreenType::CreatePartitionsDialog {
                            topic_name: topic.name.clone(),
                            current_partitions: topic.partitions.len() as i32,
                        })
                    } else {
                        EventResult::Handled
                    }
                }
                KeyCode::Char('g') => EventResult::Replace(ScreenType::ConsumerGroups),
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
                    self.switch_panel(MainPanelType::Records);
                    EventResult::Handled
                }
                KeyCode::Tab => {
                    self.switch_view_mode(InputMode::SelectGroup, app_state);
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
                    self.switch_view_mode(InputMode::SelectTopic, app_state);
                    EventResult::Handled
                }
                KeyCode::Enter => {
                    self.switch_view_mode(InputMode::SelectTopic, app_state);
                    EventResult::Handled
                }
                _ => EventResult::Handled,
            },
            InputMode::PanelFocused => {
                // First check for quit command
                if matches!(key, KeyCode::Char('q')) {
                    return EventResult::Action(AppAction::ConfirmExitApp);
                }

                // Check for logs panel focus
                if matches!(key, KeyCode::Char('l')) {
                    self.input_mode = InputMode::LogsFocused;
                    return EventResult::Handled;
                }

                // Dispatch to current panel
                let result = match &self.current_panel {
                    MainPanelType::Records => self.records_panel.handle_key(key, app_state),
                    MainPanelType::GroupDetail(_) => {
                        if let Some(panel) = &mut self.group_detail_panel {
                            panel.handle_key(key, app_state)
                        } else {
                            PanelResult::Handled
                        }
                    }
                };

                self.handle_panel_result(result, app_state)
            }
            InputMode::SelectGroup => match key {
                KeyCode::Char('q') => EventResult::Action(AppAction::ConfirmExitApp),
                KeyCode::Char('l') => {
                    self.input_mode = InputMode::LogsFocused;
                    EventResult::Handled
                }
                KeyCode::Char('r') => {
                    self.load_consumer_groups(app_state);
                    EventResult::Handled
                }
                KeyCode::Tab => {
                    self.switch_view_mode(InputMode::SelectTopic, app_state);
                    EventResult::Handled
                }
                KeyCode::Up => {
                    app_state.filtered_consumer_groups_state.groups.previous();
                    EventResult::Handled
                }
                KeyCode::Down => {
                    app_state.filtered_consumer_groups_state.groups.next();
                    EventResult::Handled
                }
                KeyCode::Enter => {
                    if let Some(group) =
                        app_state.filtered_consumer_groups_state.groups.selected()
                    {
                        let group_id = group.group_id.clone();
                        self.switch_panel(MainPanelType::GroupDetail(group_id));
                    }
                    EventResult::Handled
                }
                _ => EventResult::Handled,
            },
            InputMode::LogsFocused => {
                // Check for quit command
                if matches!(key, KeyCode::Char('q')) {
                    return EventResult::Action(AppAction::ConfirmExitApp);
                }

                // Dispatch to logs panel
                let result = self.logs_panel.handle_key(key, app_state);

                // If logs panel returned that it's no longer focused, switch back
                if !self.logs_panel.is_focused {
                    self.input_mode = InputMode::SelectTopic;
                }

                match result {
                    PanelResult::Handled => EventResult::Handled,
                    PanelResult::SwitchFocus(target) => {
                        match target {
                            FocusTarget::Topics => {
                                self.switch_view_mode(InputMode::SelectTopic, app_state);
                            }
                            FocusTarget::Groups => {
                                self.switch_view_mode(InputMode::SelectGroup, app_state);
                            }
                        }
                        EventResult::Handled
                    }
                    PanelResult::ShowModal(screen_type) => EventResult::Push(screen_type),
                }
            }
        }
    }
}
