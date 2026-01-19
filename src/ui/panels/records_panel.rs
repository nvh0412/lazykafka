use crate::state::app_state::AppState;
use crate::state::records_state::RecordsLoadingState;
use crate::ui::panels::{FocusTarget, PanelResult};
use crate::ui::screen_type::ScreenType;
use chrono::Utc;
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::{Color, Line, Modifier, Span, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};
use ratatui::Frame;

/// Input mode for the records panel
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum RecordInputMode {
    #[default]
    Normal,
    Searching,
    FilteringPartitions,
}

/// Partition colors for visual distinction
const PARTITION_COLORS: [Color; 8] = [
    Color::Cyan,
    Color::Yellow,
    Color::Magenta,
    Color::Green,
    Color::Blue,
    Color::Red,
    Color::LightCyan,
    Color::LightYellow,
];

pub struct RecordsPanel {
    table_state: TableState,
    input_mode: RecordInputMode,
    search_input: String,
}

impl RecordsPanel {
    pub fn new() -> Self {
        Self {
            table_state: TableState::default().with_selected(0),
            input_mode: RecordInputMode::Normal,
            search_input: String::new(),
        }
    }

    /// Get color for a partition index
    fn partition_color(partition: i32) -> Color {
        PARTITION_COLORS[(partition as usize) % PARTITION_COLORS.len()]
    }

    /// Format a timestamp delta as relative time
    fn format_relative_time(timestamp_delta: i64, base_timestamp: i64) -> String {
        let timestamp_ms = base_timestamp + timestamp_delta;
        if timestamp_ms <= 0 {
            return format!("{}", timestamp_delta);
        }

        let now = Utc::now().timestamp_millis();
        let diff_secs = (now - timestamp_ms) / 1000;

        if diff_secs < 0 {
            return "future".to_string();
        } else if diff_secs < 60 {
            return format!("{}s ago", diff_secs);
        } else if diff_secs < 3600 {
            return format!("{}m ago", diff_secs / 60);
        } else if diff_secs < 86400 {
            return format!("{}h ago", diff_secs / 3600);
        } else {
            return format!("{}d ago", diff_secs / 86400);
        }
    }

    /// Truncate a string to a max length with ellipsis
    fn truncate_str(s: &str, max_len: usize) -> String {
        if s.len() <= max_len {
            s.to_string()
        } else if max_len <= 3 {
            s.chars().take(max_len).collect()
        } else {
            format!("{}...", s.chars().take(max_len - 3).collect::<String>())
        }
    }

    pub fn render(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        app_state: &mut AppState,
        focused: bool,
    ) {
        // Split into toolbar (3 lines), table, and footer (1 line)
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Toolbar
                Constraint::Min(0),    // Table
                Constraint::Length(1), // Footer
            ])
            .split(area);

        self.render_toolbar(frame, chunks[0], app_state, focused);
        self.render_table(frame, chunks[1], app_state, focused);
        self.render_footer(frame, chunks[2], app_state);
    }

    fn render_toolbar(
        &self,
        frame: &mut Frame,
        area: Rect,
        app_state: &AppState,
        _focused: bool,
    ) {
        let records_state = &app_state.records_state;

        // Build toolbar content
        let topic_name = records_state
            .topic_name
            .as_deref()
            .unwrap_or("No topic");

        let sort_mode = records_state.sort_mode.display_name();
        let current_page = records_state.pagination.current_page;
        let total_pages = records_state.pagination.total_pages();
        let display_count = records_state.display_count();
        let total_estimated = records_state.pagination.total_estimated;

        // Row 1: Topic name, sort mode, pagination info
        let row1 = format!(
            " {} | Sort: {} | Page: {}/~{} | Records: {}/~{}",
            Self::truncate_str(topic_name, 30),
            sort_mode,
            current_page,
            total_pages,
            display_count,
            total_estimated
        );

        // Row 2: Search
        let search_indicator = if self.input_mode == RecordInputMode::Searching {
            format!(" Search: [{}|]", &self.search_input)
        } else if !records_state.filter.search_query.is_empty() {
            format!(" Search: [{}] (x) Clear", &records_state.filter.search_query)
        } else {
            " Search: (/) Enter search".to_string()
        };

        // Row 3: Partition filter
        let partition_info = if records_state.filter.has_partition_filter() {
            let selected: Vec<String> = records_state
                .filter
                .selected_partitions
                .iter()
                .map(|p| format!("[P{}]", p))
                .collect();
            format!(" Partitions: {} (f) Filter", selected.join(" "))
        } else {
            format!(
                " Partitions: All ({}) (f) Filter",
                records_state.filter.available_partitions.len()
            )
        };

        let toolbar_text = format!("{}\n{}\n{}", row1, search_indicator, partition_info);

        let toolbar_block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray));

        let toolbar = Paragraph::new(toolbar_text)
            .block(toolbar_block)
            .style(Style::default().fg(Color::White));

        frame.render_widget(toolbar, area);
    }

    fn render_table(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        app_state: &mut AppState,
        focused: bool,
    ) {
        let records_state = &app_state.records_state;

        // Sync table selection with records_state
        self.table_state.select(Some(records_state.selected_index));

        // Build rows from display_records
        let rows = records_state.display_records.iter().map(|record| {
            let partition = record.partition.unwrap_or(-1);
            let partition_color = Self::partition_color(partition);

            // Partition cell with color badge
            let partition_cell = Cell::from(format!("[P{}]", partition))
                .style(Style::default().fg(partition_color));

            // Key cell
            let key_str = record
                .key
                .as_ref()
                .and_then(|k| String::from_utf8(k.clone()).ok())
                .unwrap_or_else(|| "<null>".to_string());
            let key_cell = Cell::from(Self::truncate_str(&key_str, 20));

            // Timestamp cell - use base_timestamp if available, otherwise just show delta
            // TODO: Get base_timestamp from record batch when available
            let timestamp_str = Self::format_relative_time(record.timestamp_delta, 0);
            let timestamp_cell = Cell::from(timestamp_str);

            // Offset cell
            let offset_cell = Cell::from(format!("{}", record.offset_delta));

            // Value preview cell
            let value_str = String::from_utf8(record.value.clone())
                .unwrap_or_else(|_| "<binary>".to_string());
            let value_preview = Self::truncate_str(&value_str, 50);
            let value_cell = Cell::from(value_preview);

            Row::new(vec![
                partition_cell,
                key_cell,
                timestamp_cell,
                offset_cell,
                value_cell,
            ])
            .height(1)
        });

        let header = Row::new(vec![
            Cell::from("Part").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Key").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Timestamp").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Offset").style(Style::default().add_modifier(Modifier::BOLD)),
            Cell::from("Value Preview").style(Style::default().add_modifier(Modifier::BOLD)),
        ])
        .height(1)
        .style(Style::default().fg(Color::Yellow));

        let border_style = if focused {
            Style::default().fg(Color::LightGreen)
        } else {
            Style::default().fg(Color::White)
        };

        let selected_row_style = Style::default()
            .bg(Color::Blue)
            .fg(Color::White)
            .add_modifier(Modifier::BOLD);

        // Show loading or empty state
        let title = if records_state.is_loading() {
            " Records (Loading...) "
        } else if records_state.display_records.is_empty() {
            " Records (Empty) "
        } else {
            " Records "
        };

        let table = Table::new(
            rows,
            [
                Constraint::Length(6),  // Partition
                Constraint::Length(22), // Key
                Constraint::Length(12), // Timestamp
                Constraint::Length(10), // Offset
                Constraint::Min(0),     // Value
            ],
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .border_style(border_style),
        )
        .header(header)
        .row_highlight_style(selected_row_style);

        frame.render_stateful_widget(table, area, &mut self.table_state);
    }

    fn render_footer(&self, frame: &mut Frame, area: Rect, app_state: &AppState) {
        let records_state = &app_state.records_state;

        // Build footer status line
        let loading_indicator = match &records_state.loading_state {
            RecordsLoadingState::Loading => Span::styled("● Loading", Style::default().fg(Color::Yellow)),
            RecordsLoadingState::Loaded => Span::styled("● Ready", Style::default().fg(Color::Green)),
            RecordsLoadingState::Error(_) => Span::styled("● Error", Style::default().fg(Color::Red)),
            RecordsLoadingState::Idle => Span::styled("○ Idle", Style::default().fg(Color::DarkGray)),
        };

        let matched_info = format!(
            " | Matched: {}/{}",
            records_state.display_count(),
            records_state.total_count()
        );

        let nav_hint = if records_state.pagination.has_previous || records_state.pagination.has_more {
            " | [n]ext [p]rev"
        } else {
            ""
        };

        let shortcuts = " [s]ort [/]search [r]efresh [i]nfo";

        let footer_line = Line::from(vec![
            loading_indicator,
            Span::raw(matched_info),
            Span::raw(nav_hint),
            Span::styled(shortcuts, Style::default().fg(Color::DarkGray)),
        ]);

        let footer = Paragraph::new(footer_line);
        frame.render_widget(footer, area);
    }

    pub fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> PanelResult {
        match self.input_mode {
            RecordInputMode::Searching => self.handle_search_key(key, app_state),
            RecordInputMode::FilteringPartitions => self.handle_partition_filter_key(key, app_state),
            RecordInputMode::Normal => self.handle_normal_key(key, app_state),
        }
    }

    fn handle_normal_key(&mut self, key: KeyCode, app_state: &mut AppState) -> PanelResult {
        match key {
            KeyCode::Esc | KeyCode::Tab => PanelResult::SwitchFocus(FocusTarget::Topics),
            KeyCode::Up | KeyCode::Char('k') => {
                app_state.records_state.select_previous();
                self.sync_table_state(app_state);
                PanelResult::Handled
            }
            KeyCode::Down | KeyCode::Char('j') => {
                app_state.records_state.select_next();
                self.sync_table_state(app_state);
                PanelResult::Handled
            }
            KeyCode::Char('g') => {
                app_state.records_state.select_first();
                self.sync_table_state(app_state);
                PanelResult::Handled
            }
            KeyCode::Char('G') => {
                app_state.records_state.select_last();
                self.sync_table_state(app_state);
                PanelResult::Handled
            }
            KeyCode::Char('s') => {
                // Cycle sort mode
                app_state.records_state.cycle_sort_mode();
                PanelResult::Handled
            }
            KeyCode::Char('/') => {
                // Enter search mode
                self.input_mode = RecordInputMode::Searching;
                self.search_input = app_state.records_state.filter.search_query.clone();
                PanelResult::Handled
            }
            KeyCode::Char('x') => {
                // Clear search
                app_state.records_state.clear_search();
                PanelResult::Handled
            }
            KeyCode::Char('f') => {
                // Enter partition filter mode
                self.input_mode = RecordInputMode::FilteringPartitions;
                PanelResult::Handled
            }
            KeyCode::Char('n') => {
                // Next page - handled by main_screen which has access to repository
                PanelResult::Handled
            }
            KeyCode::Char('p') => {
                // Previous page - handled by main_screen which has access to repository
                PanelResult::Handled
            }
            KeyCode::Char('r') => {
                // Refresh - handled by main_screen which has access to repository
                PanelResult::Handled
            }
            KeyCode::Char('i') | KeyCode::Enter => {
                // View record detail
                if let Some(record) = app_state.records_state.selected_record() {
                    return PanelResult::ShowModal(ScreenType::MessageDetail(record.clone()));
                }
                PanelResult::Handled
            }
            _ => PanelResult::Handled,
        }
    }

    fn handle_search_key(&mut self, key: KeyCode, app_state: &mut AppState) -> PanelResult {
        match key {
            KeyCode::Esc => {
                // Cancel search and restore previous query
                self.input_mode = RecordInputMode::Normal;
                self.search_input.clear();
                PanelResult::Handled
            }
            KeyCode::Enter => {
                // Apply search
                app_state.records_state.set_search_query(self.search_input.clone());
                self.input_mode = RecordInputMode::Normal;
                PanelResult::Handled
            }
            KeyCode::Backspace => {
                self.search_input.pop();
                // Live search as user types
                app_state.records_state.set_search_query(self.search_input.clone());
                PanelResult::Handled
            }
            KeyCode::Char(c) => {
                self.search_input.push(c);
                // Live search as user types
                app_state.records_state.set_search_query(self.search_input.clone());
                PanelResult::Handled
            }
            _ => PanelResult::Handled,
        }
    }

    fn handle_partition_filter_key(&mut self, key: KeyCode, app_state: &mut AppState) -> PanelResult {
        match key {
            KeyCode::Esc | KeyCode::Enter | KeyCode::Char('f') => {
                self.input_mode = RecordInputMode::Normal;
                PanelResult::Handled
            }
            KeyCode::Char('a') => {
                // Select all partitions
                app_state.records_state.filter.select_all_partitions();
                app_state.records_state.apply_filters_and_sort();
                PanelResult::Handled
            }
            KeyCode::Char('c') => {
                // Clear partition filter (show all)
                app_state.records_state.filter.clear_partition_filter();
                app_state.records_state.apply_filters_and_sort();
                PanelResult::Handled
            }
            KeyCode::Char(c) if c.is_ascii_digit() => {
                // Toggle partition by number
                let partition = c.to_digit(10).unwrap() as i32;
                if app_state.records_state.filter.available_partitions.contains(&partition) {
                    app_state.records_state.toggle_partition(partition);
                }
                PanelResult::Handled
            }
            _ => PanelResult::Handled,
        }
    }

    fn sync_table_state(&mut self, app_state: &AppState) {
        self.table_state.select(Some(app_state.records_state.selected_index));
    }

    /// Get the current input mode
    pub fn input_mode(&self) -> RecordInputMode {
        self.input_mode
    }

    /// Check if currently in search mode
    pub fn is_searching(&self) -> bool {
        self.input_mode == RecordInputMode::Searching
    }
}

impl Default for RecordsPanel {
    fn default() -> Self {
        Self::new()
    }
}
