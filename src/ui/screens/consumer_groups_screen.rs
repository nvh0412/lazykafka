use crate::state::app_state::{AppMessage, AppState, ConsumerGroupListState};
use crate::ui::screen::{EventResult, Screen};
use crate::ui::screen_type::ScreenType;
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Scrollbar, ScrollbarOrientation, Table};
use ratatui::Frame;

pub enum InputMode {
    SelectGroup,
    SearchGroup,
}

pub struct ConsumerGroupsScreen {
    input_mode: InputMode,
}

impl ConsumerGroupsScreen {
    pub fn new() -> Self {
        Self {
            input_mode: InputMode::SelectGroup,
        }
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

    fn update_filters(&self, app_state: &mut AppState) {
        app_state
            .filtered_consumer_groups_state
            .items =
                app_state
                    .consumer_groups
                    .items
                    .iter()
                    .filter(|g| {
                        g.group_id
                            .to_lowercase()
                            .contains(&app_state.consumer_group_search_query.to_lowercase())
                    })
                    .cloned()
                    .collect();
    }

    fn render_header(&self, frame: &mut Frame, area: Rect, app_state: &AppState) {
        let header = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(25),
                Constraint::Min(20),
                Constraint::Length(40),
            ])
            .split(area);

        let title = Paragraph::new("Consumer Groups")
            .style(Style::default().add_modifier(Modifier::BOLD));
        frame.render_widget(title, header[0]);

        let count = format!(
            "Total: {} groups",
            app_state.filtered_consumer_groups_state.items.len()
        );
        frame.render_widget(Paragraph::new(count), header[1]);

        let help = Paragraph::new("Press 'q' to go back | 'r' to refresh");
        frame.render_widget(help, header[2]);
    }

    fn render_groups_table(
        &self,
        frame: &mut Frame,
        area: Rect,
        state: &mut ConsumerGroupListState,
        search_query: &str,
    ) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Search box
                Constraint::Min(0),    // Groups table
            ])
            .split(area);

        // Search box
        let search_text = if search_query.is_empty() {
            "Press / to search groups...".to_string()
        } else {
            search_query.to_string()
        };

        let search_style = match self.input_mode {
            InputMode::SearchGroup => Style::default().fg(Color::White),
            _ => Style::default().fg(Color::Gray),
        };

        let search_box = Paragraph::new(search_text).block(
            Block::default()
                .title("Search")
                .borders(Borders::ALL)
                .border_style(search_style),
        );
        frame.render_widget(search_box, chunks[0]);

        // Groups table
        let header = Row::new(vec!["Group ID", "State", "Protocol", "Members"])
            .style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
            .height(1);

        let rows: Vec<Row> = state
            .items
            .iter()
            .map(|group| {
                Row::new(vec![
                    group.group_id.clone(),
                    group.state.as_str().to_string(),
                    group.protocol_type.clone(),
                    group.member_count.to_string(),
                ])
                .style(Style::default().fg(group.state.display_color()))
            })
            .collect();

        let list_style = match self.input_mode {
            InputMode::SelectGroup => Style::default().fg(Color::White),
            _ => Style::default().fg(Color::Gray),
        };

        let table = Table::new(
            rows,
            [
                Constraint::Percentage(40),
                Constraint::Percentage(20),
                Constraint::Percentage(20),
                Constraint::Percentage(20),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .title("Consumer Groups (Press Enter for details)")
                .borders(Borders::ALL)
                .border_style(list_style),
        )
        .row_highlight_style(
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");

        state.vertical_scroll_state = state
            .vertical_scroll_state
            .content_length(state.items.len());

        // frame.render_stateful_widget(table, chunks[1], &mut state.table_state);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            chunks[1],
            &mut state.vertical_scroll_state,
        );
    }
}

impl Screen for ConsumerGroupsScreen {
    fn render(&mut self, frame: &mut Frame, _area: Rect, app_state: &mut AppState) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // Header
                Constraint::Min(0),    // Body
            ])
            .split(frame.area());

        self.render_header(frame, chunks[0], app_state);
        self.render_groups_table(
            frame,
            chunks[1],
            &mut app_state.filtered_consumer_groups_state,
            &app_state.consumer_group_search_query,
        );
    }

    fn on_mount(&mut self, app_state: &mut AppState) {
        self.load_consumer_groups(app_state);
    }

    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        EventResult::Handled
        // match self.input_mode {
        //     InputMode::SelectGroup => match key {
        //         KeyCode::Char('q') | KeyCode::Esc => EventResult::Replace(ScreenType::Main),
        //         KeyCode::Char('/') => {
        //             self.input_mode = InputMode::SearchGroup;
        //             EventResult::Handled
        //         }
        //         KeyCode::Char('i') => {
        //             if let Some(group_id) = app_state.filtered_consumer_groups_state.table_state.selected() {
        //                 EventResult::Push(ScreenType::ConsumerGroupDetail(group_id.to_string()))
        //             } else {
        //                 EventResult::Handled
        //             }
        //         }
        //         KeyCode::Up => {
        //             app_state
        //                 .filtered_consumer_groups_state
        //                 .table_state
        //                 .select_previous();
        //             app_state.filtered_consumer_groups_state.vertical_scroll = app_state
        //                 .filtered_consumer_groups_state
        //                 .vertical_scroll
        //                 .saturating_sub(1);
        //             app_state.filtered_consumer_groups_state.vertical_scroll_state = app_state
        //                 .filtered_consumer_groups_state
        //                 .vertical_scroll_state
        //                 .position(app_state.filtered_consumer_groups_state.vertical_scroll);
        //             EventResult::Handled
        //         }
        //         KeyCode::Down => {
        //             app_state.filtered_consumer_groups_state.select_next(app_state.filtered_consumer_groups_state.items.len());
        //             app_state.filtered_consumer_groups_state.vertical_scroll = app_state
        //                 .filtered_consumer_groups_state
        //                 .vertical_scroll
        //                 .saturating_add(1);
        //             app_state.filtered_consumer_groups_state.vertical_scroll_state = app_state
        //                 .filtered_consumer_groups_state
        //                 .vertical_scroll_state
        //                 .position(app_state.filtered_consumer_groups_state.vertical_scroll);
        //             EventResult::Handled
        //         }
        //         KeyCode::Enter => {
        //             if let Some(group_idx) = app_state
        //                 .filtered_consumer_groups_state
        //                 .table_state
        //                 .selected()
        //             {
        //                 let group = &app_state.filtered_consumer_groups_state.items[group_idx];
        //                 EventResult::Push(ScreenType::ConsumerGroupDetail(group.group_id.clone()))
        //             } else {
        //                 EventResult::Handled
        //             }
        //         }
        //         KeyCode::Char('r') => {
        //             self.load_consumer_groups(app_state);
        //             EventResult::Handled
        //         }
        //         _ => EventResult::Handled,
        //     },
        //     InputMode::SearchGroup => match key {
        //         KeyCode::Char(c) => {
        //             app_state.consumer_group_search_query.push(c);
        //             self.update_filters(app_state);
        //             EventResult::Handled
        //         }
        //         KeyCode::Backspace => {
        //             app_state.consumer_group_search_query.pop();
        //             self.update_filters(app_state);
        //             EventResult::Handled
        //         }
        //         KeyCode::Esc => {
        //             app_state.consumer_group_search_query.clear();
        //             self.update_filters(app_state);
        //             self.input_mode = InputMode::SelectGroup;
        //             EventResult::Handled
        //         }
        //         KeyCode::Enter => {
        //             self.input_mode = InputMode::SelectGroup;
        //             EventResult::Handled
        //         }
        //         _ => EventResult::Handled,
        //     },
        // }
    }
}
