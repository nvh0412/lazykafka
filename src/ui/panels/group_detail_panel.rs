use crate::models::consumer_group::ConsumerGroupDetail;
use crate::state::app_state::{AppMessage, AppState, ConsumerGroupTab};
use crate::ui::panels::{FocusTarget, PanelResult};
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs};
use ratatui::Frame;

pub struct GroupDetailPanel {
    group_id: String,
    loaded: bool,
}

impl GroupDetailPanel {
    pub fn new(group_id: String) -> Self {
        Self {
            group_id,
            loaded: false,
        }
    }

    pub fn load_detail(&mut self, app_state: &mut AppState) {
        let tx = app_state.tx.clone();
        let repo = app_state.repository_manager.consumer_groups().clone();
        let group_id = self.group_id.clone();

        // Reset state before loading
        app_state.consumer_group_detail_state.detail = None;
        app_state.consumer_group_detail_state.active_tab = ConsumerGroupTab::Overview;

        tokio::spawn(async move {
            match repo.fetch_detail(group_id).await {
                Ok(detail) => {
                    let _ = tx.send(AppMessage::ConsumerGroupDetailLoaded(detail));
                }
                Err(_e) => {
                    let _ = tx.send(AppMessage::ConsumerGroupError);
                }
            }
        });

        self.loaded = true;
    }

    pub fn render(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        app_state: &mut AppState,
        focused: bool,
    ) {
        // Load data on first render if needed
        if !self.loaded {
            self.load_detail(app_state);
        }

        let detail = match &app_state.consumer_group_detail_state.detail {
            Some(d) if d.group_id == self.group_id => d.clone(),
            _ => {
                // Loading state
                let loading = Paragraph::new("Loading consumer group details...").block(
                    Block::default()
                        .title(format!("Consumer Group: {}", self.group_id))
                        .borders(Borders::ALL),
                );
                frame.render_widget(loading, area);
                return;
            }
        };

        self.render_detail(frame, area, &detail, app_state, focused);
    }

    fn render_detail(
        &self,
        frame: &mut Frame,
        area: Rect,
        detail: &ConsumerGroupDetail,
        app_state: &mut AppState,
        focused: bool,
    ) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(0)
            .constraints([
                Constraint::Length(3), // Tabs
                Constraint::Length(5), // Info header
                Constraint::Min(0),    // Content
            ])
            .split(area);

        let border_style = if focused {
            Style::default().fg(Color::LightGreen)
        } else {
            Style::default().fg(Color::White)
        };

        // Tabs
        let tab_titles = vec!["Overview", "Members", "Offsets/Lag"];
        let tabs = Tabs::new(tab_titles)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(border_style)
                    .title(format!("Consumer Group: {}", detail.group_id)),
            )
            .select(match app_state.consumer_group_detail_state.active_tab {
                ConsumerGroupTab::Overview => 0,
                ConsumerGroupTab::Members => 1,
                ConsumerGroupTab::Offsets => 2,
            })
            .style(Style::default().fg(Color::White))
            .highlight_style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            );
        frame.render_widget(tabs, chunks[0]);

        // Info header
        self.render_info_header(frame, chunks[1], detail);

        // Tab content
        match app_state.consumer_group_detail_state.active_tab {
            ConsumerGroupTab::Overview => self.render_overview(frame, chunks[2], detail),
            ConsumerGroupTab::Members => self.render_members(frame, chunks[2], detail, app_state),
            ConsumerGroupTab::Offsets => self.render_offsets(frame, chunks[2], detail, app_state),
        }
    }

    fn render_info_header(&self, frame: &mut Frame, area: Rect, detail: &ConsumerGroupDetail) {
        let info_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area);

        // Left: Group info
        let state_color = detail.state.display_color();
        let left_info = format!(
            "State: {}\nProtocol: {} ({})",
            detail.state.as_str(),
            detail.protocol_type,
            detail.protocol
        );
        let left_para = Paragraph::new(left_info)
            .block(Block::default().borders(Borders::ALL).title("Group Info"))
            .style(Style::default().fg(state_color));
        frame.render_widget(left_para, info_chunks[0]);

        // Right: Coordinator
        let coordinator_info = if let Some(coord) = &detail.coordinator {
            format!(
                "Node: {}\nHost: {}:{}",
                coord.node_id, coord.host, coord.port
            )
        } else {
            "Unknown".to_string()
        };
        let right_para = Paragraph::new(coordinator_info)
            .block(Block::default().borders(Borders::ALL).title("Coordinator"));
        frame.render_widget(right_para, info_chunks[1]);
    }

    fn render_overview(&self, frame: &mut Frame, area: Rect, detail: &ConsumerGroupDetail) {
        let total_lag = detail.total_lag();
        let lag_color = if total_lag > 10000 {
            Color::Red
        } else if total_lag > 1000 {
            Color::Yellow
        } else {
            Color::Green
        };

        let summary = format!(
            "Members: {}\nAssigned Partitions: {}\nTopics: {}\nTotal Lag: {}",
            detail.members.len(),
            detail.partition_count(),
            detail.topic_count(),
            total_lag
        );

        let para = Paragraph::new(summary)
            .block(Block::default().borders(Borders::ALL).title("Summary"))
            .style(Style::default().fg(lag_color));
        frame.render_widget(para, area);
    }

    fn render_members(
        &self,
        frame: &mut Frame,
        area: Rect,
        detail: &ConsumerGroupDetail,
        app_state: &mut AppState,
    ) {
        let header = Row::new(vec!["Member ID", "Client ID", "Host", "Assignments"])
            .style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
            .height(1);

        let rows: Vec<Row> = detail
            .members
            .iter()
            .map(|m| {
                let assigns: String = m
                    .assignments
                    .iter()
                    .map(|a| format!("{}[{}]", a.topic, a.partitions.len()))
                    .collect::<Vec<_>>()
                    .join(", ");
                Row::new(vec![
                    truncate_string(&m.member_id, 30),
                    m.client_id.clone(),
                    m.client_host.clone(),
                    assigns,
                ])
            })
            .collect();

        let table = Table::new(
            rows,
            [
                Constraint::Percentage(30),
                Constraint::Percentage(20),
                Constraint::Percentage(20),
                Constraint::Percentage(30),
            ],
        )
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Members"))
        .row_highlight_style(Style::default().bg(Color::DarkGray));

        frame.render_stateful_widget(
            table,
            area,
            &mut app_state.consumer_group_detail_state.members_table_state,
        );
    }

    fn render_offsets(
        &self,
        frame: &mut Frame,
        area: Rect,
        detail: &ConsumerGroupDetail,
        app_state: &mut AppState,
    ) {
        let header = Row::new(vec!["Topic", "Partition", "Committed", "End", "Lag"])
            .style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
            .height(1);

        let rows: Vec<Row> = detail
            .topic_offsets
            .iter()
            .flat_map(|t| {
                t.partitions.iter().map(move |p| {
                    let lag_color = if p.lag > 1000 {
                        Color::Red
                    } else if p.lag > 100 {
                        Color::Yellow
                    } else {
                        Color::Green
                    };
                    Row::new(vec![
                        Cell::from(t.topic.clone()),
                        Cell::from(p.partition.to_string()),
                        Cell::from(p.committed_offset.to_string()),
                        Cell::from(p.end_offset.to_string()),
                        Cell::from(p.lag.to_string()).style(Style::default().fg(lag_color)),
                    ])
                })
            })
            .collect();

        let table = Table::new(
            rows,
            [
                Constraint::Percentage(30),
                Constraint::Percentage(15),
                Constraint::Percentage(20),
                Constraint::Percentage(20),
                Constraint::Percentage(15),
            ],
        )
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Offsets & Lag"),
        )
        .row_highlight_style(Style::default().bg(Color::DarkGray));

        frame.render_stateful_widget(
            table,
            area,
            &mut app_state.consumer_group_detail_state.offsets_table_state,
        );
    }

    pub fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> PanelResult {
        match key {
            KeyCode::Esc => PanelResult::SwitchFocus(FocusTarget::Groups),
            KeyCode::Tab | KeyCode::Right => {
                app_state.consumer_group_detail_state.active_tab =
                    match app_state.consumer_group_detail_state.active_tab {
                        ConsumerGroupTab::Overview => ConsumerGroupTab::Members,
                        ConsumerGroupTab::Members => ConsumerGroupTab::Offsets,
                        ConsumerGroupTab::Offsets => ConsumerGroupTab::Overview,
                    };
                PanelResult::Handled
            }
            KeyCode::BackTab | KeyCode::Left => {
                app_state.consumer_group_detail_state.active_tab =
                    match app_state.consumer_group_detail_state.active_tab {
                        ConsumerGroupTab::Overview => ConsumerGroupTab::Offsets,
                        ConsumerGroupTab::Members => ConsumerGroupTab::Overview,
                        ConsumerGroupTab::Offsets => ConsumerGroupTab::Members,
                    };
                PanelResult::Handled
            }
            KeyCode::Char('r') => {
                self.loaded = false; // Force reload
                PanelResult::Handled
            }
            KeyCode::Up | KeyCode::Char('k') => {
                match app_state.consumer_group_detail_state.active_tab {
                    ConsumerGroupTab::Members => {
                        let state = &mut app_state.consumer_group_detail_state.members_table_state;
                        let i = state.selected().unwrap_or(0).saturating_sub(1);
                        state.select(Some(i));
                    }
                    ConsumerGroupTab::Offsets => {
                        let state = &mut app_state.consumer_group_detail_state.offsets_table_state;
                        let i = state.selected().unwrap_or(0).saturating_sub(1);
                        state.select(Some(i));
                    }
                    _ => {}
                }
                PanelResult::Handled
            }
            KeyCode::Down | KeyCode::Char('j') => {
                match app_state.consumer_group_detail_state.active_tab {
                    ConsumerGroupTab::Members => {
                        let state = &mut app_state.consumer_group_detail_state.members_table_state;
                        let i = state.selected().unwrap_or(0).saturating_add(1);
                        state.select(Some(i));
                    }
                    ConsumerGroupTab::Offsets => {
                        let state = &mut app_state.consumer_group_detail_state.offsets_table_state;
                        let i = state.selected().unwrap_or(0).saturating_add(1);
                        state.select(Some(i));
                    }
                    _ => {}
                }
                PanelResult::Handled
            }
            _ => PanelResult::Handled,
        }
    }
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() > max_len {
        format!("{}...", &s[..max_len - 3])
    } else {
        s.to_string()
    }
}
