use crate::state::app_state::AppState;
use crate::ui::panels::{FocusTarget, PanelResult};
use crate::ui::screen_type::ScreenType;
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Rect};
use ratatui::prelude::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Cell, Row, Table, TableState};
use ratatui::Frame;

pub struct RecordsPanel {
    table_state: TableState,
}

impl RecordsPanel {
    pub fn new() -> Self {
        Self {
            table_state: TableState::default().with_selected(0),
        }
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState, focused: bool) {
        let rows = app_state.records.items.iter().map(|data| {
            let item = data.ref_array();
            item.into_iter()
                .map(|content| Cell::from(format!("\n{content}\n")))
                .collect::<Row>()
                .height(3)
        });

        let list_style = if focused {
            Style::default().fg(Color::LightGreen)
        } else {
            Style::default().fg(Color::White)
        };

        let header = ["Key", "Partition", "Timestamp", "Offset", "Value"]
            .into_iter()
            .map(|t| Cell::from(t.to_string()))
            .collect::<Row>()
            .height(1);

        let selected_row_style = Style::default()
            .bg(Color::Blue)
            .fg(Color::White)
            .add_modifier(Modifier::BOLD);

        let t = Table::new::<_, [Constraint; 5]>(
            rows,
            [
                Constraint::Length(30),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Min(0),
            ],
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Records")
                .border_style(list_style),
        )
        .header(header)
        .row_highlight_style(selected_row_style);

        frame.render_stateful_widget(t, area, &mut self.table_state);
    }

    pub fn handle_key(&mut self, key: KeyCode, app_state: &AppState) -> PanelResult {
        match key {
            KeyCode::Esc | KeyCode::Tab => PanelResult::SwitchFocus(FocusTarget::Topics),
            KeyCode::Up | KeyCode::Char('k') => {
                self.previous_row();
                PanelResult::Handled
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.next_row();
                PanelResult::Handled
            }
            KeyCode::Char('i') => {
                if let Some(record_idx) = self.table_state.selected() {
                    if record_idx < app_state.records.items.len() {
                        let record = app_state.records.items[record_idx].clone();
                        return PanelResult::ShowModal(ScreenType::MessageDetail(record));
                    }
                }
                PanelResult::Handled
            }
            _ => PanelResult::Handled,
        }
    }

    fn next_row(&mut self) {
        let i = self.table_state.selected().unwrap_or(0);
        self.table_state.select(Some(i.saturating_add(1)));
    }

    fn previous_row(&mut self) {
        let i = self.table_state.selected().unwrap_or(0);
        self.table_state.select(Some(i.saturating_sub(1)));
    }
}

impl Default for RecordsPanel {
    fn default() -> Self {
        Self::new()
    }
}
