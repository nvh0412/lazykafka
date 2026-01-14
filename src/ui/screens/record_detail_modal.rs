use ratatui::crossterm::event::KeyCode;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use crate::protocol::record::Record;
use crate::state::app_state::AppState;
use crate::ui::screen::{EventResult, Screen};
use crate::ui::screen::AppAction::ClosePopup;

pub struct RecordDetailModal {
    record: Record
}

impl RecordDetailModal {
    pub fn new(record: Record) -> Self {
        Self { record }
    }
}

impl RecordDetailModal {
    fn centered_rect(&self, percent_x: u16, percent_y: u16, r: Rect) -> Rect {
        // Cut the given rectangle into three vertical pieces
        let popup_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage((100 - percent_y) / 2),
                Constraint::Percentage(percent_y),
                Constraint::Percentage((100 - percent_y) / 2),
            ])
            .split(r);

        // Then cut the middle vertical piece into three width-wise pieces
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage((100 - percent_x) / 2),
                Constraint::Percentage(percent_x),
                Constraint::Percentage((100 - percent_x) / 2),
            ])
            .split(popup_layout[1])[1] // Return the middle chunk
    }
}

impl Screen for RecordDetailModal {
    fn render(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let popup_block = Block::default().title("Record Detail").borders(Borders::ALL);

        let area = self.centered_rect(60, 60, frame.area());
        frame.render_widget(Clear, area);

        let content = Paragraph::new(format!("{:#?}", String::from_utf8(self.record.value.clone()).unwrap_or_default())).block(popup_block);

        frame.render_widget(content, area);
    }

    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        match key {
            KeyCode::Esc => EventResult::Action(ClosePopup),
            _ => EventResult::Handled
        }
    }
}
