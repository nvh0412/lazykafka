use crate::state::app_state::AppState;
use crate::ui::screen::{ConfirmAction, EventResult, Screen};
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

pub struct ConfirmDialogModal {
    title: String,
    message: String,
}

impl ConfirmDialogModal {
    pub fn new(title: String, message: String) -> Self {
        Self { title, message }
    }

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

impl Screen for ConfirmDialogModal {
    fn render(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState) {
        let popup_block = Block::default()
            .title(self.title.clone())
            .borders(Borders::ALL)
            .title_bottom(Line::from("Yes (Y/y)").right_aligned())
            .title_bottom(Line::from("No (N/n)").right_aligned());

        let area = self.centered_rect(30, 20, frame.area());
        frame.render_widget(Clear, area);

        let content = Paragraph::new(self.message.clone())
            .wrap(Wrap { trim: true })
            .block(popup_block);

        frame.render_widget(content, area);
    }

    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        match key {
            KeyCode::Char('y') | KeyCode::Char('Y') => EventResult::Quit(ConfirmAction::Accept),
            KeyCode::Char('n') | KeyCode::Char('N') => EventResult::Quit(ConfirmAction::Decline),
            _ => EventResult::Handled,
        }
    }

    fn is_modal(&self) -> bool {
        true
    }
}
