use crate::state::app_state::AppState;
use crate::ui::screen_type::ScreenType;
use ratatui::Frame;
use ratatui::crossterm::event::KeyCode;
use ratatui::layout::Rect;

#[allow(dead_code)]
pub enum EventResult {
    Handled,
    Push(ScreenType),
    Pop,
    Replace(ScreenType),
    Quit,
    Action(AppAction),
}

#[allow(dead_code)]
pub trait Screen: Send {
    fn render(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState);
    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult;
    fn on_mount(&mut self, _app_state: &mut AppState) {}
    fn on_unmount(&mut self, _app_state: &mut AppState) {}
    fn is_modal(&self) -> bool {
        false
    }
}

#[allow(dead_code)]
pub enum AppAction {
    ClosePopup,
    DeletedTopic(String),
    PartitionsCreated { topic: String, new_count: i32 },
}
