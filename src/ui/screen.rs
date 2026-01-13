use ratatui::crossterm::event::KeyCode;
use ratatui::Frame;
use ratatui::layout::Rect;
use crate::state::app_state::AppState;
use crate::ui::screen_type::ScreenType;

pub enum EventResult {
    Handled,
    Push(ScreenType),
    Pop,
    Replace(ScreenType),
    Quit,
    Action(AppAction)
}

pub trait Screen: Send {
    fn render(&mut self, frame: &mut Frame, area: Rect, app_state: &mut AppState);
    fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult;
    fn on_mount(&mut self, app_state: &mut AppState) {}
    fn on_unmount(&mut self, app_state: &mut AppState) {}
    fn is_modal(&self) -> bool { false }
}

pub enum AppAction {
    LoadMessages(String),
    SearchTopic(char),
    SearchTopicBackspace,
    ToggleSearchMode,
    ClosePopup,
}
