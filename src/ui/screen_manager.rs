use crate::state::app_state::AppState;
use crate::ui::screen::{EventResult, Screen};
use crate::ui::screen_type::{create_screen, ScreenType};
use ratatui::crossterm::event::KeyCode;
use ratatui::Frame;

pub struct ScreenManager {
    stack: Vec<Box<dyn Screen>>,
}

impl ScreenManager {
    pub fn default(app_state: &mut AppState) -> Self {
        let mut res = Self { stack: vec![] };
        res.push(ScreenType::Main, app_state);
        res
    }
}

#[allow(dead_code)]
impl ScreenManager {
    pub fn render(&mut self, frame: &mut Frame, app_state: &mut AppState) {
        for v in self.stack.iter_mut() {
            v.render(frame, frame.area(), app_state);
        }
    }

    pub fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        self.stack.last_mut().unwrap().handle_key(key, app_state)
    }

    pub fn push(&mut self, screen_type: ScreenType, app_state: &mut AppState) {
        let mut screen = create_screen(screen_type);
        screen.on_mount(app_state);
        self.stack.push(screen);
    }
    pub fn pop(&mut self, app_state: &mut AppState) -> bool {
        if let Some(mut screen) = self.stack.pop() {
            screen.on_unmount(app_state);
            true
        } else {
            false
        }
    }
    pub fn replace(&mut self, _screen_type: ScreenType, _app_state: &mut AppState) {
        if let Some(mut screen) = self.stack.pop() {
            screen.on_unmount(_app_state);
        }
        let mut screen = create_screen(_screen_type);
        screen.on_mount(_app_state);
        self.stack.push(screen);
    }
}
