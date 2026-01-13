use ratatui::crossterm::event::KeyCode;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use crate::state::app_state::AppState;
use crate::ui::screen::{EventResult, Screen};
use crate::ui::screen_type::{create_screen, ScreenType};

pub struct ScreenManager {
    stack: Vec<Box<dyn Screen>>,
}

impl Default for ScreenManager {
    fn default() -> Self {
        let mut res = Self { stack: vec![] };
        res.push(ScreenType::Main);
        res
    }
}

impl ScreenManager {
    pub fn render(&mut self, frame: &mut Frame, app_state: &mut AppState) {
        for v in self.stack.iter_mut() {
            v.render(frame, frame.area(), app_state);
        }
    }

    pub fn handle_key(&mut self, key: KeyCode, app_state: &mut AppState) -> EventResult {
        self.stack.last_mut().unwrap().handle_key(key, app_state)
    }

    pub fn push(&mut self, screen_type: ScreenType) {
        self.stack.push(create_screen(screen_type));
    }
    pub fn pop(&mut self) -> bool {
        self.stack.pop().is_some()
    }
    pub fn replace(&mut self, screen_type: ScreenType, app_state: &mut AppState) {}
}
