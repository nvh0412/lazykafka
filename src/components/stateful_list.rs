use ratatui::widgets::ListState;

pub struct StatefulList<T> {
    pub items: Vec<T>,
    pub state: ListState,
}

impl<T> StatefulList<T> {
    pub fn next(&mut self) { /* ... */
    }
    pub fn previous(&mut self) { /* ... */
    }
    pub fn select(&mut self, index: Option<usize>) { /* ... */
    }
}
