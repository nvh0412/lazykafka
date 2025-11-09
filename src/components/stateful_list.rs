use ratatui::widgets::ListState;

pub struct StatefulList<T> {
    pub items: Vec<T>,
    pub state: ListState,
}

impl<T> StatefulList<T> {
    pub fn with_items(items: Vec<T>) -> Self {
        Self {
            items,
            state: ListState::default(),
        }
    }
    pub fn next(&mut self) { /* ... */
    }
    pub fn previous(&mut self) { /* ... */
    }
    pub fn select(&mut self, index: Option<usize>) { /* ... */
    }
}
