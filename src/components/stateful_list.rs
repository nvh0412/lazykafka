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
    pub fn next(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i >= self.items.len() - 1 {
                    0 // Wrap to beginning
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i)); // ← Using ratatui's select()
    }
    pub fn previous(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i == 0 {
                    self.items.len() - 1 // Wrap to end
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }

    pub fn selected(&self) -> Option<&T> {
        self.state.selected().and_then(|i| self.items.get(i))
    }
}
