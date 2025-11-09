pub struct FilterState {
    pub search_mode: bool,
    pub query: String,
    pub filters: MessageFilters,
}

pub struct MessageFilters {
    pub partition: Option<i32>,
    pub offset_range: Option<(i64, i64)>,
    // pub timestamp_range: Option<(DateTime, DateTime)>,
}

impl Default for MessageFilters {
    fn default() -> Self {
        Self {
            partition: Default::default(),
            offset_range: Default::default(),
        }
    }
}

impl Default for FilterState {
    fn default() -> Self {
        Self {
            search_mode: Default::default(),
            query: Default::default(),
            filters: Default::default(),
        }
    }
}
