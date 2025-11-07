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
