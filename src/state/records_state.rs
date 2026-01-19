use crate::protocol::record::Record;
use crate::repository::message_repository::{RecordFetchResult, DEFAULT_PAGE_SIZE};
use std::collections::{HashMap, HashSet};

/// How records are sorted in the display
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum RecordSortMode {
    #[default]
    NewestFirst,
    OldestFirst,
    OffsetAsc,
    OffsetDesc,
}

impl RecordSortMode {
    /// Cycle to the next sort mode
    pub fn next(self) -> Self {
        match self {
            RecordSortMode::NewestFirst => RecordSortMode::OldestFirst,
            RecordSortMode::OldestFirst => RecordSortMode::OffsetAsc,
            RecordSortMode::OffsetAsc => RecordSortMode::OffsetDesc,
            RecordSortMode::OffsetDesc => RecordSortMode::NewestFirst,
        }
    }

    /// Get display name for the sort mode
    pub fn display_name(&self) -> &'static str {
        match self {
            RecordSortMode::NewestFirst => "Newest \u{2193}",
            RecordSortMode::OldestFirst => "Oldest \u{2191}",
            RecordSortMode::OffsetAsc => "Offset \u{2191}",
            RecordSortMode::OffsetDesc => "Offset \u{2193}",
        }
    }
}

/// State for pagination
#[derive(Clone, Debug, Default)]
pub struct PaginationState {
    /// Current page number (1-indexed for display)
    pub current_page: usize,
    /// Records per page
    pub page_size: usize,
    /// Starting offsets for current page (per partition)
    pub start_offsets: HashMap<i32, i64>,
    /// Ending offsets after current page (per partition)
    pub end_offsets: HashMap<i32, i64>,
    /// Earliest available offsets (per partition)
    pub earliest_offsets: HashMap<i32, i64>,
    /// Latest available offsets (per partition)
    pub latest_offsets: HashMap<i32, i64>,
    /// Total estimated record count
    pub total_estimated: usize,
    /// Whether there are more records (next page available)
    pub has_more: bool,
    /// Whether there are previous records (prev page available)
    pub has_previous: bool,
}

impl PaginationState {
    pub fn new() -> Self {
        Self {
            current_page: 1,
            page_size: DEFAULT_PAGE_SIZE,
            ..Default::default()
        }
    }

    /// Calculate approximate total pages
    pub fn total_pages(&self) -> usize {
        if self.page_size == 0 {
            return 0;
        }
        ((self.total_estimated + self.page_size - 1) / self.page_size).max(1)
    }

    /// Update pagination state from fetch result
    pub fn update_from_result(&mut self, result: &RecordFetchResult) {
        self.start_offsets = result.start_offsets.clone();
        self.end_offsets = result.end_offsets.clone();
        self.earliest_offsets = result.earliest_offsets.clone();
        self.latest_offsets = result.latest_offsets.clone();
        self.total_estimated = result.total_estimated;
        self.has_more = result.has_more;
        self.has_previous = result.has_previous;
    }
}

/// State for filtering records
#[derive(Clone, Debug, Default)]
pub struct RecordFilterState {
    /// Search query for filtering by key/value content
    pub search_query: String,
    /// Selected partitions to show (empty = show all)
    pub selected_partitions: HashSet<i32>,
    /// All available partitions
    pub available_partitions: Vec<i32>,
}

impl RecordFilterState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a partition filter is active
    pub fn has_partition_filter(&self) -> bool {
        !self.selected_partitions.is_empty()
            && self.selected_partitions.len() < self.available_partitions.len()
    }

    /// Toggle a partition's selection
    pub fn toggle_partition(&mut self, partition: i32) {
        if self.selected_partitions.contains(&partition) {
            self.selected_partitions.remove(&partition);
        } else {
            self.selected_partitions.insert(partition);
        }
    }

    /// Select all partitions
    pub fn select_all_partitions(&mut self) {
        self.selected_partitions.clear();
        for p in &self.available_partitions {
            self.selected_partitions.insert(*p);
        }
    }

    /// Clear partition filter (show all)
    pub fn clear_partition_filter(&mut self) {
        self.selected_partitions.clear();
    }

    /// Check if a partition should be shown
    pub fn should_show_partition(&self, partition: i32) -> bool {
        self.selected_partitions.is_empty() || self.selected_partitions.contains(&partition)
    }
}

/// Loading state for records
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum RecordsLoadingState {
    #[default]
    Idle,
    Loading,
    Loaded,
    Error(String),
}

/// Complete state management for records panel
#[derive(Clone, Debug)]
pub struct RecordsState {
    /// Currently loaded records (raw from fetch)
    pub records: Vec<Record>,
    /// Filtered and sorted records for display
    pub display_records: Vec<Record>,
    /// Currently selected topic name
    pub topic_name: Option<String>,
    /// Pagination state
    pub pagination: PaginationState,
    /// Sort mode
    pub sort_mode: RecordSortMode,
    /// Filter state
    pub filter: RecordFilterState,
    /// Loading state
    pub loading_state: RecordsLoadingState,
    /// Selected row index in the table
    pub selected_index: usize,
}

impl Default for RecordsState {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordsState {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            display_records: Vec::new(),
            topic_name: None,
            pagination: PaginationState::new(),
            sort_mode: RecordSortMode::default(),
            filter: RecordFilterState::new(),
            loading_state: RecordsLoadingState::Idle,
            selected_index: 0,
        }
    }

    /// Set the current topic and reset state
    pub fn set_topic(&mut self, topic_name: String, partitions: Vec<i32>) {
        self.topic_name = Some(topic_name);
        self.records.clear();
        self.display_records.clear();
        self.pagination = PaginationState::new();
        self.filter = RecordFilterState {
            available_partitions: partitions,
            ..Default::default()
        };
        self.selected_index = 0;
        self.loading_state = RecordsLoadingState::Loading;
    }

    /// Update records from a fetch result
    pub fn update_from_result(&mut self, result: RecordFetchResult) {
        self.pagination.update_from_result(&result);
        self.records = result.records;
        self.loading_state = RecordsLoadingState::Loaded;
        self.apply_filters_and_sort();
    }

    /// Set error state
    pub fn set_error(&mut self, error: String) {
        self.loading_state = RecordsLoadingState::Error(error);
    }

    /// Apply filters and sorting to produce display_records
    pub fn apply_filters_and_sort(&mut self) {
        // Start with all records
        let mut filtered: Vec<Record> = self.records.clone();

        // Apply partition filter
        if self.filter.has_partition_filter() {
            filtered.retain(|r| {
                r.partition
                    .map(|p| self.filter.should_show_partition(p))
                    .unwrap_or(true)
            });
        }

        // Apply search filter
        if !self.filter.search_query.is_empty() {
            let query = self.filter.search_query.to_lowercase();
            filtered.retain(|r| {
                // Check key
                let key_matches = r
                    .key
                    .as_ref()
                    .and_then(|k| String::from_utf8(k.clone()).ok())
                    .map(|k| k.to_lowercase().contains(&query))
                    .unwrap_or(false);

                // Check value
                let value_matches = String::from_utf8(r.value.clone())
                    .ok()
                    .map(|v| v.to_lowercase().contains(&query))
                    .unwrap_or(false);

                key_matches || value_matches
            });
        }

        // Apply sorting
        match self.sort_mode {
            RecordSortMode::NewestFirst => {
                filtered.sort_by(|a, b| b.timestamp_delta.cmp(&a.timestamp_delta));
            }
            RecordSortMode::OldestFirst => {
                filtered.sort_by(|a, b| a.timestamp_delta.cmp(&b.timestamp_delta));
            }
            RecordSortMode::OffsetAsc => {
                filtered.sort_by(|a, b| {
                    let a_offset = a.partition.unwrap_or(0) as i64 * 1_000_000_000 + a.offset_delta;
                    let b_offset = b.partition.unwrap_or(0) as i64 * 1_000_000_000 + b.offset_delta;
                    a_offset.cmp(&b_offset)
                });
            }
            RecordSortMode::OffsetDesc => {
                filtered.sort_by(|a, b| {
                    let a_offset = a.partition.unwrap_or(0) as i64 * 1_000_000_000 + a.offset_delta;
                    let b_offset = b.partition.unwrap_or(0) as i64 * 1_000_000_000 + b.offset_delta;
                    b_offset.cmp(&a_offset)
                });
            }
        }

        self.display_records = filtered;

        // Ensure selected index is within bounds
        if !self.display_records.is_empty() && self.selected_index >= self.display_records.len() {
            self.selected_index = self.display_records.len() - 1;
        }
    }

    /// Cycle to the next sort mode
    pub fn cycle_sort_mode(&mut self) {
        self.sort_mode = self.sort_mode.next();
        self.apply_filters_and_sort();
    }

    /// Set search query and re-filter
    pub fn set_search_query(&mut self, query: String) {
        self.filter.search_query = query;
        self.apply_filters_and_sort();
    }

    /// Clear search query
    pub fn clear_search(&mut self) {
        self.filter.search_query.clear();
        self.apply_filters_and_sort();
    }

    /// Toggle a partition filter
    pub fn toggle_partition(&mut self, partition: i32) {
        self.filter.toggle_partition(partition);
        self.apply_filters_and_sort();
    }

    /// Get the currently selected record
    pub fn selected_record(&self) -> Option<&Record> {
        self.display_records.get(self.selected_index)
    }

    /// Move selection up
    pub fn select_previous(&mut self) {
        if self.selected_index > 0 {
            self.selected_index -= 1;
        }
    }

    /// Move selection down
    pub fn select_next(&mut self) {
        if self.selected_index < self.display_records.len().saturating_sub(1) {
            self.selected_index += 1;
        }
    }

    /// Jump to first record
    pub fn select_first(&mut self) {
        self.selected_index = 0;
    }

    /// Jump to last record
    pub fn select_last(&mut self) {
        if !self.display_records.is_empty() {
            self.selected_index = self.display_records.len() - 1;
        }
    }

    /// Get count of displayed (filtered) records
    pub fn display_count(&self) -> usize {
        self.display_records.len()
    }

    /// Get total count of loaded records
    pub fn total_count(&self) -> usize {
        self.records.len()
    }

    /// Check if currently loading
    pub fn is_loading(&self) -> bool {
        matches!(self.loading_state, RecordsLoadingState::Loading)
    }

    /// Get status text for footer
    pub fn status_text(&self) -> String {
        match &self.loading_state {
            RecordsLoadingState::Idle => "Ready".to_string(),
            RecordsLoadingState::Loading => "Loading...".to_string(),
            RecordsLoadingState::Loaded => {
                let filtered = self.display_count();
                let total = self.total_count();
                if filtered == total {
                    format!("{} records", total)
                } else {
                    format!("{}/{} matched", filtered, total)
                }
            }
            RecordsLoadingState::Error(e) => format!("Error: {}", e),
        }
    }
}
