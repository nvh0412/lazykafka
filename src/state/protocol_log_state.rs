use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;

use crate::models::protocol_log::{ApiType, ProtocolLogEntry, ProtocolLogFilter};

/// Maximum number of log entries to keep
const MAX_LOG_ENTRIES: usize = 1000;

/// State for protocol logging
#[derive(Debug)]
pub struct ProtocolLogState {
    /// Log entries stored in a ring buffer
    pub entries: VecDeque<ProtocolLogEntry>,
    /// Currently selected log entry index (for when focused)
    pub selected_index: Option<usize>,
    /// Scroll offset for display
    pub scroll_offset: usize,
    /// Current filter
    pub filter: ProtocolLogFilter,
    /// Whether auto-scroll is paused
    pub is_paused: bool,
    /// Pending requests waiting for responses (correlation_id -> (timestamp, api_type, api_version, broker))
    pub pending_requests: HashMap<i32, (SystemTime, ApiType, i16, String)>,
    /// Counter for generating unique entry IDs
    next_id: u64,
}

impl Default for ProtocolLogState {
    fn default() -> Self {
        Self {
            entries: VecDeque::with_capacity(MAX_LOG_ENTRIES),
            selected_index: None,
            scroll_offset: 0,
            filter: ProtocolLogFilter::default(),
            is_paused: false,
            pending_requests: HashMap::new(),
            next_id: 1,
        }
    }
}

impl ProtocolLogState {
    /// Add a new request log entry
    pub fn add_request(
        &mut self,
        api_type: ApiType,
        api_version: i16,
        correlation_id: i32,
        broker: String,
        bytes: usize,
        summary: String,
    ) {
        let id = self.next_id;
        self.next_id += 1;

        // Store pending request info
        self.pending_requests.insert(
            correlation_id,
            (SystemTime::now(), api_type, api_version, broker.clone()),
        );

        let entry = ProtocolLogEntry::new_request(
            id,
            api_type,
            api_version,
            correlation_id,
            broker,
            bytes,
            summary,
        );

        self.add_entry(entry);
    }

    /// Add a response log entry, matching with pending request
    pub fn add_response(
        &mut self,
        correlation_id: i32,
        bytes: usize,
        error_code: Option<i16>,
        summary: String,
    ) {
        let id = self.next_id;
        self.next_id += 1;

        // Look up the pending request
        if let Some((request_time, api_type, api_version, broker)) = self.pending_requests.remove(&correlation_id) {
            let duration = request_time.elapsed().unwrap_or_default();

            let entry = ProtocolLogEntry::new_response(
                id,
                api_type,
                api_version,
                correlation_id,
                broker,
                bytes,
                duration,
                error_code,
                summary,
            );

            self.add_entry(entry);
        }
    }

    /// Add an entry to the log, maintaining max size
    fn add_entry(&mut self, entry: ProtocolLogEntry) {
        // Remove oldest entry if at capacity
        if self.entries.len() >= MAX_LOG_ENTRIES {
            self.entries.pop_front();
        }

        self.entries.push_back(entry);

        // Auto-scroll to bottom if not paused
        if !self.is_paused {
            self.scroll_to_bottom();
        }
    }

    /// Get filtered entries
    pub fn filtered_entries(&self) -> Vec<&ProtocolLogEntry> {
        self.entries
            .iter()
            .filter(|e| self.filter.matches(e))
            .collect()
    }

    /// Get the total number of entries (unfiltered)
    pub fn total_count(&self) -> usize {
        self.entries.len()
    }

    /// Get the number of filtered entries
    pub fn filtered_count(&self) -> usize {
        self.entries.iter().filter(|e| self.filter.matches(e)).count()
    }

    /// Clear all log entries
    pub fn clear(&mut self) {
        self.entries.clear();
        self.selected_index = None;
        self.scroll_offset = 0;
        self.pending_requests.clear();
    }

    /// Toggle pause state
    pub fn toggle_pause(&mut self) {
        self.is_paused = !self.is_paused;
    }

    /// Cycle to the next filter
    pub fn cycle_filter(&mut self) {
        self.filter = self.filter.next();
        self.scroll_offset = 0;
    }

    /// Scroll up
    pub fn scroll_up(&mut self) {
        if self.scroll_offset > 0 {
            self.scroll_offset = self.scroll_offset.saturating_sub(1);
        }
    }

    /// Scroll down
    pub fn scroll_down(&mut self, visible_height: usize) {
        let filtered_count = self.filtered_count();
        if filtered_count > visible_height {
            let max_offset = filtered_count.saturating_sub(visible_height);
            if self.scroll_offset < max_offset {
                self.scroll_offset += 1;
            }
        }
    }

    /// Scroll to top
    pub fn scroll_to_top(&mut self) {
        self.scroll_offset = 0;
    }

    /// Scroll to bottom
    pub fn scroll_to_bottom(&mut self) {
        // Will be adjusted when rendering based on visible height
        self.scroll_offset = usize::MAX;
    }

    /// Select next entry
    pub fn select_next(&mut self) {
        let count = self.filtered_count();
        if count == 0 {
            self.selected_index = None;
            return;
        }

        self.selected_index = match self.selected_index {
            None => Some(0),
            Some(i) if i < count - 1 => Some(i + 1),
            Some(i) => Some(i),
        };
    }

    /// Select previous entry
    pub fn select_previous(&mut self) {
        let count = self.filtered_count();
        if count == 0 {
            self.selected_index = None;
            return;
        }

        self.selected_index = match self.selected_index {
            None => Some(count.saturating_sub(1)),
            Some(0) => Some(0),
            Some(i) => Some(i - 1),
        };
    }
}
