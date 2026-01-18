pub mod group_detail_panel;
pub mod panel_type;
pub mod records_panel;

pub use group_detail_panel::GroupDetailPanel;
pub use panel_type::MainPanelType;
pub use records_panel::RecordsPanel;

use crate::ui::screen_type::ScreenType;

/// Result returned by panel key handlers
pub enum PanelResult {
    /// Key was handled, no further action needed
    Handled,
    /// Return focus to sidebar
    SwitchFocus(FocusTarget),
    /// Push a modal screen
    ShowModal(ScreenType),
}

/// Target for switching focus from panel back to sidebar
#[derive(Clone, Copy, Debug)]
pub enum FocusTarget {
    Topics,
    Groups,
}
