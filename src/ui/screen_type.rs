use crate::ui::screen::Screen;
use crate::ui::screen_type::ScreenType::ConfirmDialog;
use crate::ui::screens::confirm_dialog_modal::ConfirmDialogModal;
use crate::ui::screens::main_screen::MainScreen;
use crate::ui::screens::topic_detail_modal::TopicDetailModal;

#[derive(Debug, Clone)]
pub enum ScreenType {
    Main,
    MessageDetail(String),
    TopicDetail(String),
    ConfirmDialog { title: String, message: String },
    ScreenInput { placeholder: String },
}

pub fn create_screen(screen_type: ScreenType) -> Box<dyn Screen> {
    match screen_type {
        ScreenType::Main => Box::new(MainScreen::new()),
        ScreenType::TopicDetail(t) => Box::new(TopicDetailModal::new(t)),
        ScreenType::ConfirmDialog { title, message } => Box::new(ConfirmDialogModal::new(title, message)),
        _ => unimplemented!()
    }
}
