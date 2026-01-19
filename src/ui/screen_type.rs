use crate::protocol::record::Record;
use crate::ui::screen::Screen;
use crate::ui::screens::confirm_delete_topic_dialog::ConfirmDeleteTopicDialogModal;
use crate::ui::screens::confirm_dialog_modal::ConfirmDialogModal;
use crate::ui::screens::consumer_group_detail_modal::ConsumerGroupDetailModal;
use crate::ui::screens::create_partitions_modal::CreatePartitionsModal;
use crate::ui::screens::main_screen::MainScreen;
use crate::ui::screens::record_detail_modal::RecordDetailModal;
use crate::ui::screens::topic_detail_modal::TopicDetailModal;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ScreenType {
    Main,
    MessageDetail(Record),
    TopicDetail(String),
    ConfirmDialog {
        title: String,
        message: String,
    },
    ConfirmDeleteTopicDialog {
        topic_name: String,
    },
    ScreenInput {
        placeholder: String,
    },
    // Consumer group screens
    ConsumerGroups,
    ConsumerGroupDetail(String),
    // Topic partition screen
    CreatePartitionsDialog {
        topic_name: String,
        current_partitions: i32,
    },
}

pub fn create_screen(screen_type: ScreenType) -> Box<dyn Screen> {
    match screen_type {
        ScreenType::Main => Box::new(MainScreen::new()),
        ScreenType::TopicDetail(t) => Box::new(TopicDetailModal::new(t)),
        ScreenType::ConfirmDialog { title, message } => {
            Box::new(ConfirmDialogModal::new(title, message))
        }
        ScreenType::ConfirmDeleteTopicDialog { topic_name } => {
            Box::new(ConfirmDeleteTopicDialogModal::new(topic_name))
        }
        ScreenType::MessageDetail(t) => Box::new(RecordDetailModal::new(t)),
        ScreenType::ConsumerGroupDetail(group_id) => {
            Box::new(ConsumerGroupDetailModal::new(group_id))
        }
        ScreenType::CreatePartitionsDialog {
            topic_name,
            current_partitions,
        } => Box::new(CreatePartitionsModal::new(topic_name, current_partitions)),
        _ => unimplemented!(),
    }
}
