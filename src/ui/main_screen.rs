use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

use crate::{app::App, components::stateful_list::StatefulList, models::topic::Topic};

pub fn render(frame: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Header
            Constraint::Min(0),    // Body
            Constraint::Length(1), // Footer
        ])
        .split(frame.area());

    render_header(frame, chunks[0], app);
    render_body(frame, chunks[1], app);
    render_footer(frame, chunks[2], app);
}

fn render_header(frame: &mut Frame, area: Rect, app: &App) {
    let header_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(20), // Broker name
            Constraint::Percentage(30), // Breadcrumb
            Constraint::Percentage(50), // Stats
        ])
        .split(area);

    // Render header sections
    let broker = Paragraph::new("Broker: localhost:9092");
    frame.render_widget(broker, header_layout[0]);

    let breadcrumb = Paragraph::new("Home > Topics > my-topic");
    frame.render_widget(breadcrumb, header_layout[1]);

    let stats = Paragraph::new("Connected | ↓ 1.2MB/s | ↑ 450KB/s");
    frame.render_widget(stats, header_layout[2]);
}

fn render_body(frame: &mut Frame, area: Rect, app: &mut App) {
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(25), // Topics sidebar
            Constraint::Percentage(75), // Messages + filter
        ])
        .split(area);

    render_topics_list(frame, body[0], &mut app.state.topics);
    render_messages_panel(frame, body[1], app);
}

fn render_topics_list(frame: &mut Frame, area: Rect, topics: &mut StatefulList<Topic>) {
    let items: Vec<ListItem> = topics
        .items
        .iter()
        .map(|topic| {
            let line = Line::from(vec![
                Span::raw(&topic.name),
                Span::styled(
                    format!(" [{}p]", topic.partitions),
                    Style::default().fg(Color::DarkGray),
                ),
            ]);
            ListItem::new(line)
        })
        .collect();

    let list = List::new(items)
        .block(
            Block::default()
                .title("Topics")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Gray)),
        )
        .highlight_style(
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("► ");

    frame.render_stateful_widget(list, area, &mut topics.state);
}

fn render_messages_panel(frame: &mut Frame, area: Rect, app: &App) {
    let panel = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(0), // Message list
        ])
        .split(area);

    let broker = Paragraph::new("Message list").block(Block::default().borders(Borders::ALL));
    frame.render_widget(broker, panel[0]);
}

fn render_footer(frame: &mut Frame, area: Rect, app: &App) {}
