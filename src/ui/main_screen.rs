use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

use crate::{
    app::App,
    components::stateful_list::StatefulList,
    models::{broker_stats::ConnectionStatus, topic::Topic},
};

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
    let stats = &app.state.broker_stats;
    let breadcrumb = &app.state.breadcrumb;
    let header = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(25),
            Constraint::Min(20),
            Constraint::Length(40),
        ])
        .split(area);

    // Broker
    let broker = Paragraph::new(format!("Broker: {}", stats.broker));
    frame.render_widget(broker, header[0]);

    // Breadcrumb
    let breadcrumb_text = breadcrumb.join(" > ");
    let breadcrumb_widget = Paragraph::new(breadcrumb_text);
    frame.render_widget(breadcrumb_widget, header[1]);

    // Stats
    let status_icon = match stats.status {
        ConnectionStatus::Connected => "✓",
        ConnectionStatus::Connecting => "⋯",
        ConnectionStatus::Disconnected => "✗",
        ConnectionStatus::Failed(_) => "⚠",
    };

    let stats_text = format!(
        "{} Connected | ↓ {} | ↑ {}",
        status_icon,
        format_bytes(stats.bytes_received as u64),
        format_bytes(stats.bytes_sent as u64)
    );
    let stats_widget = Paragraph::new(stats_text);
    frame.render_widget(stats_widget, header[2]);
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.2}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
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
    let items: Vec<ListItem> = app
        .state
        .records
        .iter()
        .map(|r| ListItem::new(format!("{}", String::from_utf8(r.value.clone()).unwrap())))
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title("Records")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Gray)),
    );
    frame.render_widget(list, area);
}

fn render_footer(frame: &mut Frame, area: Rect, app: &App) {}
