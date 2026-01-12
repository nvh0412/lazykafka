use crate::app::{Screen, TopicInfoState, TopicListState};
use crate::{
    app::{App, InputMode}
    ,
    models::{broker_stats::ConnectionStatus, topic::Topic},
};
use ratatui::layout::Margin;
use ratatui::widgets::{Clear, Scrollbar, ScrollbarOrientation};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table},
    Frame,
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

    render_topics_list(
        frame,
        body[0],
        &mut app.state.filtered_topic_state,
        &app.state.topic_search_query,
        &app.input_mode,
    );
    render_messages_panel(frame, body[1], app);

    let topic_state = &mut app.state.topic_info_state;

    if let Screen::TopicDetail(topic_name) = &mut app.screen {
        let topic = app
            .state
            .topics
            .items
            .iter()
            .find(|t| &t.name == topic_name);

        if let Some(topic) = topic {
            render_topic_popup(frame, topic, topic_state);
        }
    }
}

fn render_topic_popup(frame: &mut Frame, topic: &Topic, topic_info_state: &mut TopicInfoState) {
    let popup_block = Block::default().borders(Borders::NONE);

    let area = centered_rect(60, 60, frame.area());
    frame.render_widget(Clear, area);
    frame.render_widget(popup_block, area);

    // Create inner area for the content
    let inner_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0)])
        .split(area);

    let topic_name = topic.name.clone();

    // Find the topic in the app state

    let content = if let Some(config) = &topic.config {
        // Sort config entries by key name for easier reading
        let mut sorted_configs: Vec<_> = config.configs.iter().collect();
        sorted_configs.sort_by(|a, b| a.0.cmp(b.0));

        // Create table rows from sorted config
        let rows: Vec<Row> = sorted_configs
            .iter()
            .map(|(key, value)| {
                let value_str = value.value.as_deref().unwrap_or("<null>");

                Row::new(vec![Cell::from(key.as_str()), Cell::from(value_str)])
            })
            .collect();

        topic_info_state.scroll_state = topic_info_state.scroll_state.position(10);

        Table::new(
            rows,
            [Constraint::Percentage(50), Constraint::Percentage(50)],
        )
        .header(
            Row::new(vec![
                Cell::from("Config Key").style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from("Value").style(Style::default().add_modifier(Modifier::BOLD)),
            ])
            .style(Style::default().fg(Color::Yellow))
            .bottom_margin(1),
        )
        .block(
            Block::default()
                .title(format!(
                    "Topic Configuration: {} (Press Esc to close)",
                    topic_name
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .style(Style::default().fg(Color::White))
    } else {
        // Config is loading
        Table::new(
            vec![Row::new(vec![Cell::from("Loading configuration...")])],
            [Constraint::Percentage(100)],
        )
        .block(
            Block::default()
                .title(format!(
                    "Topic Configuration: {} (Press Esc to close)",
                    topic_name
                ))
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        )
    };

    frame.render_stateful_widget(content, area, &mut topic_info_state.table_state);
    frame.render_stateful_widget(
        Scrollbar::default()
            .orientation(ScrollbarOrientation::VerticalRight)
            .begin_symbol(None)
            .end_symbol(None),
        area.inner(Margin {
            vertical: 1,
            horizontal: 1,
        }),
        &mut topic_info_state.scroll_state,
    );

}

fn render_topics_list(
    frame: &mut Frame,
    area: Rect,
    topic_state: &mut TopicListState,
    search_query: &str,
    input_mode: &InputMode,
) {
    // Split the area into search box (3 lines) and topic list
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Search box
            Constraint::Min(0),    // Topic list
        ])
        .split(area);

    // Render search box
    let search_text = if search_query.is_empty() {
        "Type / to search topics...".to_string()
    } else {
        search_query.to_string()
    };

    let search_style = match input_mode {
        InputMode::SearchTopic => Style::default().fg(Color::Yellow),
        InputMode::Normal => Style::default().fg(Color::Gray),
        _ => Style::default().fg(Color::Gray),
    };

    let search_box = Paragraph::new(search_text)
        .block(
            Block::default()
                .title("Search")
                .borders(Borders::ALL)
                .border_style(search_style),
        )
        .style(Style::default().fg(Color::White));

    frame.render_widget(search_box, chunks[0]);

    // Filter topics based on search query
    let filtered_items: Vec<ListItem> = topic_state
        .topics
        .items
        .iter()
        .enumerate()
        .map(|(_, topic)| {
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

    let list = List::new(filtered_items)
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
        .highlight_symbol("► ")
        .scroll_padding(5);

    topic_state.vertical_scroll_state = topic_state
        .vertical_scroll_state
        .content_length(topic_state.topics.items.len());

    frame.render_stateful_widget(list, chunks[1], &mut topic_state.topics.state);
    frame.render_stateful_widget(
        Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓")),
        chunks[1],
        &mut topic_state.vertical_scroll_state,
    );
}

fn render_messages_panel(frame: &mut Frame, area: Rect, app: &App) {
    let items: Vec<ListItem> = app
        .state
        .records
        .items
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

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    // Cut the given rectangle into three vertical pieces
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    // Then cut the middle vertical piece into three width-wise pieces
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1] // Return the middle chunk
}
