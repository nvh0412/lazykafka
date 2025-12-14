use std::{
    io::{self, Result},
    sync::Arc,
    time::Duration,
};

use kafkatui_rs::protocol::list_offset::ListOffsetRequest;
use protocol::fetch::{FetchPartition, FetchTopic};
use protocol::offset_fetch::OffsetFetchTopic;
use ratatui::{
    Terminal,
    crossterm::{
        event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    prelude::{Backend, CrosstermBackend},
};
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use crate::{
    app::App,
    client_builder::ClientBuilder,
    protocol::list_offset::{ListOffsetPartition, ListOffsetTopic},
    ui::main_screen::render,
};

mod app;
mod client;
mod client_builder;
mod connection;
mod error;
mod protocol;

mod components;
mod models;
mod repository;
mod state;
mod ui;

#[tokio::main]
async fn main() -> Result<()> {
    let client = ClientBuilder::new()
        .brokers(vec!["localhost:9092"])
        .build()
        .await
        .unwrap();

    let sharable_client = Arc::new(RwLock::new(client));

    enable_raw_mode()?;
    let mut stderr = io::stderr(); // This is a special case. Normally using stdout is fine
    execute!(stderr, EnterAlternateScreen, EnableMouseCapture)?;

    let backend = CrosstermBackend::new(stderr);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(sharable_client);
    app.load_topics();
    app.start_stats_polling();

    let _ = run_app(&mut terminal, &mut app);

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}

enum InputMode {
    Normal,
    Editing,
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> io::Result<bool> {
    loop {
        terminal.draw(|f| render(f, app))?;

        app.handle_messages();

        if let Event::Key(key) = event::read()? {
            if key.kind == event::KeyEventKind::Release {
                // Skip events that are not KeyEventKind::Press
                continue;
            }

            match key.code {
                KeyCode::Char('q') => return Ok(false),
                KeyCode::Up => app.on_up(),
                KeyCode::Down => app.on_down(),
                KeyCode::Enter => app.on_enter(),
                _ => {}
            }
        }
    }
}
