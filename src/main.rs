use ratatui::{
    crossterm::{
        event::{self, DisableMouseCapture, EnableMouseCapture, Event},
        execute,
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    },
    prelude::{Backend, CrosstermBackend},
    Terminal,
};
use std::{
    io::{self, Result},
    sync::Arc,
};
use tokio::sync::RwLock;

use crate::{app::App, client_builder::ClientBuilder};

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

fn run_app<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> io::Result<bool> {
    loop {
        terminal.draw(|f| app.screen_manager.render(f, &mut app.state))?;
        app.handle_messages();
        if let Event::Key(key) = event::read()? {
            let mut result = app.screen_manager.handle_key(key.code, &mut app.state);
            if !app.handle_event_result(&mut result) {
                return Ok(false);
            }
        }
    }
}
