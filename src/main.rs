use clap::Parser;
use ratatui::crossterm::event::poll;
use ratatui::{
    Terminal,
    crossterm::{
        event::{self, DisableMouseCapture, EnableMouseCapture, Event},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    prelude::{Backend, CrosstermBackend},
};
use std::{
    io::{self, Result},
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;

use crate::{app::App, cli::Args, client_builder::ClientBuilder};

mod app;
mod cli;
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
    let args = Args::parse();

    let client = ClientBuilder::new()
        .brokers(args.get_brokers())
        .client_id(&args.client_id)
        .connection_timeout(args.connection_timeout())
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
        if poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
        {
            let mut result = app.screen_manager.handle_key(key.code, &mut app.state);
            if !app.handle_event_result(&mut result) {
                return Ok(false);
            }
        }
    }
}
