use std::{
    io::{self, Result},
    sync::Arc,
    time::Duration,
};

use protocol::fetch::{FetchPartition, FetchTopic};
use ratatui::{
    Terminal,
    crossterm::{
        event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
        execute,
        terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
    },
    prelude::{Backend, CrosstermBackend},
};
use tokio::sync::RwLock;

use crate::{app::App, client_builder::ClientBuilder, ui::main_screen::render};

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
    let mut client = ClientBuilder::new()
        .brokers(vec!["localhost:9092"])
        .build()
        .await
        .unwrap();

    let topics = vec![FetchTopic {
        topic: String::from("EmploymentHero.User"),
        partitions: vec![FetchPartition {
            partition: 0,
            fetch_offset: 0,
            partition_max_bytes: 1_048_576,
        }],
    }];

    let response = client.fetch_messages(topics).await;

    match response {
        Ok(r) => {
            println!("Response of fetch:");
            println!("throttle_time_ms: {}", r.throttle_time_ms);
            for topic_r in r.responses {
                println!("Topic: {}", topic_r.topic);

                for p in topic_r.paritions {
                    println!("----- Paritions:");
                    println!("----- Parition index: {}", p.partition_index);
                    println!("----- Parition error code: {}", p.error_code);
                    println!("----- Parition high watermark: {}", p.high_watermark);
                    println!("----- RECORDS: {:?}", p.records);
                }
            }
        }
        Err(err) => {
            println!("error {}", err)
        }
    }

    // let kafka_client = Arc::new(RwLock::new(client));

    // enable_raw_mode()?;
    // let mut stderr = io::stderr(); // This is a special case. Normally using stdout is fine
    // execute!(stderr, EnterAlternateScreen, EnableMouseCapture)?;

    // let backend = CrosstermBackend::new(stderr);
    // let mut terminal = Terminal::new(backend)?;

    // let mut app = App::new(kafka_client);
    // app.load_topics();
    // app.start_stats_polling();

    // let _ = run_app(&mut terminal, &mut app);

    // disable_raw_mode()?;
    // execute!(
    //     terminal.backend_mut(),
    //     LeaveAlternateScreen,
    //     DisableMouseCapture
    // )?;
    // terminal.show_cursor()?;

    Ok(())
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
                _ => {}
            }
        }
    }
}
