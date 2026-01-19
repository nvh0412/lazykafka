# LazyKafka

Dealing with Kafka topics and groups can be a real mission using just the standard scripts. I looked at the web tools available and thought, 'Yeah, nah—too much effort.'

If you're like me and can't be bothered setting up a local web UI just to check a record, here is LazyKafka. It’s the terminal app that does the hard work so you don't have to.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Rust](https://img.shields.io/badge/rust-2024-orange.svg)

## Features

<img width="1624" height="1060" alt="Demo" src="https://github.com/user-attachments/assets/e4a64377-e867-4cb6-ac36-668eb7bc734b" />

### Topic Management

Browse, search, and manage Kafka topics with ease.

![topic-management](https://github.com/user-attachments/assets/15cf1a31-6f31-4e43-a1cf-7cd2a655650c)

<!-- Demo GIF here -->

- Browse all topics in your cluster
- Real-time search filtering
- View topic details (partitions, replication factor, configs)
- Delete topics with confirmation
- Add partitions to scale your topics

### Message Browser

Explore records with powerful filtering and pagination.

![message-management](https://github.com/user-attachments/assets/6fefd519-b874-4e4b-8761-67d4a05e0e68)

- Paginated loading (configurable page size)
- Multiple sort modes: Newest First, Oldest First, Offset Asc/Desc
- Search by key, value, or headers
- Filter by partition
- View full record details (key, value, headers, timestamp, offset)

### Consumer Group Management

Monitor consumer groups and their state.

![consumer_group](https://github.com/user-attachments/assets/6ab97e6c-e793-46a3-97c9-4cb8a76152fa)

<!-- Demo GIF here -->

- List all consumer groups
- View group details: state, protocol, members
- Inspect member assignments
- Track committed offsets per topic/partition

### Protocol Logging

Debug Kafka interactions with built-in protocol logging.

<!-- Demo GIF here -->
<img width="774" height="155" alt="Screenshot 2026-01-19 at 11 29 18 pm" src="https://github.com/user-attachments/assets/df1a433b-af62-4880-b8b0-015f9f6f3bbf" />

- Live logging of API requests/responses
- Correlation ID tracking
- Response time monitoring
- Error code visibility

### Cluster Monitoring

Keep an eye on your connection and cluster stats.

<!-- Demo GIF here -->
<img width="273" height="51" alt="Screenshot 2026-01-19 at 11 32 21 pm" src="https://github.com/user-attachments/assets/10f4877b-0c06-409d-9566-d61272274fc3" />

- Real-time connection status
- Bytes sent/received statistics
- Multi-broker support

## Installation

### From releases

Download the executable file compatible with your OS from the releases page here.

https://github.com/nvh0412/lazykafka/releases

#### Linux / macOS

```
# Download the binary (replace with your platform)
# Make it executable
chmod +x lazykafka-*
# Move to a directory in your PATH
sudo mv lazykafka-* /usr/local/bin/lazykafka
```

#### Windows
Download the .exe file and add it to your PATH.

### From Source

```bash
git clone https://github.com/nvh0412/lazykafka
cd lazykafka
cargo build --release
```

### Homebrew (coming soon)

```bash
brew install lazykafka
```

## Usage

```bash
# Connect to local Kafka
lazykafka

# Connect to specific broker(s)
lazykafka -b kafka1:9092 -b kafka2:9092

# With custom client ID and timeout
lazykafka -b localhost:9092 --client-id my-client --timeout 30
```

## Keyboard Shortcuts

### Global

| Key | Action |
|-----|--------|
| `q` | Quit |
| `Tab` | Switch between Topics/Consumer Groups |
| `l` | Focus logs panel |
| `/` | Search |
| `Esc` | Cancel/Back |

### Topics

| Key | Action |
|-----|--------|
| `Enter` | View records |
| `i` | View topic details |
| `d` | Delete topic |
| `p` | Add partitions |

### Records

| Key | Action |
|-----|--------|
| `j/k` or `↑/↓` | Navigate |
| `s` | Cycle sort mode |
| `f` | Filter by partition |
| `n/p` | Next/Previous page |
| `i` | View record details |
| `r` | Refresh |

### Logs

| Key | Action |
|-----|--------|
| `p` | Pause/Resume |
| `c` | Clear logs |
| `f` | Filter logs |
| `g/G` | Jump to top/bottom |

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `-b, --broker` | `localhost:9092` | Kafka broker address (repeatable) |
| `--client-id` | `lazykafka-client` | Client identifier |
| `--timeout` | `10` | Connection timeout in seconds |

## Roadmap

- [ ] Topic creation UI
- [ ] Producer UI for sending test messages
- [ ] Avro/Protobuf schema support
- [ ] Multi-cluster management
- [ ] Record export (JSON/CSV)
- [ ] Real-time tail mode

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT
