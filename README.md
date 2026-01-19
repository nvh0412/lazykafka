# LazyKafka

A fast, keyboard-driven terminal UI for Apache Kafka. Browse topics, view messages, manage consumer groups, and monitor your cluster — all without leaving the terminal.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Rust](https://img.shields.io/badge/rust-2024-orange.svg)

## Features

### Topic Management

Browse, search, and manage Kafka topics with ease.

<!-- Demo GIF here -->

- Browse all topics in your cluster
- Real-time search filtering
- View topic details (partitions, replication factor, configs)
- Delete topics with confirmation
- Add partitions to scale your topics

### Message Browser

Explore records with powerful filtering and pagination.

<!-- Demo GIF here -->

- Paginated loading (configurable page size)
- Multiple sort modes: Newest First, Oldest First, Offset Asc/Desc
- Search by key, value, or headers
- Filter by partition
- View full record details (key, value, headers, timestamp, offset)

### Consumer Group Management

Monitor consumer groups and their state.

<!-- Demo GIF here -->

- List all consumer groups
- View group details: state, protocol, members
- Inspect member assignments
- Track committed offsets per topic/partition

### Protocol Logging

Debug Kafka interactions with built-in protocol logging.

<!-- Demo GIF here -->

- Live logging of API requests/responses
- Correlation ID tracking
- Response time monitoring
- Error code visibility

### Cluster Monitoring

Keep an eye on your connection and cluster stats.

<!-- Demo GIF here -->

- Real-time connection status
- Bytes sent/received statistics
- Multi-broker support

## Installation

### From Source

```bash
git clone https://github.com/yourusername/lazykafka
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
