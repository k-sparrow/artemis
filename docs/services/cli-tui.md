# CLI / TUI

**Entry point:** `artemis` CLI  
**Framework:** Textual (TUI) + Click (CLI)  
**Source:** `src/cli/`

The Artemis CLI is a terminal-based operator tool for managing data sources and inspecting
the document registry. The primary interface is a full-screen TUI; command-line subcommands
are also available for scripting.

---

## CLI Commands

### Top-level

```
artemis [--gateway-url URL]   # Overrides ARTEMIS_GATEWAY_URL
  tui                         # Launch the TUI (default if no subcommand)
  sources <subcommand>
```

### `artemis sources`

| Command | Description |
|---------|-------------|
| `artemis sources list [--json]` | List data sources; `--json` outputs raw JSON |
| `artemis sources get <id>` | Show a single data source |
| `artemis sources create --name --type --path [--recursive]` | Register a new filesystem data source |
| `artemis sources pause <id>` | Pause the Kafka connector |
| `artemis sources resume <id>` | Resume the Kafka connector |
| `artemis sources restart <id>` | Restart the Kafka connector |
| `artemis sources delete <id>` | Delete the data source and dispatch group delete |

---

## TUI Screens

Launched via `artemis tui` or bare `artemis`.

```
┌─────────────────────────────────────────────────────────────────────┐
│  Artemis                                          [H] [C] [O] [Q]   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  [H] HomeScreen     — system metrics + recent activity              │
│  [C] ConnectorsScreen — live DataTable of data sources + status     │
│  [O] ObjectsScreen  — namespace tree + paginated object list        │
│  [Q] Quit                                                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### HomeScreen

- System summary: namespace count, indexed object count, active connector count
- Recent ingestion tasks with status indicators

### ConnectorsScreen

- Live-polling DataTable (polls every N seconds via Textual `set_interval`)
- Columns: name, type, namespace, status, created_at
- Actions: pause, resume, restart, delete (via inline keybindings)
- `N` key: navigate to CreateScreen to add a new data source

### ObjectsScreen

- Left panel: namespace tree (grouped by owner/connector)
- Right panel: object list for selected namespace/group (paginated)
- Shows: source filename, content type, size, indexed_at

### CreateScreen

- Form for creating a new filesystem data source
- Fields: name, type (currently only `filesystem`), watch_path, recursive checkbox
- Submit calls `artemis sources create` logic
- Validation error messages inline

### ConfirmScreen

- Modal overlay for destructive actions (delete, restart)
- Keybindings: `Y` to confirm, `N` / `Esc` to cancel

---

## Configuration

The CLI reads from environment or `~/.config/artemis/config.toml`:

```toml
[gateway]
url = "http://localhost:9080"
```

Or via env var:
```
ARTEMIS_GATEWAY_URL=http://localhost:9080
```

When the `gateway` compose profile is running, the TUI connects to port 9080. Without the
gateway, connect directly:
```
ARTEMIS_GATEWAY_URL=http://localhost:9500   # data-sources API
```

---

## Auth (Deferred)

`artemis login` and `artemis logout` commands are scaffolded but not yet implemented.
They will initiate a PKCE OAuth 2.0 flow against the Hydra authorization server,
store the access token in `~/.config/artemis/tokens.json`, and refresh it automatically.

This is blocked on the Hydra auth epic. Until auth is wired:
- All requests go to the gateway without an auth token
- The gateway does not yet validate tokens (deferred to the same epic)

---

## Running the TUI

```bash
# From the project root (with venv active)
artemis

# Or if the CLI package is not installed
python -m src.cli.main

# Against a non-default gateway
ARTEMIS_GATEWAY_URL=http://localhost:9500 artemis sources list
```
