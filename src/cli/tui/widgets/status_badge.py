"""Status badge helpers for Kafka connector states and ingestion task states."""


def status_markup(state: str | None) -> str:
    """Return Rich markup string for a connector state."""
    match (state or "").upper():
        case "RUNNING":
            return "[bold green]● RUNNING[/]"
        case "PAUSED":
            return "[bold yellow]○ PAUSED[/]"
        case "FAILED":
            return "[bold red]✗ FAILED[/]"
        case "UNASSIGNED":
            return "[dim]◌ UNASSIGNED[/]"
        case _:
            return "[dim]? UNKNOWN[/]"


def task_status_markup(status: str | None) -> str:
    """Return Rich markup string for an IngestionTaskResponse.status value.

    Deliberately separate from status_markup: ingestion task status
    ('running'/'success'/'failure') is a different vocabulary from Kafka
    Connect's connector states ('RUNNING'/'PAUSED'/'FAILED'/'UNASSIGNED') —
    'success' and 'failure' don't match any of that function's cases and
    would silently render as "? UNKNOWN" if reused as-is.
    """
    match (status or "").lower():
        case "running":
            return "[bold green]● running[/]"
        case "success":
            return "[bold cyan]✓ success[/]"
        case "failure":
            return "[bold red]✗ failure[/]"
        case _:
            return "[dim]? unknown[/]"
