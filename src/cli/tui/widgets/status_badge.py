"""Status badge helper for Kafka connector states."""


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
