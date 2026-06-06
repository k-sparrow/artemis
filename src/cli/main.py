"""Artemis CLI entrypoint.

Usage:
    artemis tui                       # launch TUI (default)
    artemis sources list [--json]
    artemis sources get <id> [--json]
    artemis sources create ...
    artemis sources pause <id>
    artemis sources resume <id>
    artemis sources restart <id>
    artemis sources delete <id> [--yes]
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid

import click

from src.cli.client import DataSourcesClient
from src.backend.enterprise.data_sources.api.sources.schemas import DataSourceCreate


def _run(coro):
    return asyncio.run(coro)


def _print_source(source, as_json: bool) -> None:
    if as_json:
        click.echo(source.model_dump_json(indent=2))
    else:
        state = (source.kafka_status.state if source.kafka_status else "UNKNOWN")
        ns = source.namespace_name or str(source.namespace_id)[:8]
        click.echo(
            f"{str(source.id)[:8]}  {source.display_name:<20}  {source.source_type:<12}"
            f"  {state:<10}  {ns}"
        )


@click.group()
@click.option(
    "--gateway",
    envvar="GATEWAY_URL",
    default=None,
    metavar="URL",
    help="Artemis gateway base URL (overrides GATEWAY_URL env var).",
)
@click.pass_context
def cli(ctx: click.Context, gateway: str | None) -> None:
    """Artemis operator CLI."""
    ctx.ensure_object(dict)
    ctx.obj["gateway"] = gateway


@cli.command()
@click.pass_context
def tui(ctx: click.Context) -> None:
    """Launch the interactive TUI."""
    from src.cli.tui.app import DataSourcesApp
    gateway = ctx.obj.get("gateway")
    DataSourcesApp(gateway_url=gateway).run()


@cli.group()
def sources():
    """Manage data sources."""


def _client(ctx: click.Context) -> DataSourcesClient:
    return DataSourcesClient(base_url=ctx.obj.get("gateway"))


@sources.command("list")
@click.option("--json", "as_json", is_flag=True)
@click.pass_context
def sources_list(ctx: click.Context, as_json: bool):
    """List all data sources."""
    items = _run(_client(ctx).list_sources())
    if as_json:
        click.echo(json.dumps([s.model_dump(mode="json") for s in items], indent=2, default=str))
    else:
        for s in items:
            _print_source(s, as_json=False)


@sources.command("get")
@click.argument("source_id")
@click.option("--json", "as_json", is_flag=True)
@click.pass_context
def sources_get(ctx: click.Context, source_id: str, as_json: bool):
    """Get a single data source."""
    source = _run(_client(ctx).get_source(uuid.UUID(source_id)))
    _print_source(source, as_json)


@sources.command("create")
@click.option("--name", required=True)
@click.option("--namespace", required=True)
@click.option("--org", required=True, help="Organisation name (owner_id seed).")
@click.option("--path", required=True)
@click.option("--recursive/--no-recursive", default=True)
@click.pass_context
def sources_create(ctx: click.Context, name: str, namespace: str, org: str, path: str, recursive: bool):
    """Create a filesystem data source."""
    payload = DataSourceCreate(
        display_name=name,
        namespace=namespace,
        org_name=org,
        path=path,
        recursive=recursive,
    )
    source = _run(_client(ctx).create_source(payload))
    click.echo(f"Created: {source.id}")


@sources.command("pause")
@click.argument("source_id")
@click.pass_context
def sources_pause(ctx: click.Context, source_id: str):
    """Pause a connector."""
    source = _run(_client(ctx).pause_source(uuid.UUID(source_id)))
    state = source.kafka_status.state if source.kafka_status else "UNKNOWN"
    click.echo(f"Paused {source.display_name}: {state}")


@sources.command("resume")
@click.argument("source_id")
@click.pass_context
def sources_resume(ctx: click.Context, source_id: str):
    """Resume a connector."""
    source = _run(_client(ctx).resume_source(uuid.UUID(source_id)))
    state = source.kafka_status.state if source.kafka_status else "UNKNOWN"
    click.echo(f"Resumed {source.display_name}: {state}")


@sources.command("restart")
@click.argument("source_id")
@click.pass_context
def sources_restart(ctx: click.Context, source_id: str):
    """Restart a connector and all its tasks."""
    source = _run(_client(ctx).restart_source(uuid.UUID(source_id)))
    state = source.kafka_status.state if source.kafka_status else "UNKNOWN"
    click.echo(f"Restarted {source.display_name}: {state}")


@sources.command("delete")
@click.argument("source_id")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def sources_delete(ctx: click.Context, source_id: str, yes: bool):
    """Delete a data source and its connector."""
    if not yes:
        click.confirm(f"Delete data source {source_id}?", abort=True)
    _run(_client(ctx).delete_source(uuid.UUID(source_id)))
    click.echo(f"Deleted {source_id}")


def main():
    cli()


if __name__ == "__main__":
    main()
