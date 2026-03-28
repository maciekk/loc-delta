#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "requests",
#   "rich",
# ]
# ///
"""
Show daily line-change stats across all GitHub repos for a given user.

Usage:
    uv run loc_deltas.py [N_DAYS] [--user USERNAME] [--no-cache]

Requires GITHUB_TOKEN env var (or rate-limit is 60 req/hour).

Cache lives in ~/.cache/loc-deltas/<username>.json.
Today's data is never cached (it changes as you commit).
Use --no-cache to force a full re-fetch of all days.
"""

import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from rich.console import Console
from rich.table import Table

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")


def headers():
    h = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

REPO_CACHE_TTL_SECONDS = 3600  # re-fetch repo list at most once per hour


def cache_dir() -> Path:
    base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    return base / "loc-deltas"


def load_cache(username: str) -> dict:
    path = cache_dir() / f"{username}.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_cache(username: str, data: dict) -> None:
    path = cache_dir() / f"{username}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


def load_cached_repos(username: str) -> tuple[list[dict] | None, float | None]:
    """Return (repo list, age_seconds) if within TTL, else (None, None)."""
    path = cache_dir() / f"{username}.repos.json"
    if not path.exists():
        return None, None
    try:
        data = json.loads(path.read_text())
        age = datetime.now(timezone.utc).timestamp() - data["fetched_at"]
        if age < REPO_CACHE_TTL_SECONDS:
            return data["repos"], age
    except (json.JSONDecodeError, KeyError, OSError):
        pass
    return None, None


def save_cached_repos(username: str, repos: list[dict]) -> None:
    path = cache_dir() / f"{username}.repos.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"fetched_at": datetime.now(timezone.utc).timestamp(), "repos": repos}))


def cache_file_size(username: str) -> int:
    """Total bytes of all cache files for this user."""
    total = 0
    for name in (f"{username}.json", f"{username}.repos.json"):
        p = cache_dir() / name
        if p.exists():
            total += p.stat().st_size
    return total


# ---------------------------------------------------------------------------
# Stats tracking
# ---------------------------------------------------------------------------

@dataclass
class RunStats:
    days_from_cache: int = 0
    days_fetched: int = 0
    repos_from_cache: bool = False
    repo_cache_age_s: float | None = None
    api_calls: int = 0
    commits_processed: int = 0
    commits_skipped: int = 0


# ---------------------------------------------------------------------------
# GitHub API
# ---------------------------------------------------------------------------

def get_repos(username: str, stats: RunStats) -> list[dict]:
    repos, page = [], 1
    while True:
        r = requests.get(
            f"https://api.github.com/users/{username}/repos",
            headers=headers(),
            params={"per_page": 100, "page": page, "type": "owner"},
        )
        stats.api_calls += 1
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        repos.extend(batch)
        page += 1
    return repos


def get_commits(repo_full_name: str, since: datetime, until: datetime, author: str, stats: RunStats) -> list[dict]:
    commits, page = [], 1
    while True:
        r = requests.get(
            f"https://api.github.com/repos/{repo_full_name}/commits",
            headers=headers(),
            params={
                "author": author,
                "since": since.isoformat(),
                "until": until.isoformat(),
                "per_page": 100,
                "page": page,
            },
        )
        stats.api_calls += 1
        if r.status_code == 409:  # empty repo
            break
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        commits.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return commits


def get_commit_detail(repo_full_name: str, sha: str, stats: RunStats) -> dict:
    r = requests.get(
        f"https://api.github.com/repos/{repo_full_name}/commits/{sha}",
        headers=headers(),
    )
    stats.api_calls += 1
    r.raise_for_status()
    return r.json()


def compute_file_stats(detail: dict) -> tuple[int, int, int]:
    """
    Per file: changed = min(additions, deletions) — lines modified in place.
    added   = additions - changed   (truly new lines)
    deleted = deletions - changed   (truly removed lines)
    """
    added = changed = deleted = 0
    for f in detail.get("files", []):
        a = f.get("additions", 0)
        d = f.get("deletions", 0)
        c = min(a, d)
        added += a - c
        changed += c
        deleted += d - c
    return added, changed, deleted


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args(argv: list[str]) -> tuple[int, str, bool]:
    n_days = 7
    username = "maciekk"
    no_cache = False
    i = 1
    while i < len(argv):
        if argv[i] == "--user" and i + 1 < len(argv):
            username = argv[i + 1]
            i += 2
        elif argv[i] == "--no-cache":
            no_cache = True
            i += 1
        else:
            try:
                n_days = int(argv[i])
            except ValueError:
                print(f"Unknown argument: {argv[i]}", file=sys.stderr)
                sys.exit(1)
            i += 1
    return n_days, username, no_cache


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    n_days, username, no_cache = parse_args(sys.argv)

    now = datetime.now(timezone.utc)
    today = now.date()

    console = Console()
    stats = RunStats()

    if not GITHUB_TOKEN:
        console.print(
            "[yellow]Warning:[/yellow] GITHUB_TOKEN not set — unauthenticated rate limit is 60 req/hour."
        )

    # Load cache (ignore if --no-cache)
    cache: dict[str, list[int]] = {} if no_cache else load_cache(username)
    if no_cache:
        console.print("[dim]Cache disabled — fetching all data fresh (including repo list).[/dim]")

    # Determine which days still need fetching
    dates_needed = []
    for i in range(n_days):
        date = today - timedelta(days=i)
        if date == today or str(date) not in cache:
            dates_needed.append(date)

    stats.days_from_cache = n_days - len(dates_needed)
    stats.days_fetched = len(dates_needed)

    # date -> [added, changed, deleted]
    daily: dict = defaultdict(lambda: [0, 0, 0])

    # Seed from cache for days we already have
    for date_str, day_stats in cache.items():
        daily[date_str] = list(day_stats)

    if dates_needed:
        fetch_since = datetime.combine(min(dates_needed), datetime.min.time()).replace(tzinfo=timezone.utc)
        fetch_until = datetime.combine(max(dates_needed), datetime.max.time()).replace(tzinfo=timezone.utc)

        repos, repo_cache_age = (None, None) if no_cache else load_cached_repos(username)
        if repos is not None:
            stats.repos_from_cache = True
            stats.repo_cache_age_s = repo_cache_age
            repo_source = "[dim](repo list from cache)[/dim]"
        else:
            status_msg = f"Fetching repos for [bold]{username}[/bold]"
            if stats.days_from_cache:
                status_msg += f" ([dim]{stats.days_from_cache} day(s) from cache[/dim])"
            status_msg += "…"
            with console.status(status_msg):
                try:
                    repos = get_repos(username, stats)
                except requests.HTTPError as e:
                    console.print(f"[red]Error fetching repos:[/red] {e}")
                    sys.exit(1)
            save_cached_repos(username, repos)
            repo_source = ""

        console.print(
            f"Found [bold]{len(repos)}[/bold] repos {repo_source}. Scanning commits…"
        )

        dates_needed_set = {str(d) for d in dates_needed}

        for repo in repos:
            try:
                commits = get_commits(repo["full_name"], fetch_since, fetch_until, username, stats)
            except requests.HTTPError:
                stats.commits_skipped += 1
                continue

            for commit in commits:
                date_str = commit["commit"]["author"]["date"]
                date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).date()

                if str(date) not in dates_needed_set:
                    continue

                try:
                    detail = get_commit_detail(repo["full_name"], commit["sha"], stats)
                except requests.HTTPError:
                    stats.commits_skipped += 1
                    continue

                a, c, d = compute_file_stats(detail)
                daily[str(date)][0] += a
                daily[str(date)][1] += c
                daily[str(date)][2] += d
                stats.commits_processed += 1

        console.print(
            f"Processed [bold]{stats.commits_processed}[/bold] commit(s)"
            + (f", skipped {stats.commits_skipped} due to errors." if stats.commits_skipped else ".")
        )

        # Update cache — persist everything except today
        updated_cache = dict(cache)
        for date_str in dates_needed_set:
            if date_str != str(today):
                updated_cache[date_str] = daily[date_str]
        save_cache(username, updated_cache)

    else:
        console.print(f"All {n_days} day(s) served from cache.")

    console.print()

    # ---------------------------------------------------------------------------
    # Render table
    # ---------------------------------------------------------------------------
    table = Table(
        title=f"Daily line changes — [bold]{username}[/bold] — last {n_days} day(s)",
        show_footer=True,
    )
    table.add_column("Date", style="cyan", footer="TOTAL")
    table.add_column("Added", style="green", justify="right")
    table.add_column("Changed", style="yellow", justify="right")
    table.add_column("Deleted", style="red", justify="right")

    totals = [0, 0, 0]

    for i in range(n_days):
        date = today - timedelta(days=i)
        a, c, d = daily.get(str(date), [0, 0, 0])
        totals[0] += a
        totals[1] += c
        totals[2] += d
        suffix = " [dim](today)[/dim]" if date == today else ""
        row_style = "dim" if not (a or c or d) else ""
        table.add_row(str(date) + suffix, f"{a:,}", f"{c:,}", f"{d:,}", style=row_style)

    table.columns[1].footer = f"[green]{totals[0]:,}[/green]"
    table.columns[2].footer = f"[yellow]{totals[1]:,}[/yellow]"
    table.columns[3].footer = f"[red]{totals[2]:,}[/red]"

    console.print(table)

    # ---------------------------------------------------------------------------
    # Cache stats
    # ---------------------------------------------------------------------------
    cache_size = cache_file_size(username)

    stats_table = Table(show_header=False, box=None, padding=(0, 2))
    stats_table.add_column(style="dim")
    stats_table.add_column(justify="right")

    stats_table.add_row("Days from cache", f"[green]{stats.days_from_cache}[/green] / {n_days}")
    stats_table.add_row("Days fetched", str(stats.days_fetched))
    if stats.repos_from_cache and stats.repo_cache_age_s is not None:
        age_min = int(stats.repo_cache_age_s // 60)
        stats_table.add_row("Repo list", f"[green]cached[/green] ({age_min}m ago)")
    else:
        stats_table.add_row("Repo list", "fetched")
    stats_table.add_row("API calls made", str(stats.api_calls))
    stats_table.add_row("Commits processed", str(stats.commits_processed))
    if stats.commits_skipped:
        stats_table.add_row("Commits skipped", f"[yellow]{stats.commits_skipped}[/yellow]")
    stats_table.add_row("Cache size on disk", f"{cache_size / 1024:.1f} KB")

    console.print(stats_table)


if __name__ == "__main__":
    main()
