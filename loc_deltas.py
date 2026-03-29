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
Past days are cached indefinitely. Today's data is re-used for up to
TODAY_CACHE_TTL_SECONDS seconds, then re-fetched automatically.
Use --no-cache to force a full re-fetch of all days.
"""

import json
import os
import sys
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from rich.align import Align
from rich.console import Console
from rich.table import Table

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
DETAIL_WORKERS = 10          # parallel threads for commit-detail fetches
TODAY_CACHE_TTL_SECONDS = 300  # re-fetch today after 5 minutes


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
# In-place progress line
# ---------------------------------------------------------------------------

_progress_len = 0


def progress(msg: str) -> None:
    """Overwrite the current terminal line in place."""
    global _progress_len
    try:
        cols = os.get_terminal_size().columns
    except OSError:
        cols = 80
    display = msg[:cols]
    # Pad to cover any previous longer content
    padded = display.ljust(max(_progress_len, len(display)))
    sys.stdout.write(f"\r\033[2m{padded}\033[0m")
    sys.stdout.flush()
    _progress_len = len(display)


def clear_progress() -> None:
    """Erase the progress line so the next console.print starts cleanly."""
    global _progress_len
    if _progress_len:
        sys.stdout.write(f"\r{' ' * _progress_len}\r")
        sys.stdout.flush()
        _progress_len = 0


# ---------------------------------------------------------------------------
# Stats tracking
# ---------------------------------------------------------------------------

@dataclass
class RunStats:
    days_from_cache: int = 0
    days_fetched: int = 0
    repos_from_cache: bool = False
    repo_cache_age_s: float | None = None
    today_from_cache: bool = False
    today_cache_age_s: float | None = None
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

    local_tz = datetime.now().astimezone().tzinfo
    now = datetime.now(local_tz)
    today = now.date()

    t_start = now.timestamp()
    console = Console()
    stats = RunStats()

    if not GITHUB_TOKEN:
        console.print(
            "[yellow]Warning:[/yellow] GITHUB_TOKEN not set — unauthenticated rate limit is 60 req/hour."
        )

    # Load cache (ignore if --no-cache)
    cache: dict = {} if no_cache else load_cache(username)
    if no_cache:
        console.print("[dim]Cache disabled — fetching all data fresh (including repo list).[/dim]")

    # Check if today's cached data is still fresh
    today_cached_at = cache.get("_today_cached_at", 0)
    today_cache_age = now.timestamp() - today_cached_at
    today_is_fresh = (not no_cache) and (str(today) in cache) and (today_cache_age < TODAY_CACHE_TTL_SECONDS)

    if today_is_fresh:
        stats.today_from_cache = True
        stats.today_cache_age_s = today_cache_age

    # Determine which days still need fetching
    dates_needed = []
    for i in range(n_days):
        date = today - timedelta(days=i)
        if date == today:
            if not today_is_fresh:
                dates_needed.append(date)
        elif str(date) not in cache:
            dates_needed.append(date)

    stats.days_from_cache = n_days - len(dates_needed)
    stats.days_fetched = len(dates_needed)

    # date -> [added, changed, deleted, commits]
    daily: dict = defaultdict(lambda: [0, 0, 0, 0])

    # Seed from cache for days we already have (pad to 4 elements for old caches)
    for date_str, day_stats in cache.items():
        if date_str.startswith("_"):
            continue  # skip metadata keys
        padded = list(day_stats) + [0] * (4 - len(day_stats))
        daily[date_str] = padded

    if dates_needed:
        fetch_since = datetime.combine(min(dates_needed), datetime.min.time()).replace(tzinfo=local_tz)
        fetch_until = datetime.combine(max(dates_needed), datetime.max.time()).replace(tzinfo=local_tz)

        repos, repo_cache_age = (None, None) if no_cache else load_cached_repos(username)
        if repos is not None:
            stats.repos_from_cache = True
            stats.repo_cache_age_s = repo_cache_age
            repo_source = "[dim](repo list from cache)[/dim]"
        else:
            progress(f"  Fetching repo list for {username}…")
            try:
                repos = get_repos(username, stats)
            except requests.HTTPError as e:
                clear_progress()
                console.print(f"[red]Error fetching repos:[/red] {e}")
                sys.exit(1)
            save_cached_repos(username, repos)
            repo_source = ""

        clear_progress()
        console.print(
            f"Found [bold]{len(repos)}[/bold] repos {repo_source}. Scanning commits…"
        )

        dates_needed_set = {str(d) for d in dates_needed}
        n_repos = len(repos)

        # Phase 1: collect all (repo, sha, date_str) needing detail fetches
        pending: list[tuple[str, str, str]] = []
        for idx, repo in enumerate(repos):
            progress(f"  [{idx + 1}/{n_repos}]  {repo['full_name']}")
            try:
                commits = get_commits(repo["full_name"], fetch_since, fetch_until, username, stats)
            except requests.HTTPError:
                stats.commits_skipped += 1
                continue
            for commit in commits:
                date_str = commit["commit"]["author"]["date"]
                date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(local_tz).date()
                if str(date) in dates_needed_set:
                    pending.append((repo["full_name"], commit["sha"], str(date)))

        # Phase 2: fetch commit details in parallel
        total_pending = len(pending)
        completed = 0
        lock = threading.Lock()

        def fetch_detail(item: tuple[str, str, str]):
            repo_full_name, sha, date_str = item
            detail = get_commit_detail(repo_full_name, sha, stats)
            return date_str, compute_file_stats(detail)

        clear_progress()
        if total_pending:
            console.print(
                f"Fetching details for [bold]{total_pending}[/bold] commit(s)"
                f" ([dim]{DETAIL_WORKERS} threads[/dim])…"
            )
            with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
                futures = {executor.submit(fetch_detail, item): item for item in pending}
                for future in as_completed(futures):
                    try:
                        date_str, (a, c, d) = future.result()
                        with lock:
                            daily[date_str][0] += a
                            daily[date_str][1] += c
                            daily[date_str][2] += d
                            daily[date_str][3] += 1
                            stats.commits_processed += 1
                            completed += 1
                        progress(f"  Details  {completed}/{total_pending}")
                    except requests.HTTPError:
                        with lock:
                            stats.commits_skipped += 1
                            completed += 1

        clear_progress()
        console.print(
            f"Processed [bold]{stats.commits_processed}[/bold] commit(s)"
            + (f", skipped {stats.commits_skipped} due to errors." if stats.commits_skipped else ".")
        )

        # Update cache — past days kept indefinitely; today stamped with fetch time
        updated_cache = {k: v for k, v in cache.items() if k.startswith("_")}
        updated_cache.update({k: v for k, v in cache.items() if not k.startswith("_")})
        for date_str in dates_needed_set:
            updated_cache[date_str] = daily[date_str]
        updated_cache["_today_cached_at"] = now.timestamp()
        save_cache(username, updated_cache)

    else:
        clear_progress()
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
    table.add_column("Commits", style="blue", justify="right")
    table.add_column("Added", style="green", justify="right")
    table.add_column("Changed", style="yellow", justify="right")
    table.add_column("Deleted", style="red", justify="right")

    totals = [0, 0, 0, 0]

    for i in range(n_days):
        date = today - timedelta(days=i)
        a, c, d, n = daily.get(str(date), [0, 0, 0, 0])
        totals[0] += a
        totals[1] += c
        totals[2] += d
        totals[3] += n
        date_cell = f"[bold white]{date}[/bold white]" if date == today else str(date)
        row_style = "bold" if date == today else ("dim" if not (a or c or d) else "")
        table.add_row(date_cell, str(n) if n else "", f"{a:,}", f"{c:,}", f"{d:,}", style=row_style)

    table.columns[1].footer = f"[blue]{totals[3]}[/blue]"
    table.columns[2].footer = f"[green]{totals[0]:,}[/green]"
    table.columns[3].footer = f"[yellow]{totals[1]:,}[/yellow]"
    table.columns[4].footer = f"[red]{totals[2]:,}[/red]"

    console.print(Align.center(table, width=80))

    # ---------------------------------------------------------------------------
    # Stats panel — cache column + fetch column side by side
    # ---------------------------------------------------------------------------
    elapsed = datetime.now(timezone.utc).timestamp() - t_start
    cache_size = cache_file_size(username)

    def kv_table() -> Table:
        t = Table(show_header=False, box=None, padding=(0, 2))
        t.add_column(style="dim")
        t.add_column(justify="right")
        return t

    # Left column
    left_col = kv_table()
    left_col.add_row("Total time", f"{elapsed:.1f}s")
    left_col.add_row("API calls made", str(stats.api_calls))
    left_col.add_row("Days fetched", str(stats.days_fetched))
    left_col.add_row("Days from cache", f"[green]{stats.days_from_cache}[/green] / {n_days}")

    # Right column
    right_col = kv_table()
    if stats.repos_from_cache and stats.repo_cache_age_s is not None:
        age_min = int(stats.repo_cache_age_s // 60)
        right_col.add_row("Repo list", f"[green]cached[/green] ({age_min}m)")
    else:
        right_col.add_row("Repo list", "fetched")
    if stats.today_from_cache and stats.today_cache_age_s is not None:
        right_col.add_row("Today", f"[green]cached[/green] ({int(stats.today_cache_age_s)}s)")
    else:
        right_col.add_row("Today", "fetched")
    right_col.add_row("|Cache| on disk", f"{cache_size / 1024:.1f} KB")

    outer = Table(show_header=False, box=None, padding=(0, 2))
    outer.add_column()
    outer.add_column()
    outer.add_row(left_col, right_col)

    console.print(Align.center(outer, width=80))


if __name__ == "__main__":
    main()
