# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ArXiv Pusher — a scheduled bot that fetches new arXiv papers daily, optionally filters them via an LLM interest check, summarizes them with an LLM, and emails Markdown reports to per-user mailing lists. Documentation, code comments, and commit messages are written in Chinese.

## Commands

```bash
uv sync                                       # install dependencies (Python 3.12, managed by uv)
uv run main.py                                # start blocking scheduler (daily 16:00, CronTrigger in run_scheduler())
uv run test_email.py                          # manually email the repo-root report.md as a test
uv run query_usage.py --summary               # token/cost summary for all users
uv run query_usage.py --user "组名" --days 7   # per-user usage over last N days
```

There is no formal test suite or linter. `test_email.py` is a standalone manual script, not part of a test framework.

To run the pipeline once for testing, edit the `if __name__ == "__main__":` block at the bottom of `main.py` to call `daily_job()` instead of `run_scheduler()`.

## Configuration (required, not in repo)

`config.py` is gitignored but imported at module level by `main.py` — nothing runs without it. Create it following the complete example in README.md. It must define:

- `AI_CONFIG` — OpenAI-compatible API settings (`api_key`, `base_url`, `model`; optional `price_per_million_input_tokens` / `price_per_million_output_tokens` enable cost logging)
- `EMAIL_SERVER_CONFIG` — SMTP settings (`sender`, `password`, `smtp_server`, `smtp_port`, `use_tls`)
- `GENERAL_CONFIG` — `days_lookback`, `max_papers_per_user` (`None` = unlimited)
- `USERS_CONFIG` — list of user groups: `name`, `email` (comma-separated for multiple), `arxiv_categories`, optional `custom_prompt` (uses `{text}` placeholder), optional `interest_filter_prompt` (uses `{abstract}` placeholder)
- `DEFAULT_PROMPT_TEMPLATE` — fallback summarization prompt with `{text}` placeholder

## Architecture

Three modules:

- **`main.py`** — entire pipeline plus scheduling. `daily_job()` iterates `USERS_CONFIG`, calling `process_user()` per user with a 60s delay between users (arXiv rate limiting). Per user: `fetch_papers()` → optional AI interest filter → hard truncation → text extraction → AI summarize → email + local report.
- **`database.py`** — `TokenUsageDB`, SQLite wrapper over `token_usage.db` recording per-user-per-day token usage and cost. `get_db()` returns thread-local instances. One record per user/day (UNIQUE constraint, `INSERT OR REPLACE`).
- **`query_usage.py`** — rich-based CLI over the same database.

### Pipeline details (`process_user` in main.py)

1. `fetch_papers()` queries the arXiv API for the user's categories; the target date is the previous **workday** (weekends roll back to Friday).
2. Interest filtering (if `interest_filter_prompt` is set) runs concurrently with `ThreadPoolExecutor` (max 3 workers). Ambiguous or failed AI answers default to **"interested"** so papers are never silently lost. Rejected papers become a review appendix at the end of the report.
3. `get_paper_text()` uses a fallback chain: direct PDF + PyPDF2 → arXiv HTML version via BeautifulSoup (with optional wkhtmltopdf if installed) → abstract as last resort. Text is truncated at 129,024 chars.
4. `gpt_summarize()` uses the user's `custom_prompt` or `DEFAULT_PROMPT_TEMPLATE`.

### Email

`send_email()` converts Markdown to HTML with `markdown2` (extras: `tables`, `latex`, `fenced-code-blocks`); the `latex` extra depends on `latex2mathml` for equation rendering. Multi-recipient addresses are comma-separated strings split before sending.

### Runtime artifacts (gitignored)

`temp/<user_name>/` (downloaded PDFs, HTML, `report.md` per user), `token_usage.db`, `arxiv_pusher.log`.
