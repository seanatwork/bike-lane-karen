# Austin 311

A Telegram bot for Austin 311 data — alerts, graffiti, bicycle infrastructure, animal services, traffic, noise, parking, parks, water quality, permits, bars, court caseloads, and more.

## Commands

| Command | Description |
|---|---|
| `/start` | Main menu with all services |
| `/help` | List all commands and info |
| `/subscribe` | Push alerts for 311 reports, animals, and crashes near you |
| `/myalerts` | View and manage your active alerts |
| `/unsubscribe` | Cancel all alerts |
| `/deletedata` | Remove all your stored data |

All services are accessed through inline menus from `/start`.

## Setup

```bash
git clone <repo-url>
cd austin311bot-unofficial
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # add your AUSTIN311_BOT_TOKEN
python austin311_bot.py
```

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `AUSTIN311_BOT_TOKEN` | Yes | Telegram bot token from [@BotFather](https://t.me/BotFather) |
| `AUSTINAPIKEY` | No | Austin Open Data token (higher rate limits) |

## Data Sources

- [Austin Open311 API](https://austintexas.gov/department/311) — live service requests
- [Austin Open Data Portal](https://data.austintexas.gov) — crash reports, permits, court data, water quality, etc.
- [Texas Open Data Portal](https://data.texas.gov) — TABC mixed beverage sales

## License

MIT