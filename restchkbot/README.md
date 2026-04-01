# Restaurant Inspection Bot

A Telegram bot that searches Austin restaurant inspection data using the city's open data API.

## Setup

1. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```

2. Fill in your environment variables in `.env`:
   - `TELEGRAM_TOKEN`: Get this from [@BotFather](https://t.me/botfather) on Telegram
   - `AUSTIN_APP_TOKEN`: Get this from [Austin Open Data Portal](https://data.austintexas.gov/)
   - `LOG_LEVEL`: Set to `debug`, `info`, `warn`, or `error` (default: `info`)
   - `EXEMPT_USERS`: Comma-separated list of Telegram user IDs exempt from rate limiting (optional)

3. Install dependencies:
   ```bash
   npm install
   ```

4. Run the bot:
   ```bash
   node bot.js
   ```

## Usage

**Commands:**
- `/help` - Show help message with usage examples
- `/lowscores` - Show 10 restaurants with lowest inspection scores
- Any other text - Search for restaurants by name or address

**Search Examples:**
- Restaurant name: `McDonald's`, `Taco Bell`, `Starbucks`
- Address: `1234 Congress Ave`, `500 E 5th St`
- Partial names: `pizza` (finds all pizza places)

**Features:**
- Smart search detection (automatically detects addresses vs restaurant names)
- Rate limiting (30 requests/minute global, 5 requests/minute per user)
- User exemptions - Specific users can be exempted from rate limits
- Search suggestions when no results found
- Retry logic for API failures
- Comprehensive error handling
- Pagination hints for large result sets

## Features

- **Modular Architecture**: Clean separation of concerns with dedicated modules
- **Smart Search**: Automatic detection of addresses vs restaurant names
- **Rate Limiting**: Built-in protection against API abuse with user exemptions
- **User Exemptions**: Specific users can be exempted from rate limiting via configuration
- **Error Resilience**: Retry logic with exponential backoff
- **Input Validation**: Comprehensive sanitization and validation
- **Search Suggestions**: Helpful alternatives when no results found
- **Pagination**: Smart handling of large result sets
- **Logging**: Configurable logging levels
- **Help System**: Built-in help command with examples
- **Polling-based**: No webhook setup required
- **Markdown Formatting**: Rich message formatting
- **Comprehensive Error Handling**: User-friendly error messages
