# Restaurant Inspection Bot - Project Context

## Project Overview
A Telegram bot that queries Austin restaurant inspection data from the city's open data API. Built with Node.js and node-telegram-bot-api.

## Current Features
- **Restaurant Search**: Search by restaurant name (case-insensitive, special character handling)
- **Address Search**: Search by street address (extracts street portion from full addresses)
- **Low Scores Command**: `/lowscores` shows restaurants with worst inspection scores
- **Grouped Results**: Restaurants with same name are grouped with most recent inspection shown first
- **Error Handling**: Comprehensive error handling for API failures and user-friendly messages

## Technical Implementation

### API Integration
- **Dataset**: Austin Restaurant Inspections (`ecmv-9xxi`)
- **Authentication**: Uses `$$app_token` parameter
- **Query Format**: Socrata SoQL with proper URL encoding
- **Ordering**: Results sorted by `inspection_date DESC` for most recent first

### Search Logic
```javascript
// Address detection
const hasNumbers = /\d/.test(searchTerm);
const hasStreetIndicator = /(st|ave|avenue|rd|road|dr|drive|blvd|boulevard|ln|lane|way|court|ct|pl|place|sq|square)/i.test(searchTerm);
const isAddress = hasNumbers && hasStreetIndicator;

// Case-insensitive search
upper(restaurant_name) like upper('%searchterm%')
```

### Result Formatting
- Groups restaurants by exact name
- Shows most recent inspection details first
- Displays total inspection count per restaurant
- Uses emojis for better UX (💩 for low scores)

## Decisions Made

### 1. Single File Architecture
- **Decision**: Keep everything in `bot.js` for simplicity
- **Rationale**: Easier deployment and maintenance for a focused bot

### 2. Polling vs Webhooks
- **Decision**: Use polling
- **Rationale**: Simpler setup, no need for public URL/SSL

### 3. Search Strategy
- **Decision**: Flexible search with address/name detection
- **Rationale**: Better user experience - one interface for multiple search types

### 4. Error Handling
- **Decision**: Graceful degradation with user-friendly messages
- **Rationale**: Users get helpful feedback instead of technical errors

### 5. Result Grouping
- **Decision**: Group by restaurant name, show most recent first
- **Rationale**: More useful than chronological list of all inspections

## Known Issues

### 1. API Response Format
- **Issue**: Sometimes returns HTML instead of JSON
- **Status**: Partially fixed with better error handling
- **Impact**: Users get "database unavailable" message instead of results

### 2. Address Matching
- **Issue**: Some addresses don't match due to format differences
- **Status**: Improved with street extraction and case-insensitive search
- **Impact**: Some address searches may still fail

### 3. Restaurant Name Variations
- **Issue**: Franchise names may have variations in database
- **Status**: Improved with case-insensitive search and special character removal
- **Impact**: Some restaurants may still not be found

### 4. Score Data Quality
- **Issue**: Some restaurants have null scores
- **Status**: Handled in `/lowscores` with `score is not null` filter
- **Impact**: Low scores command excludes restaurants without scores

## Current Status
- ✅ Basic functionality working
- ✅ Restaurant and address search operational
- ✅ Low scores command implemented
- ✅ Error handling in place
- ⚠️ API connectivity sometimes unreliable
- ⚠️ Search matching could be improved

## Next Steps

### High Priority
1. **Improve Search Accuracy**
   - Add fuzzy matching for restaurant names
   - Handle more address format variations
   - Add search suggestions for near matches

2. **API Reliability**
   - Implement retry logic for failed requests
   - Add caching to reduce API calls
   - Monitor API response patterns

3. **User Experience**
   - Add pagination for large result sets
   - Implement search history
   - Add help command with examples

### Medium Priority
1. **Additional Commands**
   - `/recent` - most recent inspections
   - `/highscores` - best performing restaurants
   - `/stats` - inspection statistics

2. **Data Enrichment**
   - Add restaurant type filtering
   - Include violation details if available
   - Add location-based search

### Low Priority
1. **Performance**
   - Optimize API queries
   - Add response caching
   - Implement rate limiting

2. **Maintenance**
   - Add logging for debugging
   - Monitor bot uptime
   - Set up alerts for failures

## Environment Setup
- **Node.js**: Latest LTS
- **Dependencies**: node-telegram-bot-api, dotenv
- **Environment Variables**: TELEGRAM_TOKEN, AUSTIN_APP_TOKEN
- **Running**: `node bot.js` or `nodemon bot.js`

## File Structure
```
restchkbot/
├── bot.js           # Main bot implementation
├── .env            # Environment variables (not tracked)
├── .env.example    # Template for environment variables
├── package.json     # Node.js project configuration
└── README.md       # User documentation
```

## Testing Notes
- Restaurant search working (McDonald's, Popeyes tested)
- Address search needs more testing
- Low scores command functional
- Error handling tested with invalid inputs

## Deployment Considerations
- Requires persistent storage for .env file
- Node.js environment needed
- No external dependencies beyond npm packages
- Can run on any cloud platform supporting Node.js
