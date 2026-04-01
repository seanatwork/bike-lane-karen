require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const AustinDataAPI = require('./api-client');
const Logger = require('./logger');
const InputValidator = require('./validator');
const RateLimiter = require('./rate-limiter');

const token = process.env.TELEGRAM_TOKEN;
const austinAppToken = process.env.AUSTIN_APP_TOKEN;
const logLevel = process.env.LOG_LEVEL || 'info';
const exemptUsers = process.env.EXEMPT_USERS ? process.env.EXEMPT_USERS.split(',').map(id => id.trim()) : [];

// Initialize logger
const logger = new Logger(logLevel);

// Initialize rate limiter with exempt users
const rateLimiter = new RateLimiter(30, 60000, exemptUsers);
const chatRateLimiter = rateLimiter.createChatLimiter(5, 60000, exemptUsers);

logger.info('Starting Restaurant Inspection Bot');
logger.debug('Loaded tokens:', {
    telegram: token ? `${token.substring(0, 10)}...` : 'NOT FOUND',
    austin: austinAppToken ? `${austinAppToken.substring(0, 10)}...` : 'NOT FOUND'
});
logger.debug('Exempt users:', exemptUsers.length > 0 ? exemptUsers : 'None');

if (!token || !austinAppToken) {
    logger.error('Missing required environment variables: TELEGRAM_TOKEN and AUSTIN_APP_TOKEN');
    process.exit(1);
}

const bot = new TelegramBot(token, { polling: true });
const apiClient = new AustinDataAPI(austinAppToken, logger);

async function searchLowestScoringRestaurants(limit = 10) {
    try {
        return await apiClient.getLowestScoringRestaurants(limit);
    } catch (error) {
        logger.error('Error fetching low scoring restaurants:', error.message);
        throw error;
    }
}

function formatLowScores(restaurants) {
    if (!restaurants || !Array.isArray(restaurants) || restaurants.length === 0) {
        return "No restaurant scores found. Please try again later.";
    }
    
    let message = `🚨 *Lowest Scoring Restaurants* 🚨\n\n`;
    message += `Showing ${restaurants.length} restaurants with lowest inspection scores:\n\n`;
    
    restaurants.forEach((restaurant, index) => {
        const name = restaurant.restaurant_name || 'Unknown';
        const address = restaurant.address || 'Address not available';
        const inspectionDate = restaurant.inspection_date || 'Date not available';
        const score = restaurant.score ? Math.round(restaurant.score) : 'N/A';
        const processDesc = restaurant.process_description || 'Not available';
        
        message += `${index + 1}. *${name}*\n`;
        message += `💩 Score: ${score}\n`;
        message += `📍 ${address}\n`;
        message += `📅 ${inspectionDate}\n`;
        message += `📋 ${processDesc}\n\n`;
    });
    
    return message;
}

async function searchRestaurants(searchTerm, limit = 10) {
    try {
        // Validate and sanitize input
        const sanitizedTerm = InputValidator.sanitizeSearchTerm(searchTerm);
        const validatedLimit = InputValidator.validateLimit(limit);
        
        if (!validatedLimit.valid) {
            throw new Error(validatedLimit.error);
        }
        
        return await apiClient.searchRestaurants(sanitizedTerm, validatedLimit.sanitized);
    } catch (error) {
        logger.error('Error searching restaurants:', error.message);
        throw error;
    }
}

function getSearchSuggestions(searchTerm, searchType) {
    const suggestions = [];
    
    if (searchType === 'restaurant_name') {
        // Common restaurant name variations
        const commonVariations = {
            'mcdonald': 'McDonald\'s, McDonalds, Mcdonalds',
            'burger': 'Burger King, Whataburger, Hamburger',
            'pizza': 'Pizza Hut, Domino\'s, Papa Johns',
            'taco': 'Taco Bell, Taco Cabana, Torchy\'s',
            'chicken': 'Popeyes, KFC, Chick-fil-A',
            'starbucks': 'Starbucks, Coffee'
        };
        
        const lowerSearch = searchTerm.toLowerCase();
        for (const [key, variations] of Object.entries(commonVariations)) {
            if (lowerSearch.includes(key)) {
                suggestions.push(`Try: ${variations}`);
                break;
            }
        }
    }
    
    if (suggestions.length === 0) {
        suggestions.push('Try using a more general search term');
        suggestions.push('Check spelling and try again');
        if (searchType === 'restaurant_name') {
            suggestions.push('Try searching by address instead');
        }
    }
    
    return suggestions;
}

function formatRestaurantResults(restaurants, searchTerm) {
    if (!restaurants || !Array.isArray(restaurants) || restaurants.length === 0) {
        const searchType = InputValidator.detectSearchType(searchTerm);
        const suggestions = getSearchSuggestions(searchTerm, searchType.type);
        
        let message = searchType.type === 'address' 
            ? "No restaurants found at that address."
            : "No restaurants found with that name.";
        
        message += "\n\n💡 *Suggestions:*\n";
        suggestions.forEach((suggestion, index) => {
            message += `${index + 1}. ${suggestion}\n`;
        });
        
        return message;
    }
    
    // Group restaurants by exact name
    const groupedRestaurants = {};
    restaurants.forEach(restaurant => {
        const name = restaurant.restaurant_name || 'Unknown';
        if (!groupedRestaurants[name]) {
            groupedRestaurants[name] = [];
        }
        groupedRestaurants[name].push(restaurant);
    });
    
    // Sort each group by inspection date (most recent first)
    Object.keys(groupedRestaurants).forEach(name => {
        groupedRestaurants[name].sort((a, b) => {
            const dateA = new Date(a.inspection_date || '1900-01-01');
            const dateB = new Date(b.inspection_date || '1900-01-01');
            return dateB - dateA; // Most recent first
        });
    });
    
    let message = `Found ${restaurants.length} inspection(s) for ${Object.keys(groupedRestaurants).length} restaurant(s):\n\n`;
    
    // Display grouped results with pagination for large sets
    Object.keys(groupedRestaurants).forEach((restaurantName, groupIndex) => {
        const inspections = groupedRestaurants[restaurantName];
        const mostRecent = inspections[0]; // First one is most recent after sorting
        
        message += `🏪 *${restaurantName}*\n`;
        message += `📍 ${mostRecent.address || 'Address not available'}\n`;
        message += `📅 Most recent: ${mostRecent.inspection_date || 'Date not available'}\n`;
        message += `⭐ Latest score: ${mostRecent.score ? Math.round(mostRecent.score) : 'N/A'}\n`;
        message += `📋 ${mostRecent.process_description || 'Not available'}\n`;
        
        if (inspections.length > 1) {
            message += `📊 ${inspections.length} total inspections recorded\n`;
        }
        
        message += '\n';
        
        // Add pagination hint if we have many results
        if (groupIndex >= 8 && Object.keys(groupedRestaurants).length > 10) {
            message += `... and ${Object.keys(groupedRestaurants).length - groupIndex - 1} more restaurants.\n`;
            message += '💡 Try a more specific search for fewer results.\n\n';
            return message; // Early return for long lists
        }
    });
    
    return message;
}

// Handle /help command
bot.onText(/\/help/, async (msg) => {
    const chatId = msg.chat.id;
    
    const helpMessage = `🤖 *Restaurant Inspection Bot Help*

📝 *How to use:*
• Simply type a restaurant name (e.g., "McDonald's")
• Or type an address (e.g., "123 Main St")
• Use /lowscores to see restaurants with worst inspection scores

🔍 *Search Examples:*
• McDonald's
• 1234 Congress Ave
• Taco Bell
• 500 E 5th St

📋 *Commands:*
• /help - Show this help message
• /lowscores - Show 10 restaurants with lowest scores

💡 *Tips:*
• Search is case-insensitive
• Use partial names ("pizza" finds all pizza places)
• Addresses work with street names and numbers
• Results show most recent inspections first

⚠️ *Rate Limits:*
• 30 requests per minute globally
• 5 requests per minute per user

📊 *Data Source:* Austin Restaurant Inspections (City of Austin Open Data)`;
    
    try {
        await bot.sendMessage(chatId, helpMessage, { parse_mode: 'Markdown' });
    } catch (error) {
        logger.error('Error sending help message:', error.message);
        await bot.sendMessage(chatId, 'Sorry, I encountered an error. Please try again.');
    }
});

// Handle /lowscores command
bot.onText(/\/lowscores/, async (msg) => {
    const chatId = msg.chat.id;
    
    try {
        // Check rate limits
        const rateCheck = await chatRateLimiter.checkLimit(chatId);
        if (!rateCheck.allowed) {
            await bot.sendMessage(chatId, rateCheck.message);
            return;
        }
        
        await bot.sendMessage(chatId, '🔍 Searching for restaurants with the lowest inspection scores...');
        
        const restaurants = await searchLowestScoringRestaurants();
        const formattedMessage = formatLowScores(restaurants);
        
        await bot.sendMessage(chatId, formattedMessage, { parse_mode: 'Markdown' });
        
    } catch (error) {
        logger.error('Error searching for low scores:', error.message);
        
        let errorMessage = 'Sorry, I encountered an error while searching for restaurant scores. ';
        
        if (error.code === 'ENOTFOUND') {
            errorMessage += 'The restaurant database appears to be unavailable. Please try again later.';
        } else if (error.message.includes('JSON')) {
            errorMessage += 'There was an issue processing the restaurant data. Please try again.';
        } else if (error.message.includes('timeout')) {
            errorMessage += 'The request timed out. Please try again.';
        } else {
            errorMessage += 'Please try again in a few moments.';
        }
        
        await bot.sendMessage(chatId, errorMessage);
    }
});

bot.onText(/(.+)/, async (msg) => {
    const chatId = msg.chat.id;
    const searchTerm = msg.text.trim();
    
    if (searchTerm.startsWith('/')) {
        return;
    }
    
    try {
        // Validate input
        const validation = InputValidator.validateSearchTerm(searchTerm);
        if (!validation.valid) {
            await bot.sendMessage(chatId, `❌ ${validation.error}`);
            return;
        }
        
        // Check rate limits
        const rateCheck = await chatRateLimiter.checkLimit(chatId);
        if (!rateCheck.allowed) {
            await bot.sendMessage(chatId, rateCheck.message);
            return;
        }
        
        const searchType = InputValidator.detectSearchType(searchTerm);
        await bot.sendMessage(chatId, `🔍 Searching for restaurants by ${searchType.type}: "${validation.sanitized}"...`);
        
        const restaurants = await searchRestaurants(validation.sanitized);
        const formattedMessage = formatRestaurantResults(restaurants, validation.sanitized);
        
        await bot.sendMessage(chatId, formattedMessage, { parse_mode: 'Markdown' });
        
    } catch (error) {
        logger.error('Error searching restaurants:', error.message);
        
        let errorMessage = 'Sorry, I encountered an error while searching for restaurants. ';
        
        if (error.code === 'ENOTFOUND') {
            errorMessage += 'The restaurant database appears to be unavailable. Please try again later.';
        } else if (error.message.includes('JSON')) {
            errorMessage += 'There was an issue processing the restaurant data. Please try again.';
        } else if (error.message.includes('timeout')) {
            errorMessage += 'The request timed out. Please try again.';
        } else {
            errorMessage += 'Please try again in a few moments.';
        }
        
        await bot.sendMessage(chatId, errorMessage);
    }
});

bot.on('polling_error', (error) => {
    logger.error('Telegram polling error:', error.message);
});

bot.on('error', (error) => {
    logger.error('Telegram bot error:', error.message);
});

logger.info('Restaurant inspection bot is running...');
