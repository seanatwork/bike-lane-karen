class RateLimiter {
    constructor(maxRequests = 30, timeWindow = 60000, exemptUsers = []) { // 30 requests per minute
        this.maxRequests = maxRequests;
        this.timeWindow = timeWindow;
        this.requests = [];
        this.exemptUsers = exemptUsers.map(id => id.toString()); // Ensure all IDs are strings
    }

    async checkLimit(chatId = null) {
        // Check if user is exempt from rate limiting
        if (chatId && this.exemptUsers.includes(chatId.toString())) {
            return { allowed: true, exempt: true };
        }

        const now = Date.now();
        
        // Clean old requests
        this.requests = this.requests.filter(timestamp => 
            now - timestamp < this.timeWindow
        );
        
        // Check if we've exceeded the limit
        if (this.requests.length >= this.maxRequests) {
            const oldestRequest = Math.min(...this.requests);
            const waitTime = this.timeWindow - (now - oldestRequest);
            
            return {
                allowed: false,
                waitTime: Math.ceil(waitTime / 1000),
                message: `Rate limit exceeded. Please wait ${Math.ceil(waitTime / 1000)} seconds before trying again.`
            };
        }
        
        // Add current request
        this.requests.push(now);
        return { allowed: true };
    }

    async waitForSlot() {
        const check = await this.checkLimit();
        if (!check.allowed) {
            await this.sleep(check.waitTime * 1000);
            return this.waitForSlot();
        }
        return true;
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    // Per-chat rate limiting (optional enhancement)
    createChatLimiter(maxRequests = 5, timeWindow = 60000, exemptUsers = []) {
        const chatLimiters = new Map();
        
        return {
            checkLimit: async (chatId) => {
                // Check if user is exempt from rate limiting
                if (exemptUsers.includes(chatId.toString())) {
                    return { allowed: true, exempt: true };
                }

                if (!chatLimiters.has(chatId)) {
                    chatLimiters.set(chatId, new RateLimiter(maxRequests, timeWindow));
                }
                return chatLimiters.get(chatId).checkLimit(chatId);
            }
        };
    }
}

module.exports = RateLimiter;
