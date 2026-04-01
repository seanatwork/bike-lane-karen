class InputValidator {
    static validateSearchTerm(searchTerm) {
        if (!searchTerm || typeof searchTerm !== 'string') {
            return {
                valid: false,
                error: 'Search term is required and must be a string'
            };
        }

        const trimmed = searchTerm.trim();
        
        if (trimmed.length === 0) {
            return {
                valid: false,
                error: 'Search term cannot be empty'
            };
        }

        if (trimmed.length > 200) {
            return {
                valid: false,
                error: 'Search term is too long (max 200 characters)'
            };
        }

        // Check for potentially dangerous characters
        const dangerousPatterns = [
            /<script/i,
            /javascript:/i,
            /on\w+=/i,
            /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/  // Control characters
        ];

        for (const pattern of dangerousPatterns) {
            if (pattern.test(trimmed)) {
                return {
                    valid: false,
                    error: 'Search term contains invalid characters'
                };
            }
        }

        return {
            valid: true,
            sanitized: trimmed
        };
    }

    static sanitizeSearchTerm(searchTerm) {
        const validation = this.validateSearchTerm(searchTerm);
        if (!validation.valid) {
            throw new Error(validation.error);
        }
        
        // Additional sanitization for API safety
        return validation.sanitized
            .replace(/['"]/g, '')  // Remove quotes
            .replace(/\s+/g, ' ')  // Normalize whitespace
            .trim();
    }

    static detectSearchType(searchTerm) {
        const hasNumbers = /\d/.test(searchTerm);
        const hasStreetIndicator = /(st|ave|avenue|rd|road|dr|drive|blvd|boulevard|ln|lane|way|court|ct|pl|place|sq|square)/i.test(searchTerm);
        const isAddress = hasNumbers && hasStreetIndicator;
        
        return {
            isAddress,
            type: isAddress ? 'address' : 'restaurant_name'
        };
    }

    static validateLimit(limit) {
        const parsed = parseInt(limit, 10);
        if (isNaN(parsed) || parsed < 1 || parsed > 50) {
            return {
                valid: false,
                error: 'Limit must be a number between 1 and 50'
            };
        }
        return {
            valid: true,
            sanitized: parsed
        };
    }
}

module.exports = InputValidator;
