const https = require('https');
const { promisify } = require('util');

class AustinDataAPI {
    constructor(appToken, logger = console) {
        this.appToken = appToken;
        this.baseURL = 'https://data.austintexas.gov/resource/ecmv-9xxi.json';
        this.logger = logger;
        this.timeout = 10000; // 10 seconds timeout
        this.maxRetries = 3;
        this.retryDelay = 1000; // Base delay in ms
    }

    async makeRequest(url, retries = 0) {
        return new Promise((resolve, reject) => {
            const request = https.get(url, { timeout: this.timeout }, (res) => {
                let data = '';
                
                res.on('data', (chunk) => {
                    data += chunk;
                });
                
                res.on('end', () => {
                    try {
                        if (data.trim().startsWith('<')) {
                            throw new Error('API returned HTML instead of JSON - check app token or URL format');
                        }
                        
                        const jsonData = JSON.parse(data);
                        this.logger.debug(`API response: ${jsonData.length} records`);
                        resolve(jsonData);
                    } catch (error) {
                        this.logger.error('API parsing error:', error.message);
                        this.logger.debug('Raw response (first 200 chars):', data.substring(0, 200));
                        reject(error);
                    }
                });
            });

            request.on('error', (error) => {
                this.logger.error('API request error:', error.message);
                reject(error);
            });

            request.on('timeout', () => {
                request.destroy();
                reject(new Error('API request timeout'));
            });
        }).catch(async (error) => {
            if (retries < this.maxRetries && this.shouldRetry(error)) {
                const delay = this.retryDelay * Math.pow(2, retries);
                this.logger.warn(`Retrying API request in ${delay}ms (attempt ${retries + 1}/${this.maxRetries})`);
                await this.sleep(delay);
                return this.makeRequest(url, retries + 1);
            }
            throw error;
        });
    }

    shouldRetry(error) {
        const retryableErrors = [
            'ETIMEDOUT',
            'ENOTFOUND',
            'ECONNRESET',
            'ECONNREFUSED'
        ];
        return retryableErrors.some(code => error.code === code) || 
               error.message.includes('timeout') ||
               error.message.includes('HTML instead of JSON');
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    buildURL(params) {
        const urlParams = new URLSearchParams();
        
        Object.entries(params).forEach(([key, value]) => {
            if (value !== undefined && value !== null) {
                urlParams.append(key, value);
            }
        });
        
        urlParams.append('$$app_token', this.appToken);
        return `${this.baseURL}?${urlParams.toString()}`;
    }

    async searchRestaurants(searchTerm, limit = 10) {
        const isAddress = this.detectAddress(searchTerm);
        let whereClause;
        
        if (isAddress) {
            const streetAddress = searchTerm.split(',')[0].trim();
            whereClause = `upper(address) like upper('%${streetAddress}%')`;
        } else {
            const cleanSearchTerm = searchTerm.replace(/[^\w\s]/g, '').trim();
            whereClause = `upper(restaurant_name) like upper('%${cleanSearchTerm}%')`;
        }
        
        const url = this.buildURL({
            $where: whereClause,
            $order: 'inspection_date DESC',
            $limit: limit
        });
        
        this.logger.debug(`Searching ${isAddress ? 'address' : 'restaurant'}: "${searchTerm}"`);
        return this.makeRequest(url);
    }

    async getLowestScoringRestaurants(limit = 10) {
        const url = this.buildURL({
            $where: 'score is not null',
            $order: 'score ASC',
            $limit: limit
        });
        
        this.logger.debug('Fetching lowest scoring restaurants');
        return this.makeRequest(url);
    }

    detectAddress(searchTerm) {
        const hasNumbers = /\d/.test(searchTerm);
        const hasStreetIndicator = /(st|ave|avenue|rd|road|dr|drive|blvd|boulevard|ln|lane|way|court|ct|pl|place|sq|square)/i.test(searchTerm);
        return hasNumbers && hasStreetIndicator;
    }
}

module.exports = AustinDataAPI;
