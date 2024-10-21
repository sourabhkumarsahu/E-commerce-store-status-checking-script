const fs = require('fs');
const csv = require('csv-parser');
const axios = require('axios');
const cheerio = require('cheerio');
const path = require('path');
const {promisify} = require('util');
const writeFile = promisify(fs.writeFile);

// File paths
const inputFile = path.join(__dirname, 'data', 'client_data.csv');
const outputFile = 'updated_client_data.csv';

// Delete the output file if it exists
if (fs.existsSync(outputFile)) {
    fs.unlinkSync(outputFile);
}

async function axiosRetry(url, options, retries = 2, backoff = 3000) {
    for (let i = 0; i < retries; i++) {
        try {
            return await axios.get(url, options);
        } catch (error) {
            if (i < retries - 1) {
                console.warn(`Retrying request to ${url} (${i + 1}/${retries})...`);
                await new Promise(resolve => setTimeout(resolve, backoff * (i + 1)));
            } else {
                throw error;
            }
        }
    }
}

async function isShopifyStore(url) {
    try {
        const response = await axiosRetry(url, {timeout: 15000});
        const html = response.data;

        if (url.includes('.myshopify.com') || url.includes('shopify.com')) {
            return {isShopify: true, isPasswordProtected: false};
        }

        const poweredByHeader = response.headers['powered-by'];
        if (poweredByHeader && poweredByHeader.includes('Shopify')) {
            return {isShopify: true, isPasswordProtected: false};
        }

        const $ = cheerio.load(html);
        const isShopifyMeta = $('meta[name="shopify-checkout-api-token"]').length > 0;
        const hasShopifyScripts = $('script[src*="shopify"]').length > 0;

        return {isShopify: isShopifyMeta || hasShopifyScripts, isPasswordProtected: false};
    } catch (error) {
        if (error.response) {
            const poweredByHeader = error.response.headers['powered-by'];
            if (poweredByHeader && poweredByHeader.includes('Shopify')) {
                return {isShopify: true, isPasswordProtected: false};
            }
            if (error.response.status === 404) {
                console.error(`URL not found: ${url}`);
                return {isShopify: false, isPasswordProtected: false};
            }
        }
        console.error(`Error checking if URL is Shopify store ${url}:`, error.message);
        return {isShopify: false, isPasswordProtected: false};
    }
}

async function checkURL(url) {
    try {
        const response = await axiosRetry(url, {timeout: 15000});
        return response.status === 200;
    } catch (error) {
        if (error.response) {
            const poweredByHeader = error.response.headers['powered-by'];
            if (poweredByHeader && poweredByHeader.includes('Shopify')) {
                return false; // Store is down but it's a Shopify store
            }
            if (error.response.status === 404) {
                console.error(`URL not found: ${url}`);
                return false;
            }
        }
        console.error(`Error checking URL ${url}:`, error.message);
        return false;
    }
}

async function checkPasswordProtection(url) {
    try {
        console.log(`Checking password protection for URL: ${url}`);

        // Send a GET request to the store URL
        const response = await axios.get(url, { timeout: 15000, maxRedirects: 5, validateStatus: status => status < 400 });
        console.log(`Password protection check response status for ${url}: ${response.status}`);

        // Check if the final URL is different from the requested URL
        const responseUrl = response.request.res.responseUrl || response.config.url;
        if (!responseUrl) {
            console.error(`Response URL is undefined for ${url}`);
            return false;
        }

        // If redirected to /password, check for input[type="password"]
        if (responseUrl.endsWith('/password')) {
            console.log('works');
            const $ = cheerio.load(response.data);
            const hasPasswordInput = $('input[type="password"]').length > 0;
            return hasPasswordInput;
        }

        // If not redirected, the store is not password protected
        return false;
    } catch (error) {
        if (error.response) {
            console.log(`Password protection check error response status for ${url}: ${error.response.status}`);
            if (error.response.status === 301 || error.response.status === 302) {
                return false;
            }
            if (error.response.status === 404) {
                return false;
            }
        }
        console.error(`Error checking password protection for URL ${url}:`, error.message);
        return false;
    }
}


async function processURL(url) {
    // Ensure the URL has a protocol
    if (!url.startsWith('http://') && !url.startsWith('https://')) {
        url = 'http://' + url;
    }

    const {isShopify} = await isShopifyStore(url);
    let isPasswordProtected = false;
    let isActive = false;

    if (isShopify) {
        isActive = await checkURL(url);
        if (isActive) {
            isPasswordProtected = await checkPasswordProtection(url);
        }
    }

    return {isShopify, isActive, isPasswordProtected};
}

async function processCSV() {
    console.time('Processing Time');
    const startTime = Date.now();
    const results = [];
    const readStream = fs.createReadStream(inputFile).pipe(csv());

    const concurrencyLimit = 10; // Number of concurrent requests
    const batch = [];

    for await (const data of readStream) {
        const url = data.URL;
        const clientId = data.ClientId;
        batch.push({url, clientId});

        if (batch.length >= concurrencyLimit) {
            const batchResults = await Promise.all(batch.map(({url, clientId}) => processURL(url).then(result => ({
                clientId,
                url, ...result
            }))));
            results.push(...batchResults);
            batch.length = 0; // Clear the batch
        }
    }

    if (batch.length > 0) {
        const batchResults = await Promise.all(batch.map(({url, clientId}) => processURL(url).then(result => ({
            clientId,
            url, ...result
        }))));
        results.push(...batchResults);
    }

    await writeCSV(results, startTime);
    const endTime = Date.now();
    console.timeEnd('Processing Time');
    console.log(`Start Time: ${new Date(startTime).toLocaleString()}`);
    console.log(`End Time: ${new Date(endTime).toLocaleString()}`);
}

async function writeCSV(data, startTime) {
    const headers = ['ClientId', 'URL', 'isShopify', 'isActive', 'isPasswordProtected'];
    const rows = data.map(row => {
        const clientId = row.ClientId || row.clientId;
        const url = row.URL || row.url;
        return `${clientId},${url},${row.isShopify},${row.isActive},${row.isPasswordProtected}`;
    }).join('\n');
    console.log(rows)
    const csvContent = [headers.join(','), rows].join('\n');
    // Calculate metrics
    const totalStores = data.length;
    const shopifyStores = data.filter(row => row.isShopify).length;
    const activeStores = data.filter(row => row.isActive).length;
    const passwordProtectedStores = data.filter(row => row.isPasswordProtected).length;
    const inactiveStores = totalStores - activeStores;
    const endTime = Date.now();
    const processingTime = (endTime - startTime) / 1000; // in seconds

    const metrics = [
        `Total Stores: ${totalStores}`,
        `Shopify Stores: ${shopifyStores}`,
        `Active Stores: ${activeStores}`,
        `Inactive Stores: ${inactiveStores}`,
        `Password Protected Stores: ${passwordProtectedStores}`,
        `Percentage Active: ${(activeStores / totalStores * 100).toFixed(2)}%`,
        `Percentage Inactive: ${(inactiveStores / totalStores * 100).toFixed(2)}%`,
        `Start Time: ${new Date(startTime).toLocaleString()}`,
        `End Time: ${new Date(endTime).toLocaleString()}`,
        `Processing Time: ${processingTime} seconds`
    ].join('\n');

    const finalContent = [csvContent, metrics].join('\n\n');

    await writeFile(outputFile, finalContent);
    console.log('CSV file updated successfully with metrics!');
}

processCSV().catch(error => console.error('Error processing CSV:', error));