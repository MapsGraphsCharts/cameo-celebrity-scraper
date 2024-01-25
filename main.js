const { BasicCrawler, RequestQueue } = require('crawlee');
const axios = require('axios');
const sqlite3 = require('sqlite3').verbose();

// Define an array of category paths
const categories = [
    "/creators",
    "/creators/influencers",
    "/actors",
    "/athletes",
    "/more",
    "/musicians",
    "/reality-tv",
    "/creators/influencers/tiktok",
    "/actors/tv",
    "/international",
    "/creators/influencers/youtube",
    "/musicians/singersongwriter",
    "/comedians",
    "/athletes/ncaa",
    "/more/models",
    "/athletes/football",
    "/athletes/basketball",
    "/actors/movies",
    "/international/en-espanol",
    "/musicians/hip-hop",
    "/creators/podcast",
    "/actors/voice-actors",
    "/creators/influencers/fitness",
    "/creators/bloggers",
    "/more/tv-hosts",
    "/actors/comedy",
    "/actors/netflix",
    "/musicians/rappers",
    "/featured",
    "/more/authors",
    "/more/dancers",
    "/actors/theater",
    "/athletes/baseball",
    "/musicians/lead-singers",
    "/more/radio",
    "/actors/film",
    "/musicians/rock-music",
    "/more/artists",
    "/athletes/wrestlers",
    "/actors/theater/broadway"
];

const db = new sqlite3.Database('./cameo.db', (err) => {
    if (err) {
        console.error(err.message);
    }
    console.log('Connected to the SQLite database.');
});

// Get today's date and format it as YYYYMMDD
const date = new Date();
const dateString = `${date.getFullYear()}${String(date.getMonth() + 1).padStart(2, '0')}${String(date.getDate()).padStart(2, '0')}`;

// Use the date string in the table name
const tableName = `users_${dateString}`;

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS ${tableName} (
        _id TEXT PRIMARY KEY,
        name TEXT,
        username TEXT,
        imageUrlKey TEXT,
        profession TEXT,
        price INTEGER,
        dmPrice INTEGER,
        iosPrice INTEGER,
        businessPrice INTEGER,
        talentSettings TEXT,
        userPromotions TEXT,
        averageMillisecondsToComplete INTEGER,
        tags TEXT,
        temporarilyUnavailable BOOLEAN,
        averageRating REAL,
        numOfRatings INTEGER,
        isAvailableForBusiness BOOLEAN,
        aaQueryId TEXT,
        aaIndex TEXT
    )`);
});

async function main() {
    // Initialize the RequestQueue
    const requestQueue = await RequestQueue.open();

    // Enqueue the initial requests for each category
    for (const category of categories) {
        const uniqueKey = `https://www.cameo.com/api/v3/search/users${category}0`; // Generate a unique key based on the URL and category
        await requestQueue.addRequest({
            url: 'https://www.cameo.com/api/v3/search/users',
            uniqueKey: uniqueKey, // Override the uniqueKey property
            userData: { category, page: 0 }
        });
    }

    const crawler = new BasicCrawler({
        requestQueue,
        async requestHandler({ request }) {
            const { category, page } = request.userData;
            const data = JSON.stringify({
                "requests": [
                    {
                        "indexName": "prod_cameo_talent_v0",
                        "params": {
                            "analytics": false,
                            "facets": [
                                "tags_slugs",
                                "new_category_slugs_paths",
                                "promo_price"
                            ],
                            "highlightPostTag": "__/ais-highlight__",
                            "highlightPreTag": "__ais-highlight__",
                            "hitsPerPage": 48,
                            "maxValuesPerFacet": 41,
                            "page": page,
                            "query": "",
                            "tagFilters": "",
                            "facetFilters": [`new_category_slugs_paths:${category}`]
                        }
                    }
                ]
            });

            let config = {
                method: 'post',
                url: request.url,
                headers: {
                    'Content-Type': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                },
                data: data
            };

            try {
                const apiResponse = await axios.request(config);
                const totalPages = apiResponse.data.results[0].nbPages;
                const hits = apiResponse.data.results[0].hits;

                for (const hit of hits) {
                    db.run(`INSERT OR REPLACE INTO ${tableName} (
                    _id, name, username, imageUrlKey, profession, price, dmPrice, iosPrice, businessPrice,
                    talentSettings, userPromotions, averageMillisecondsToComplete, tags, temporarilyUnavailable,
                    averageRating, numOfRatings, isAvailableForBusiness, aaQueryId, aaIndex
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                        [
                            hit._id, hit.name, hit.username, hit.imageUrlKey, hit.profession, hit.price, hit.dmPrice,
                            hit.iosPrice, hit.businessPrice, JSON.stringify(hit.talentSettings), JSON.stringify(hit.userPromotions),
                            hit.averageMillisecondsToComplete, JSON.stringify(hit.tags), hit.temporarilyUnavailable,
                            hit.averageRating, hit.numOfRatings, hit.isAvailableForBusiness, hit.aaQueryId, hit.aaIndex
                        ],
                        function (err) {
                            if (err) {
                                console.error(`Error occurred while inserting ${hit._id}:`, err);
                            }
                        }
                    );
                }

                console.log(`Current page: ${page}`);
                console.log(`Total pages: ${totalPages}`);

                // Enqueue the next page if available
                if (page + 1 < totalPages) {
                    console.log('Adding new request to queue');
                    const uniqueKey = `https://www.cameo.com/api/v3/search/users${category}${page + 1}`; // Generate a unique key based on the URL, category, and page
                    await requestQueue.addRequest({
                        url: 'https://www.cameo.com/api/v3/search/users',
                        uniqueKey: uniqueKey, // Override the uniqueKey property
                        userData: { category, page: page + 1 }
                    });
                } else {
                    console.log('No more pages to add');
                }

            } catch (error) {
                console.error('Error fetching API data:', error);
                throw error;
            }
        }
    });

    await crawler.run();
    db.close(() => {
        console.log('Closed the database connection.');
    });
}

main();
