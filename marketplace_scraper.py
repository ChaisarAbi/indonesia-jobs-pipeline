import requests
import json
from datetime import datetime

def scrap_marketplace(keyword="ssd samsung"):
    print(f"Memulai scraping untuk keyword: {keyword}")
    
    # URL contoh (kita pakai API search internal mereka kalau bisa, atau scrape HTML)
    # Ini adalah contoh dummy logic untuk simulasi ETL
    url = f"https://api.example-marketplace.com/search?q={keyword}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://www.google.com/'
    }

    # Simulasi data (nanti kita ganti dengan logic request asli)
    # Di DevOps, kita fokus ke pipeline-nya dulu
    mock_results = [
        {"item": "SSD Samsung 980 500GB", "price": 850000, "merchant": "TechStore"},
        {"item": "Samsung 980 Pro 1TB", "price": 1500000, "merchant": "Official Samsung"}
    ]

    processed_data = []
    for product in mock_results:
        # ETL: Kita tambahin metadata
        product['keyword'] = keyword
        product['scraped_at'] = datetime.now().isoformat()
        product['source'] = 'Tokopedia' # Contoh
        processed_data.append(product)

    # Kirim ke OpenSearch
    os_url = "http://aventra-opensearch:9200/marketplace-intelligence/_doc"
    
    for item in processed_data:
        res = requests.post(os_url, json=item)
        print(f"Push to OpenSearch: {res.status_code}")

if __name__ == "__main__":
    scrap_marketplace()
