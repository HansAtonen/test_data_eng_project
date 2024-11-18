import requests
from bs4 import BeautifulSoup
import json

def crawl_website(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup
    except Exception as e:
        print(f"Error crawling website {url}: {e}")
        return None

def extract_headings(soup,website):
    if 'ft' in website:
        main_content = soup.find('main',id='site-content')
        if main_content:
            headings = soup.find_all(lambda tag: tag.name == 'span' and 'text--color-black' in tag.get('class', []))
            return [heading.text.strip() for heading in headings]
    else:
        headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        return [heading.text.strip() for heading in headings]    

def count_keyword_occurrences(headings, keywords):
    keyword_count = {keyword: 0 for keyword in keywords}
    for heading in headings:
        for keyword in keywords:
            if keyword.lower() in heading.lower():
                keyword_count[keyword] += 1
    return keyword_count

def monitor_keywords_on_websites(keywords, websites):
    results = []
    for website in websites:
        soup = crawl_website(website)
        if soup:
            headings = extract_headings(soup,website)
            keyword_count = count_keyword_occurrences(headings, keywords)
            for keyword, count in keyword_count.items():
                result = {
                    "term": keyword,
                    "incidence": count,
                    "site": website
                }
                results.append(result)
    json_results = json.dumps(results)
    return json_results

if __name__ == "__main__":
    keywords = ['election', 'war', 'economy']
    websites = ['https://www.ft.com/', 'https://www.theguardian.com/europe']

    monitor_keywords_on_websites(keywords, websites)