import json
import requests
from bs4 import BeautifulSoup
def download_page(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text
def main(url):
    content = download_page(url)
    soup = BeautifulSoup(content, 'html.parser')
    result = {}
    for row in soup.table.find_all('tr'):
        row_header = row.th.get_text()
        row_cell = row.td.get_text()
        result[row_header] = row_cell
    with open('book_table.json', 'w') as storage_file:
        storage_file.write(json.dumps(result))
if __name__ == "__main__":
    main("https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html")