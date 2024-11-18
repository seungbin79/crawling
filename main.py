"""Kinaxis web crawler for case studies."""

import os
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


class KinaxisCrawler:
    """Crawler for extracting case studies from Kinaxis website."""

    def __init__(self):
        self.base_url = "https://www.kinaxis.com"
        self.visited_urls = set()
        self.case_studies = []
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        }

    def get_links(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a", href=True)

            valid_links = []
            for link in links:
                href = link["href"]
                full_url = urljoin(self.base_url, href)
                if (
                    full_url.startswith(self.base_url)
                    and full_url not in self.visited_urls
                ):
                    valid_links.append(full_url)

            return valid_links
        except Exception as e:
            print(f"Error getting links from {url}: {e}")
            return []

    def crawl_page(self, url, depth=0):
        if depth > 3 or url in self.visited_urls:  # 최대 깊이 제한
            return

        self.visited_urls.add(url)
        print(f"Crawling: {url}")

        try:
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, "html.parser")

            # 케이스 스터디 데이터 추출
            title = soup.find("h1").text.strip() if soup.find("h1") else ""
            content = soup.find("article").text.strip() if soup.find("article") else ""

            if title and content:
                self.case_studies.append(
                    {"url": url, "title": title, "content": content}
                )

            # 다음 링크 탐색
            links = self.get_links(url)
            for link in links:
                time.sleep(1)  # 서버 부하 방지
                self.crawl_page(link, depth + 1)

        except Exception as e:
            print(f"Error crawling {url}: {e}")

    def save_results(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"kinaxis_case_studies_{timestamp}.md"

        with open(filename, "w", encoding="utf-8") as f:
            f.write("# Kinaxis Case Studies\n\n")
            for case in self.case_studies:
                f.write(f"## {case['title']}\n\n")
                f.write(f"{case['content']}\n\n")
                f.write(f"[Source]({case['url']})\n\n")
                f.write("---\n\n")

        print(f"Results saved to {filename}")


def main():
    crawler = KinaxisCrawler()
    crawler.crawl_page("https://www.kinaxis.com/en/customers")
    crawler.save_results()


if __name__ == "__main__":
    main()
