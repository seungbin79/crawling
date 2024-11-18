"""Extended Kinaxis crawler with OpenAI integration."""

import csv
import json
import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin

import openai
import requests
from bs4 import BeautifulSoup


class KinaxisCrawler:
    def __init__(self):
        self.base_url = "https://www.kinaxis.com"
        self.visited_urls = set()
        self.case_studies = []
        self.extracted_data = []
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        }
        # Set your OpenAI API key
        openai.api_key = os.getenv(
            "OPENAI_API_KEY"
        )  # Make sure to set this environment variable

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

    def extract_information(self, content, url):
        """Extract structured information from case study content.
        
        Args:
            content (str): The case study content text
            url (str): Source URL of the content
            
        Returns:
            dict: Extracted information in structured format
        """
        try:
            prompt = (
                "다음 텍스트에서 아래 정보를 추출해 주세요:\n\n"
                f"텍스트:\n{content}\n\n"
                "추출할 정보 (JSON 형식):\n"
                "{\n"
                '  "고객사정보": "",\n'
                '  "적용솔루션 툴": "",\n'
                '  "적용솔루션시점": "",\n'
                '  "URL 등록시점": "",\n'
                '  "URL 주소": ""\n'
                "}"
            )
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=500,
            )
            result = response["choices"][0]["message"]["content"]
            extracted_info = json.loads(result)
            extracted_info["URL 주소"] = url  # Add the URL to the extracted info
            return extracted_info
        except Exception as e:
            print(f"Error extracting information: {e}")
            return None

    def save_content_as_markdown(self, title, content, url):
        # Sanitize the title to create a valid filename
        filename = re.sub(r'[\\/*?:"<>|]', "", title) + ".md"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"# {title}\n\n")
            f.write(f"{content}\n\n")
            f.write(f"[Source]({url})\n")
        print(f"Content saved to {filename}")

    def crawl_page(self, url, depth=0):
        if depth > 3 or url in self.visited_urls:  # Maximum depth limit
            return

        self.visited_urls.add(url)
        print(f"Crawling: {url}")

        try:
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, "html.parser")

            # Extract case study data
            title = soup.find("h1").text.strip() if soup.find("h1") else ""
            content = soup.find("article").text.strip() if soup.find("article") else ""

            if title and content:
                self.case_studies.append(
                    {"url": url, "title": title, "content": content}
                )

                # Use LLM to extract information
                extracted_info = self.extract_information(content, url)
                if extracted_info:
                    self.extracted_data.append(extracted_info)

                # Save content as markdown file
                self.save_content_as_markdown(title, content, url)

            # Explore next links
            links = self.get_links(url)
            for link in links:
                time.sleep(1)  # Prevent server overload
                self.crawl_page(link, depth + 1)

        except Exception as e:
            print(f"Error crawling {url}: {e}")

    def save_results(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save case studies as a markdown file
        filename = f"kinaxis_case_studies_{timestamp}.md"
        with open(filename, "w", encoding="utf-8") as f:
            f.write("# Kinaxis Case Studies\n\n")
            for case in self.case_studies:
                f.write(f"## {case['title']}\n\n")
                f.write(f"{case['content']}\n\n")
                f.write(f"[Source]({case['url']})\n\n")
                f.write("---\n\n")
        print(f"Results saved to {filename}")

        # Save extracted data as CSV
        data_filename = f"extracted_data_{timestamp}.csv"
        keys = [
            "고객사정보",
            "적용솔루션 툴",
            "적용솔루션시점",
            "URL 등록시점",
            "URL 주소",
        ]
        with open(data_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            writer.writeheader()
            for data in self.extracted_data:
                writer.writerow(data)
        print(f"Extracted data saved to {data_filename}")


def main():
    crawler = KinaxisCrawler()
    crawler.crawl_page("https://www.kinaxis.com/en/customers")
    crawler.save_results()


if __name__ == "__main__":
    main()
