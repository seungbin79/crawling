# # Session Management and Dynamic Content Crawling
# # Crawl4AI excels at handling complex scenarios, such as crawling multiple pages with dynamic content loaded via JavaScript. Here's an example of crawling GitHub commits across multiple pages:

# import asyncio
# import re
# from bs4 import BeautifulSoup
# from crawl4ai import AsyncWebCrawler

# async def crawl_typescript_commits():
#     first_commit = ""
#     async def on_execution_started(page):
#         nonlocal first_commit
#         try:
#             while True:
#                 await page.wait_for_selector('li.Box-sc-g0xbh4-0 h4')
#                 commit = await page.query_selector('li.Box-sc-g0xbh4-0 h4')
#                 commit = await commit.evaluate('(element) => element.textContent')
#                 commit = re.sub(r'\s+', '', commit)
#                 if commit and commit != first_commit:
#                     first_commit = commit
#                     break
#                 await asyncio.sleep(0.5)
#         except Exception as e:
#             print(f"Warning: New content didn't appear after JavaScript execution: {e}")

#     async with AsyncWebCrawler(verbose=True) as crawler:
#         crawler.crawler_strategy.set_hook('on_execution_started', on_execution_started)

#         url = "https://github.com/microsoft/TypeScript/commits/main"
#         session_id = "typescript_commits_session"
#         all_commits = []

#         js_next_page = """
#         const button = document.querySelector('a[data-testid="pagination-next-button"]');
#         if (button) button.click();
#         """

#         for page in range(3):  # Crawl 3 pages
#             result = await crawler.arun(
#                 url=url,
#                 session_id=session_id,
#                 css_selector="li.Box-sc-g0xbh4-0",
#                 js=js_next_page if page > 0 else None,
#                 bypass_cache=True,
#                 js_only=page > 0
#             )

#             assert result.success, f"Failed to crawl page {page + 1}"

#             soup = BeautifulSoup(result.cleaned_html, 'html.parser')
#             commits = soup.select("li")
#             all_commits.extend(commits)

#             print(f"Page {page + 1}: Found {len(commits)} commits")

#         await crawler.crawler_strategy.kill_session(session_id)
#         print(f"Successfully crawled {len(all_commits)} commits across 3 pages")

# if __name__ == "__main__":
#     asyncio.run(crawl_typescript_commits())


# # This example demonstrates Crawl4AI's ability to handle complex scenarios where content is loaded asynchronously.
# # It crawls multiple pages of GitHub commits, executing JavaScript to load new content and using custom hooks to ensure data is loaded before proceeding.
# # For more advanced usage examples, check out our Examples section in the documentation.


#!/usr/bin/env python3

import asyncio
import base64
import os

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


async def analyze_image_with_gpt4(image_data):
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            # model="gpt-4-vision-preview",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "What is the company name shown in this image? Please return only the company name.",
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_data}"
                            },
                        },
                    ],
                }
            ],
            max_tokens=100,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Error analyzing image with GPT-4: {e}")
        return None


async def extract_company_names():
    print("[DEBUG] Starting extract_company_names()")
    async with AsyncWebCrawler(
        verbose=True, timeout=120000
    ) as crawler:  # 2 minute timeout
        session_id = "customer_case_study_session"
        url = "https://kinaxis.com/en/customers"
        all_companies = []

        print("[DEBUG] Starting crawl of Kinaxis customers page")
        print(f"[DEBUG] Using URL: {url}")

        for page in range(5):
            print(f"[DEBUG] Attempting to crawl page {page + 1}")
            print(f"[DEBUG] Attempting to execute crawler.arun() with parameters:")
            print(f"[DEBUG] - URL: {url}")
            print(f"[DEBUG] - Session ID: {session_id}")
            print(f"[DEBUG] - Wait for selector: .bisc-resource-content")
            print(f"[DEBUG] - Page number: {page + 1}")

            try:
                result = await crawler.arun(
                    url=url,
                    session_id=session_id,
                    wait_for="css:.bisc-resource-content",  # Try multiple possible selectors
                    js_code=(
                        "document.querySelector('.slick-next')?.click();"
                        if page > 0
                        else None
                    ),
                    css_selector=".bisc-resource-content",
                    timeout=120000,  # 2 minute timeout per request
                )
                print("[DEBUG] crawler.arun() completed")
            except Exception as e:
                print(f"[DEBUG] Exception in crawler.arun(): {str(e)}")
                print(f"[DEBUG] Exception type: {type(e).__name__}")
                raise

            if not result.success:
                print(f"[DEBUG] Page {page + 1} crawl failed")
                print(f"[DEBUG] Result status: {result.success}")
                print(
                    f"[DEBUG] Result error: {getattr(result, 'error', 'No error message')}"
                )
                print(f"페이지 {page + 1} 크롤링에 실패했습니다.")
                break

            soup = BeautifulSoup(result.cleaned_html, "html.parser")
            print(f"[DEBUG] Page HTML received, length: {len(result.cleaned_html)}")

            # Try multiple possible selectors
            cards = soup.select(".bisc-resource-content")
            print(f"[DEBUG] Found {len(cards)} image cards on page {page + 1}")

            for card in cards:
                try:
                    # Get image source URL
                    img_src = card.get("src")
                    if not img_src:
                        continue

                    # Get image data
                    img_result = await crawler.arun(
                        url=img_src, session_id=session_id, raw_response=True
                    )

                    if img_result.success and img_result.content:
                        # Convert image to base64
                        img_base64 = base64.b64encode(img_result.content).decode(
                            "utf-8"
                        )

                        # Analyze image with GPT-4
                        company_name = await analyze_image_with_gpt4(img_base64)

                        if company_name:
                            all_companies.append(company_name)
                            print(f"Company Name: {company_name}")

                except Exception as e:
                    print(f"Error processing card: {e}")
                    continue

            print(f"Page {page + 1}: Extracted {len(cards)} company names")

        await crawler.crawler_strategy.kill_session(session_id)
        print(f"총 {len(all_companies)}개의 회사명을 추출했습니다.")


if __name__ == "__main__":
    asyncio.run(extract_company_names())
