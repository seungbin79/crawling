"""Kinaxis case study scraper with pagination support."""

import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


def scrape_kinaxis_case_studies():
    """
    Scrapes customer case studies from Kinaxis website with pagination
    """
    base_url = "https://www.kinaxis.com/en/customers"
    all_case_studies = []
    page = 1
    total_resources = 0

    try:
        while True:
            print(f"Scraping page {page}...")

            # Add page parameter if not first page
            url = f"{base_url}?page={page}" if page > 1 else base_url

            # Send GET request with headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            }

            response = requests.get(url, headers=headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Find all case study cards
            case_study_cards = soup.find_all("div", class_="card")

            if not case_study_cards:
                break

            # Extract information from each card
            for card in case_study_cards:
                try:
                    # Find title and description
                    title_element = card.find("h3")
                    description_element = card.find("p")

                    if title_element and description_element:
                        case_study = {
                            "company_name": title_element.text.strip(),
                            "description": description_element.text.strip(),
                            "page_number": page,
                        }
                        all_case_studies.append(case_study)
                except Exception as e:
                    print(f"Error processing card: {e}")
                    continue

            # Check if there's a next page by looking for the 'Next' button
            next_button = soup.find("a", string="Next")
            if not next_button:
                break

            page += 1
            # Add delay to be respectful to the server
            time.sleep(2)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")

    # Create DataFrame
    df = pd.DataFrame(all_case_studies)

    # Save to both CSV and Excel for convenience
    df.to_csv("kinaxis_case_studies.csv", index=False, encoding="utf-8")
    df.to_excel("kinaxis_case_studies.xlsx", index=False)

    return df


def main():
    print("Starting Kinaxis case studies scraping...")
    df = scrape_kinaxis_case_studies()

    if not df.empty:
        print("\nScraping completed successfully!")
        print(f"Total case studies found: {len(df)}")
        print("\nCase Studies Overview:")
        for idx, row in df.iterrows():
            print(f"\nCompany: {row['company_name']}")
            print(f"Description: {row['description'][:100]}...")
            print("-" * 80)
    else:
        print("No case studies were found.")


if __name__ == "__main__":
    main()
