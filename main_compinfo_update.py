"""Company information processor focusing on fixing names and revenue data using Perplexity and OpenAI APIs."""

import json
import os
import time
import pandas as pd
import requests
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

def get_company_info_from_perplexity(company_name):
    """Query Perplexity AI API for company information with focus on official name and revenue"""
    try:
        api_url = "https://api.perplexity.ai/chat/completions"
        headers = {
            "Authorization": f"Bearer {os.getenv('PERPLEXITY_API_KEY')}",
            "Content-Type": "application/json"
        }

        query = f"""For {company_name}, provide ONLY these details with high focus on accuracy:
                1. Official/Legal company name (full correct name)
                2. Parent company's country of headquarters
                3. Revenue in USD for 2023 (if not available, most recent year)
                
                Format response as:
                Official Name: [exact legal name]
                Country: [country]
                Revenue: [amount in USD with year]
                
                Be precise with company names and revenue figures.
                If any information is unavailable, write 'Not Available'."""

        payload = {
            "model": "llama-3.1-sonar-small-128k-online",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a corporate information specialist focused on providing accurate company names and revenue data."
                },
                {
                    "role": "user",
                    "content": query
                }
            ],
            "temperature": 0.1,
            "top_p": 0.9,
            "return_citations": True
        }

        response = requests.request("POST", api_url, json=payload, headers=headers)
        perplexity_response = response.json()

        # Initialize OpenAI client
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

        # Prepare the message for OpenAI
        system_prompt = """You are a data structuring assistant specializing in company information. 
        Convert the provided company information into a specific JSON format.
        Follow these rules strictly:
        - Company names must be official legal names
        - Revenue should be in million USD with M suffix
        - Use "Not Available" for missing information
        - If revenue year is not 2023, include the year in parentheses
        
        Example output:
        {
            "original_company_name": "Tesla, Inc.",
            "parent_company_country": "US",
            "revenue_2023_usd": "96,773 M"
        }
        """

        user_prompt = f"""Convert this company information for {company_name} into the specified JSON format:
        
        {perplexity_response['choices'][0]['message']['content']}
        """

        # Call OpenAI API
        completion = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,
            response_format={ "type": "json_object" }
        )

        # Parse the JSON response
        structured_data = json.loads(completion.choices[0].message.content)
        return structured_data

    except Exception as e:
        print(f"Error processing data for {company_name}: {str(e)}")
        return {
            'original_company_name': 'Not Available',
            'parent_company_country': 'Not Available',
            'revenue_2023_usd': 'Not Available'
        }

def process_company_data():
    try:
        # Read the CSV file
        df = pd.read_csv('company_info_updated.csv')
        
        # Create a copy for updated data
        updated_df = df.copy()
        
        # Process each company
        for idx, row in df.iterrows():
            company_name = row['initial_company_name']
            
            # Check if name needs fixing or revenue is missing/incorrect
            if pd.isna(row['original_company_name']) or \
               row['original_company_name'] == 'Not Available' or \
               pd.isna(row['revenue_2023_usd']) or \
               row['revenue_2023_usd'] == 'Not Available' or \
               not str(row['revenue_2023_usd']).endswith('M'):
                
                print(f"Processing: {company_name}")
                
                # Get updated information
                company_info = get_company_info_from_perplexity(company_name)
                
                if company_info:
                    # Update the dataframe
                    updated_df.at[idx, 'original_company_name'] = company_info.get('original_company_name')
                    updated_df.at[idx, 'parent_company_country'] = company_info.get('parent_company_country')
                    updated_df.at[idx, 'revenue_2023_usd'] = company_info.get('revenue_2023_usd')
                
                # Add delay to respect API rate limits
                time.sleep(1)
        
        # Save to Excel file
        output_file = 'company_info_corrected.xlsx'
        updated_df.to_excel(output_file, index=False)
        print(f"Successfully updated company information and saved to {output_file}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    process_company_data()
