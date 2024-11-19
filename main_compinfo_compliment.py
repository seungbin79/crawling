"""Company information processor focusing on NAICS codes and revenue data using Perplexity and OpenAI APIs."""

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
    """Query Perplexity AI API for detailed NAICS codes and revenue information"""
    try:
        api_url = "https://api.perplexity.ai/chat/completions"
        headers = {
            "Authorization": f"Bearer {os.getenv('PERPLEXITY_API_KEY')}",
            "Content-Type": "application/json"
        }

        query = f"""For {company_name}, provide these details with strict NAICS code formatting:
                1. NAICS codes with descriptions (MUST follow exact digit requirements):
                   - EXACTLY 3-digit code (e.g., '334') with description
                   - EXACTLY 4-digit code (e.g., '3341') with description
                   - EXACTLY 5-digit code (e.g., '33411') with description
                2. Revenue:
                   - Most recent annual revenue (specify year)
                   - If 2023 not available, provide latest year
                   - Convert all amounts to USD
                
                Format response EXACTLY as:
                NAICS-3: [3 digits only] - [description]
                NAICS-4: [4 digits only] - [description]
                NAICS-5: [5 digits only] - [description]
                Revenue: [amount] USD ([year])
                
                Rules for NAICS codes:
                - 3-digit must be EXACTLY 3 digits
                - 4-digit must be EXACTLY 4 digits
                - 5-digit must be EXACTLY 5 digits
                - Each code must be properly nested (4-digit must start with same digits as 3-digit)
                If any information is unavailable, write 'Not Available'."""

        payload = {
            "model": "llama-3.1-sonar-small-128k-online",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a corporate information specialist focused on providing accurate NAICS classifications and revenue data."
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
        system_prompt = """You are a data structuring assistant specializing in NAICS codes and revenue data. 
        Convert the provided company information into a specific JSON format.
        Follow these rules strictly:
        - NAICS codes MUST be exact lengths (3, 4, or 5 digits) with descriptions
        - 3-digit code must be exactly 3 digits (e.g., "334")
        - 4-digit code must be exactly 4 digits (e.g., "3341")
        - 5-digit code must be exactly 5 digits (e.g., "33411")
        - Revenue should be in million USD with M suffix
        - Include the year for revenue in parentheses
        - Use "Not Available" for missing information
        - Ensure NAICS codes are properly nested (4-digit must start with same digits as 3-digit)
        
        Example output:
        {
            "industry_naics_3_digit": "334 - Computer and Electronic Product Manufacturing",
            "industry_naics_4_digit": "3341 - Computer and Peripheral Equipment Manufacturing",
            "industry_naics_5_digit": "33411 - Computer and Peripheral Equipment Manufacturing",
            "revenue_latest": "96,773 M (2023)"
        }
        """

        user_prompt = f"""Convert this company information for {company_name} into the specified JSON format:
        
        {perplexity_response['choices'][0]['message']['content']}
        """

        # Call OpenAI API
        completion = client.chat.completions.create(
            model="gpt-4o",
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
            'industry_naics_3_digit': 'Not Available',
            'industry_naics_4_digit': 'Not Available',
            'industry_naics_5_digit': 'Not Available',
            'revenue_latest': 'Not Available'
        }

def validate_naics_code(code, expected_length):
    """Validate NAICS code format and length"""
    if code == 'Not Available':
        return True
    try:
        # Remove description part if present
        code_part = code.split(' - ')[0].strip()
        # Check if it's exactly the expected length and all digits
        return len(code_part) == expected_length and code_part.isdigit()
    except:
        return False

def process_company_data():
    try:
        # Read the Excel file
        df = pd.read_excel('company_info_updated_o9.xlsx')
        
        # Create a copy for updated data
        updated_df = df.copy()
        
        # Process each company
        for idx, row in df.iterrows():
            company_name = row['initial_company_name']
            
            # Check if NAICS codes or revenue need updating
            needs_update = False
            
            # Validate NAICS codes
            if (pd.isna(row['industry_naics_3_digit']) or 
                not validate_naics_code(str(row['industry_naics_3_digit']), 3)):
                needs_update = True
            
            if (pd.isna(row['industry_naics_4_digit']) or 
                not validate_naics_code(str(row['industry_naics_4_digit']), 4)):
                needs_update = True
            
            if (pd.isna(row['industry_naics_5_digit']) or 
                not validate_naics_code(str(row['industry_naics_5_digit']), 5)):
                needs_update = True
            
            # Check revenue
            if (row['revenue_2023_usd'] == 'Not Available' or 
                pd.isna(row['revenue_2023_usd'])):
                needs_update = True
                
            if needs_update:
                
                print(f"Processing: {company_name}")
                
                # Get updated information
                company_info = get_company_info_from_perplexity(company_name)
                
                if company_info:
                    # Update NAICS codes if they were missing or incorrect
                    if pd.isna(row['industry_naics_3_digit']) or row['industry_naics_3_digit'] == 'Not Available':
                        updated_df.at[idx, 'industry_naics_3_digit'] = company_info.get('industry_naics_3_digit')
                    if pd.isna(row['industry_naics_4_digit']) or row['industry_naics_4_digit'] == 'Not Available':
                        updated_df.at[idx, 'industry_naics_4_digit'] = company_info.get('industry_naics_4_digit')
                    if pd.isna(row['industry_naics_5_digit']) or row['industry_naics_5_digit'] == 'Not Available':
                        updated_df.at[idx, 'industry_naics_5_digit'] = company_info.get('industry_naics_5_digit')
                    
                    # Update revenue if it was missing
                    if pd.isna(row['revenue_2023_usd']) or row['revenue_2023_usd'] == 'Not Available':
                        updated_df.at[idx, 'revenue_2023_usd'] = company_info.get('revenue_latest')
                
                # Add delay to respect API rate limits
                time.sleep(1)
        
        # Save to Excel file
        output_file = 'company_info_o9_naics_revenue_updated.xlsx'
        updated_df.to_excel(output_file, index=False)
        print(f"Successfully updated company information and saved to {output_file}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    process_company_data()
