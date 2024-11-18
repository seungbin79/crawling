"""Company information processor using Perplexity and OpenAI APIs."""

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
    """Query Perplexity AI API for company information"""
    try:
        # Add your Perplexity AI API configuration here
        api_url = "https://api.perplexity.ai/chat/completions"
        headers = {
            "Authorization": f"Bearer {os.getenv('PERPLEXITY_API_KEY')}",
            "Content-Type": "application/json"
        }

        query = f"""For {company_name}, provide ONLY these details in a structured format:                            
                1. Original/Parent company name                    
                2. Parent company's country of headquarters        
                3. Industry classification:                        
                    - NAICS 3-digit code                            
                    - NAICS 4-digit code                            
                    - NAICS 5-digit code                            
                4. Annual revenue in USD (2023 or most recent)     
                                                                    
                Format response as:                                
                Original Company: [name]                           
                Parent Company: [name]                             
                Country: [country]                                 
                NAICS-3: [code]                                    
                NAICS-4: [code]                                    
                NAICS-5: [code]                                    
                Revenue: [amount in USD]                           
                                                                    
                If any information is unavailable, write 'Not Available'."""  
        
        
        payload = {
                    "model": "llama-3.1-sonar-small-128k-online",
                    "messages": [
                        {
                            "role": "system",
                            "content": "Be precise and concise."
                        },
                        {
                            "role": "user",
                            "content": query
                        }
                    ],
                    "temperature": 0.2,
                    "top_p": 0.9,
                    "return_citations": True,
                    "search_domain_filter": ["perplexity.ai"],
                    "return_images": False,
                    "return_related_questions": False,
                    "top_k": 0,
                    "stream": False,
                    "presence_penalty": 0,
                    "frequency_penalty": 1
                }

        
        response = requests.request("POST", api_url, json=payload, headers=headers)

        perplexity_response = response.json()
        
        # Initialize OpenAI client
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # Prepare the message for OpenAI
        system_prompt = """You are a data structuring assistant. Convert the provided company information into a specific JSON format. 
        Follow these rules strictly:
        - All company names should be official legal names
        - NAICS codes should include descriptions
        - Revenue should be in million USD with M suffix
        - Use "Not Available" for missing information
        - Parent company should be ultimate parent, or same as original if independent
        
        Example output:
        initial_company_name: Tesla

        {
            "initial_company_name": "Tesla",
            "original_company_name": "Tesla, Inc.",
            "parent_company": "Tesla, Inc.",
            "parent_company_country": "US",
            "industry_naics_3_digit": "336 - Transportation Equipment Manufacturing",
            "industry_naics_4_digit": "3361 - Motor Vehicle Manufacturing",
            "industry_naics_5_digit": "33611 - Automobile and Light Duty Motor Vehicle Manufacturing",
            "revenue_2023_usd": "96,773 M"
        }
        
        """
        
        user_prompt = f"""Convert this company information for {company_name} into the specified JSON format:
        
        {perplexity_response['choices'][0]['message']['content']}
        """

        print(user_prompt)
        
        # Call OpenAI API
        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.2,
            response_format={ "type": "json_object" }
        )
        
        # Parse the JSON response
        structured_data = json.loads(completion.choices[0].message.content)
        
        # Add the initial company name to the response
        structured_data["initial_company_name"] = company_name
        
        return structured_data
                                                            
    except requests.Timeout:                               
         print(f"Timeout error querying API for {company_name}")                                           
    except requests.RequestException as e:                 
        print(f"Network error querying API for {company_name}: {str(e)}")                                 
    except Exception as e:                                 
        print(f"Error processing data for {company_name}: {str(e)}")                                                 
                                                            
        # Return default values if any error occurs            
        return {                                               
            'original_company_name': 'Not Available',          
            'parent_company': 'Not Available',                 
            'parent_company_country': 'Not Available',         
            'industry_naics_3_digit': 'Not Available',         
            'industry_naics_4_digit': 'Not Available',         
            'industry_naics_5_digit': 'Not Available',         
            'revenue_2023_usd': 'Not Available'                
    }             

def process_missing_data():
    try:
        # Read the CSV file with missing data
        df = pd.read_csv('comp_missing.csv')
        
        # Find rows with missing or 'Not Available' in original_company_name
        missing_mask = (df['original_company_name'].isna()) | \
                      (df['original_company_name'] == 'Not Available') | \
                      (df['original_company_name'] == 'Not applicable')
        
        # Process each company with missing data
        for idx, row in df[missing_mask].iterrows():
            company_name = row['initial_company_name']
            print(f"Querying information for: {company_name}")
            
            # Get information from Perplexity AI
            company_info = get_company_info_from_perplexity(company_name)
            
            if company_info:
                # Update the dataframe with the retrieved information
                # You'll need to parse the API response and map it to your columns
                df.at[idx, 'initial_company_name'] = company_info.get('initial_company_name')
                df.at[idx, 'original_company_name'] = company_info.get('original_company_name')
                df.at[idx, 'parent_company'] = company_info.get('parent_company')
                df.at[idx, 'parent_company_country'] = company_info.get('parent_company_country')
                df.at[idx, 'industry_naics_3_digit'] = company_info.get('industry_naics_3_digit')
                df.at[idx, 'industry_naics_4_digit'] = company_info.get('industry_naics_4_digit')
                df.at[idx, 'industry_naics_5_digit'] = company_info.get('industry_naics_5_digit')
                df.at[idx, 'revenue_2023_usd'] = company_info.get('revenue_2023_usd')
            
            # Add a delay to respect API rate limits
            time.sleep(1)
        
        # Save the updated dataframe
        df.to_csv('company_info_updated.csv', index=False)
        print("Successfully updated company information and saved to company_info_updated.csv")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    process_missing_data()
