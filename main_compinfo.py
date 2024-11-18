import pandas as pd
import requests
import time

def get_company_info_from_perplexity(company_name):
    """Query Perplexity AI API for company information"""
    try:
        # Add your Perplexity AI API configuration here
        api_url = "YOUR_PERPLEXITY_API_ENDPOINT"
        headers = {
            "Authorization": "Bearer YOUR_API_KEY",
            "Content-Type": "application/json"
        }
        
        query = f"What is the parent company, country of origin, and industry details for {company_name}?"
        
        response = requests.post(
            api_url,
            headers=headers,
            json={"query": query}
        )
        
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        print(f"Error querying API for {company_name}: {str(e)}")
        return None

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
                df.at[idx, 'original_company_name'] = company_info.get('parent_company')
                df.at[idx, 'parent_company'] = company_info.get('parent_company')
                df.at[idx, 'parent_company_country'] = company_info.get('country')
                df.at[idx, 'industry_naics_3_digit'] = company_info.get('industry_3digit')
                df.at[idx, 'industry_naics_4_digit'] = company_info.get('industry_4digit')
                df.at[idx, 'industry_naics_5_digit'] = company_info.get('industry_5digit')
                df.at[idx, 'revenue_2023_usd'] = company_info.get('revenue')
            
            # Add a delay to respect API rate limits
            time.sleep(1)
        
        # Save the updated dataframe
        df.to_csv('company_info_updated.csv', index=False)
        print("Successfully updated company information and saved to company_info_updated.csv")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    process_missing_data()
