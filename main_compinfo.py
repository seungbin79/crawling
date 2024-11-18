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
        
        response = requests.post(
            api_url,
            headers=headers,
            json={"query": query},
            timeout=30
        )
        
        if response.status_code == 200:
            response_data = response.json()                
                                                            
             # Extract and validate the data                
            company_info = {
                 'original_company_name': str(response_data.get('Original Company', 'Not Available')).strip(),                                      
                 'parent_company': str(response_data.get('Parent Company', 'Not Available')).strip(),                                      
                 'parent_company_country': str(response_data.get('Country', 'Not Available')).strip(),
                 'industry_naics_3_digit': str(response_data.get('NAICS-3', 'Not Available')).strip(),
                 'industry_naics_4_digit': str(response_data.get('NAICS-4', 'Not Available')).strip(),
                 'industry_naics_5_digit': str(response_data.get('NAICS-5', 'Not Available')).strip(),
                 'revenue_2023_usd': str(response_data.get('Revenue', 'Not Available')).strip()
             }                                              
                                                            
             # Validate NAICS codes are numeric             
            for key in ['industry_naics_3_digit', 'industry_naics_4_digit', 'industry_naics_5_digit']:       
                if company_info[key] != 'Not Available':   
                    if not company_info[key].isdigit():    
                        company_info[key] = 'Not Available'
                                                            
            return company_info                            
                                                            
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
