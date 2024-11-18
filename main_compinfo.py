import pandas as pd

def convert_excel_to_csv():
    # Read the Excel file
    try:
        # Read the 'result' sheet from the Excel file
        df = pd.read_excel('company_info.xlsx', sheet_name='result')
        
        # Save to CSV file
        output_filename = 'company_info_result.csv'
        df.to_csv(output_filename, index=False)
        print(f"Successfully converted 'result' sheet to {output_filename}")
        
    except FileNotFoundError:
        print("Error: company_info.xlsx file not found")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    convert_excel_to_csv()
