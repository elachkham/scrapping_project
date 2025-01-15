import pandas as pd
import glob
import os


folder_path = "C:/Users/khali/OneDrive/Documents/Big data/web_scraping/clean data"

file_paths = glob.glob(os.path.join(folder_path, '*.xlsx'))

if not file_paths:
    print("no files")
else:
    
    dataframes = []

    for file_path in file_paths:
        try:
            df = pd.read_excel(file_path)
            
            columns_to_remove = ['article_content', 'article_raw_content']
            df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
            
            site_id = os.path.splitext(os.path.basename(file_path))[0]
            df['site_id'] = site_id

            date_columns = ['article_date_published', 'article_date_modified', 'scraping_date']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y%m%d%H%M%S')
            
            dataframes.append(df)
        except Exception as e:
            print(f"Error lecture {file_path}: {e}")

    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        output_file = os.path.join(folder_path, 'combined_data1.xlsx')
        combined_df.to_excel(output_file, index=False)
        
        print(f"file saved : {output_file}")
    else:
        print("no valide data")
