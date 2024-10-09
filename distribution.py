import pandas as pd
import os
import chardet
import csv
from collections import Counter

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        raw_data = file.read(10000)
    return chardet.detect(raw_data)['encoding']

def read_csv_with_encoding(file_path):
    encodings_to_try = ['utf-8', 'iso-8859-1', 'windows-1252', 'latin1']
    
    detected_encoding = detect_encoding(file_path)
    encodings_to_try.insert(0, detected_encoding)
    
    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(file_path, 
                             encoding=encoding, 
                             on_bad_lines='skip', 
                             low_memory=False,
                             quoting=csv.QUOTE_MINIMAL,
                             quotechar='"',
                             escapechar='\\')
            print(f"Successfully read file with {encoding} encoding.")
            return df
        except UnicodeDecodeError:
            print(f"Failed to read with {encoding} encoding. Trying next...")
        except Exception as e:
            print(f"An unexpected error occurred with {encoding} encoding: {str(e)}")
    
    raise ValueError("Failed to read the CSV file with any of the attempted encodings.")

def count_occurrences(df, column):
    return Counter(df[column].astype(str).str.split(',').explode())

def write_counts_to_csv(imdb_counts, omdb_counts, filename, column_name):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([column_name, "IMDB Count", "OMDB Count"])
        
        all_values = set(imdb_counts.keys()) | set(omdb_counts.keys())
        
        for value in all_values:
            imdb_count = imdb_counts.get(value, 0)
            omdb_count = omdb_counts.get(value, 0)
            writer.writerow([value, imdb_count, omdb_count])

def calculate_m_to_n_score(imdb_unique, omdb_unique, total_unique):
    overlap = imdb_unique + omdb_unique - total_unique
    
    max_overlap = min(imdb_unique, omdb_unique)
    
    if max_overlap == 0:
        return 0
    
    score = overlap / max_overlap
    return score

def analyze_column(imdb_df, omdb_df, column):
    print(f"\nAnalyzing column: {column}")
    
    imdb_column = column
    omdb_column = column.capitalize()
    
    if imdb_column in imdb_df.columns and omdb_column in omdb_df.columns:
        imdb_counts = count_occurrences(imdb_df, imdb_column)
        omdb_counts = count_occurrences(omdb_df, omdb_column)
        
        output_filename = f'{column}_counts.csv'
        write_counts_to_csv(imdb_counts, omdb_counts, output_filename, column.capitalize())
        print(f"Counts for {column} have been written to '{output_filename}'")
        
        imdb_unique = len(imdb_counts)
        omdb_unique = len(omdb_counts)
        total_unique = len(set(imdb_counts.keys()) | set(omdb_counts.keys()))
        
        print(f"Total unique {column} combinations in IMDB: {imdb_unique}")
        print(f"Total unique {column} combinations in OMDB: {omdb_unique}")
        print(f"Total unique {column} combinations across both datasets: {total_unique}")
        
        m_to_n_score = calculate_m_to_n_score(imdb_unique, omdb_unique, total_unique)
        print(f"M-to-N score: {m_to_n_score:.4f}")
        
        return imdb_unique, omdb_unique, total_unique, m_to_n_score
    else:
        print(f"Column {column} not found in one or both datasets. Skipping.")
        return 0, 0, 0, 0
    
current_dir = os.getcwd()
imdb_path = os.path.join(current_dir, 'imdb.csv')
omdb_path = os.path.join(current_dir, 'omdb.csv')

imdb_df = read_csv_with_encoding(imdb_path)
omdb_df = read_csv_with_encoding(omdb_path)

potential_join_columns = ['title', 'director', 'cast', 'writer', 'genre', 'year']

join_candidates = []

for column in potential_join_columns:
    imdb_unique, omdb_unique, total_unique, m_to_n_score = analyze_column(imdb_df, omdb_df, column)
    
    if imdb_unique > 1 and omdb_unique > 1:
        join_candidates.append((column, imdb_unique, omdb_unique, total_unique, m_to_n_score))

print("\nPotential join columns ranked by M-to-N score:")
join_candidates.sort(key=lambda x: x[4], reverse=True)
for column, imdb_unique, omdb_unique, total_unique, m_to_n_score in join_candidates:
    print(f"{column}: IMDB unique: {imdb_unique}, OMDB unique: {omdb_unique}, Total unique: {total_unique}, M-to-N score: {m_to_n_score:.4f}")
    
def count_rows(df):
    return len(df)

imdb_rows = count_rows(imdb_df)
omdb_rows = count_rows(omdb_df)

print(f"\nNumber of rows in IMDB dataset: {imdb_rows}")
print(f"Number of rows in OMDB dataset: {omdb_rows}")

print("\nProcessing complete. Check the generated CSV files for detailed counts.")