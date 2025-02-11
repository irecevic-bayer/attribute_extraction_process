import pandas as pd
import re
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('./google-credentials.json')

# Function to extract product details in multiple languages
def extract_multilingual_details(product_name):
    # Patterns for extraction (English, German, Spanish, French, Italian terms)
    volume_pattern = r'(\d+\.?\d*)\s?(ml|mL|l|L|g|mg|kg|\u00b5g|oz|gr|Gramm|Liter|Milliliter|mililitros|litro|gramo|milligramme|litre|gramme|millilitro|litri|grammi)'
    count_pattern = r'(\d+)\s?(tablet|capsule|caps|bottle|pack|vial|drop|effervescence|karton|count|piece|pieces|Tabletten|Kapseln|Flasche|Packung|Ampullen|Tropfen|St\u00fcck|tableta|c\u00e1psula|botella|paquete|vial|gota|effervescente|cart\u00f3n|comprim\u00e9|capsule|bouteille|paquet|flacon|goutte|compressa|capsula|bottiglia|confezione|fiala|goccia)'
    dosage_pattern = r'(\d+\.?\d*)\s?(mg|g|gr|\u00b5g|Milligramm|Gramm|Mikrogramm|miligramos|gramos|microgramos|milligramme|gramme|microgramme|milligrammo|grammo|microgrammo)'
    packaging_pattern = r'\b(tablet|capsule|drop|effervescence|bottle|vial|karton|pack|count|piece|pieces|Tabletten|Kapseln|Flasche|Packung|Ampulle|Tropfen|St\u00fcck|tableta|c\u00e1psula|botella|paquete|vial|gota|effervescente|cart\u00f3n|comprim\u00e9|capsule|bouteille|paquet|flacon|goutte|compressa|capsula|bottiglia|confezione|fiala|goccia)\b'
    multiplication_pattern = r'(\d+)x(\d+\.?\d*)\s?(mg|g|gr|\u00b5g)'

    # Initialize extracted fields
    volume, volume_unit = None, None
    count, count_unit = None, None
    dosage, dosage_unit = None, None
    packaging_type = None

    # Extract multiplication dosage (e.g., 3x3.5g → 10.5g)
    multiplication_match = re.search(multiplication_pattern, product_name, re.IGNORECASE)
    if multiplication_match:
        num_units, unit_weight, unit = multiplication_match.groups()
        total_weight = float(num_units) * float(unit_weight)
        dosage, dosage_unit = str(total_weight), unit
    else:
        # Extract dosage normally
        dosage_match = re.search(dosage_pattern, product_name, re.IGNORECASE)
        if dosage_match:
            dosage, dosage_unit = dosage_match.groups()

    # Extract volume
    volume_match = re.search(volume_pattern, product_name, re.IGNORECASE)
    if volume_match:
        volume, volume_unit = volume_match.groups()

    # Extract count
    count_match = re.search(count_pattern, product_name, re.IGNORECASE)
    if count_match:
        count, count_unit = count_match.groups()

    # Extract packaging type
    packaging_match = re.search(packaging_pattern, product_name, re.IGNORECASE)
    if packaging_match:
        packaging_type = packaging_match.group(1)

    return pd.Series({
        'Volume': volume,
        'Volume_Unit': volume_unit,
        'Count': count,
        'Count_Unit': count_unit,
        'Dosage': dosage,
        'Dosage_Unit': dosage_unit,
        'Packaging_Type': packaging_type
    })

# Connect to BigQuery and load data
def load_data_from_bigquery(query, project_id):
    client = bigquery.Client(project=project_id,credentials=credentials)
    query_job = client.query(query)
    results = query_job.to_dataframe()
    return results

# Save results back to BigQuery
def save_results_to_bigquery(dataframe, table_id, project_id):
    client = bigquery.Client(project=project_id,credentials=credentials)
    job = client.load_table_from_dataframe(dataframe, table_id)
    job.result()  # Wait for the job to complete
    print(f"Data saved to {table_id}")

# Compare results with previous data
def compare_with_previous_results(new_data, table_id, project_id):
    client = bigquery.Client(project=project_id,credentials=credentials)
    previous_data_query = f"SELECT * FROM `{table_id}`"
    previous_data = client.query(previous_data_query).to_dataframe()

    # Perform comparison (example: find new rows)
    differences = new_data.merge(previous_data, how='outer', indicator=True)
    new_entries = differences[differences['_merge'] == 'left_only']

    print(f"Found {len(new_entries)} new entries.")
    return new_entries

# Main function to process data
def process_bigquery_data(input_query, output_table, project_id):
    # Load data from BigQuery
    data = load_data_from_bigquery(input_query, project_id)

    # Ensure the 'name' column is properly formatted
    data['name'] = data['name'].fillna("").astype(str)

    # Apply the extraction process
    extracted_data = data['name'].apply(extract_multilingual_details)

    # Combine the original data with the extracted information
    processed_data = pd.concat([data, extracted_data], axis=1)

    # Compare with previous results
    new_entries = compare_with_previous_results(processed_data, output_table, project_id)

    # Save new entries back to BigQuery
    if not new_entries.empty:
        save_results_to_bigquery(new_entries, output_table, project_id)

# Example usage
input_query = "SELECT DISTINCT country_code, ASIN, name FROM `bayer-ch-ecommerce.ch_h10.fact_profitero_sns`"
output_table = "bayer-ch-ecommerce.ch_ecommerce_eu5.fact_product_attributes"
project_id = "bayer-ch-ecommerce"
process_bigquery_data(input_query, output_table, project_id)
