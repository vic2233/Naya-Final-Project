import csv
import json

# Function to convert CSV to JSON
def csv_to_json(csv_file_path, json_file_path):
    data = []
    
    # Read the CSV file
    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        
        # Convert each row into a dictionary and add it to the data list
        for row in csv_reader:
            data.append(row)
    
    # Write the data to a JSON file
    with open(json_file_path, mode='w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4)

# Example usage
csv_file_path = '/home/developer/projects/FinalProject/LoadData/Customer_support_data.csv'
json_file_path = '/home/developer/projects/FinalProject/LoadData/Customer_support_data.json'
csv_to_json(csv_file_path, json_file_path)
