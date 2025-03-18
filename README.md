# JSON Flattener for Google Cloud Functions

## Overview

This repository contains a Google Cloud Function that flattens complex JSON files, cleans column names, enforces schema, and loads structured data into BigQuery. Supports JSON with nested lists and dictionaries, handles case-insensitive duplicate columns, and auto-creates tables.

## Features

- Reads JSON files from Google Cloud Storage.
- Flattens complex JSON structures, including nested lists and dictionaries.
- Cleans column names by removing special characters.
- Handles duplicate column names due to case insensitivity.
- Enforces schema before loading data into BigQuery.
- Automatically creates tables in BigQuery if they do not exist.
- Optimized for Google Cloud Functions.

## Installation

1. Clone the repository:
   ```sh
   git clone <repo_url>
   cd <repo_name>
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## Deployment

### Prerequisites

- Google Cloud SDK installed and authenticated.
- Google Cloud Project with BigQuery and Cloud Storage enabled.
- Required environment variables set up.

### Deploy to Google Cloud Functions

Run the following command to deploy:

```sh
gcloud functions deploy flatten_json \
    --runtime python310 \
    --trigger-http \
    --allow-unauthenticated \
    --set-env-vars DATASET_NAME=<dataset_name>
```

## Usage

1. Upload a JSON file to the Cloud Storage.
2. The Cloud Function is triggered, processes the file, and loads structured data into BigQuery.
3. Verify the table in BigQuery.

## Configuration

Set the following environment variables:

- `DATASET_NAME`: The BigQuery dataset where tables should be created.

## Contributing

Contributions are welcome! Please submit issues and pull requests for enhancements.

## License

This project is licensed under the MIT License.

## Author

Farhhan Adil

