# GCP Success Factors Ingestion Framework

## Pre-requisites

1. You have a Success Factors API endpoint and basic auth (user/password)
2. You have a GCP project the BigQuery, Cloud Storage and Data Fusion APIs activated
3. You have a comopute platform where to run a Docker application (local, Cloud Run, VM, etc)
4. The traffic between your compute platform and the Success Factors API is opened
5. A Service Account with proper lñevel of access to BigQuery and GCS in a GCP project

## 

This is a Python module which leverages the Success Factors REST API to:
1. Pulls the metadata of the entity that wants to be ingested using the metadata endpoint of the ODATA API: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/505856f7d9814f76a8894ec4f0d9e16e.html
2. Creates a BigQuery dataset (if not exists) corresponding to the Entity's module in Success Factors (employee central, learning, recruiting, etc)
3. Creates a BigQuery table inside the dataset created at the point number two
4. Downloads the historical data of the SuccessFactors entity using a query operation to the ODATA API. The data is paginated in one thousand lenght pages and downloaded into local filesystem in new line JSON format: https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/dc29c875b2934f82a551bbbfa6d546f2.html
5. Uploads the resulting JSON files into a GCS bucket
6. Builds and triggers a BigQuery Load Job to load the JSON data into a temporary table, loading all the columns as string
7. reated table at the point number 3
8. 


## Success Factors documentation
https://help.sap.com/docs/SAP_SUCCESSFACTORS_PLATFORM/d599f15995d348a1b45ba5603e2aba9b/03e1fc3791684367a6a76a614a2916de.html
