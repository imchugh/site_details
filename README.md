# site_details

This repository contains a simple script (sheets_site_details.py) to access the data in the TERN-DSA Google Sheet 'sensors-and-platforms'. The simple class contains the relevant site data from the sheet in a dataframe that can be accessed as a class attribute, and that has a method to write the data to an excel spreadsheet. Additional functionality to be added as required / requested.

For users of the Anaconda Python distribution, a yaml file containing the requisite packages is included.

While the sheet is unrestricted access for viewing, the user requires a Google service account, and must login to the cloud console (https://console.cloud.google.com) in order to:
  1) create the service account
  2) enable the Sheets and Drive APIs
  3) generate a json security key (which must reside in the root repository directory and be named 'client_secrets.json') 

A useful description of how to create the service account and generate keys can be found here: https://pyshark.com/google-sheets-api-using-python/
