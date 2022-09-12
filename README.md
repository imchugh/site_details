# site_details

This repository contains scripts to access TERN / OzFlux flux site information, via two methods: 1) accessing the TERN-DSA Google Sheet 'sensors-and-platforms', and; 2) accessing the TERN-DSA SPARQL endpoint. The latter is preferred, since the service account for the former (see below) limits the number of times it can be accessed.

While the sheet is unrestricted access for viewing, the user requires a Google service account, and must login to the cloud console (https://console.cloud.google.com) in order to:
  1) create the service account
  2) enable the Sheets and Drive APIs
  3) generate a json security key (which must reside in the root repository directory and be named 'client_secrets.json') 

A useful description of how to create the service account and generate keys can be found here (see 'Creating Google API credentials'): https://pyshark.com/google-sheets-api-using-python/

While the external access methods are different, the class that contains the data is essentially similar. It contains the relevant site data in a dataframe that can be accessed as a class attribute, and that has a method to write the data to an excel spreadsheet. Additional functionality to be added as required / requested.
