#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 30 17:55:20 2022

@author: imchugh
"""

import datetime as dt
import gspread
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import pathlib
from timezonefinder import TimezoneFinder
from pytz import timezone
import pdb

#------------------------------------------------------------------------------
### CONSTANTS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
CRED_FILE = 'client_secrets.json'
SHEET_KEY = '19RUT2otvKF6sgk-ShxZHlSJSJyl74QMBMi6runm4Bd8'
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
    ]
ALIAS_DICT = {'Alpine Peatland': 'Alpine Peat',
              'ArcturusEmerald': 'Emerald',
              'Calperum Chowilla': 'Calperum',
              'Dargo High Plains': 'Dargo',
              'Longreach Mitchell Grass Rangeland': 'Longreach',
              'Nimmo High Plains': 'Nimmo',
              'Samford Ecological Research Facility': 'Samford'}
SUBSET_LIST = ['latitude', 'longitude', 'elevation', 'time_zone', 'GMT_zone',
               'date_commissioned', 'date_decommissioned', 'is_decommissioned']

# See below for complete column list
# SUBSET_LIST = ['persistent_uri', 'ozflux_id', 'fluxnet_id', 'is_hosted_by',
#                'igbp_code', 'climate_code', 'tower_type', 
#                'date_commissioned', 'date_decommissioned', 
#                'is_decommissioned', 'purpose', 'description', 'latitude', 
#                'longitude', 'elevation', 'terrain_type',
#                'power_supply_type', 'land_ownership_type', 'land_owner',
#                'funding_organization', 'owner_organization', 'managed_by',
#                'collaborator_organizations', 'project_manager',
#                'principal_investigator', 'technician', 'data_manager', 
#                'collaborators',
#                'time_zone', 'GMT_zone']
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### FUNCTIONS ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_frame_from_sheets(use_alias=True):
    """

    Read Google sheet and return pandas dataframe
    
    Parameters
    ----------
    use_alias : Boolean, optional
        Forces the dataframe index to use the EPCN names for the 
        sites. The default is True.

    Returns
    -------
    df : pandas dataframe
        Dataframe containing the fileds of the Google sheet 
        platforms-and-sensors.

    """
    
    credentials = (
        ServiceAccountCredentials.from_json_keyfile_name(CRED_FILE, SCOPES)
        )
    file = gspread.authorize(credentials)
    book = file.open_by_key(key=SHEET_KEY)
    sheet = book.worksheet(title='Flux Towers')
    df = pd.DataFrame(sheet.get_all_records())
    df.replace('', np.nan, inplace=True)
    df = df[~pd.isnull(df.name)]
    df.name = [x.replace('Flux Station', '').strip() for x in df.name]
    df = df.assign(new_name = lambda x: x.name.map(ALIAS_DICT).fillna(x.name))
    df.index = [''.join(x.split(' ')) for x in df.new_name]
    df.drop(['name', 'new_name'], axis=1, inplace=True)
    df = df.assign(time_zone = _get_timezones(df))
    df = df.assign(GMT_zone = _get_GMT_offset(df))
    df['is_decommissioned'] = df.is_decommissioned=='TRUE'
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_timezones(df):
    
    """Get the timezone (as region/city)"""
    
    tf = TimezoneFinder()
    tz_list = []
    for site in df.index:
        try: 
            tz = tf.timezone_at(
                lng=df.loc[site, 'longitude'], 
                lat=df.loc[site, 'latitude']
                )
        except ValueError:
            tz = np.nan
        tz_list.append(tz)
    return tz_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_GMT_offset(df):
    
    """Get the UTC offset (local standard time)"""
    
    offset_list = []
    date = dt.datetime.now()
    for site in df.index:
        try:
            tz_obj = timezone(df.loc[site, 'time_zone'])
            utc_offset = tz_obj.utcoffset(date)
            utc_offset -= tz_obj.dst(date)
            utc_offset = utc_offset.seconds / 3600
        except AttributeError:
            utc_offset = np.nan
        offset_list.append(utc_offset)
    return offset_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
### CLASSES ###
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
class site_details():
    
    """Class to retrieve site data from Google sheet sensors-and-platforms"""
    
    def __init__(self, use_alias=True):
        
        self.df = _get_frame_from_sheets(use_alias=use_alias)
        
    def export_to_excel(self, path, subset_cols=SUBSET_LIST, 
                        operational_sites_only=True):
        
        """

        Parameters
        ----------
        path : str
            Outut path for excel spreadsheet.
        subset_cols : list, optional
            List of columns to output to file. The default is SUBSET_LIST.
        operational_sites_only : Boolean, optional
            Drop non-operational sites. The default is True.

        Returns
        -------
        None.

        """
        
        if operational_sites_only:
            df = self.get_operational_sites()
        else:
            df = self.df
        if not subset_cols:
            df.to_excel(path)
            return
        cols = [x for x in subset_cols if x in df.columns]
        df.to_excel(path, columns=cols, index_label='Site')    
        
    def get_operational_sites(self):
        
        """
        Get the operational subset of sites.

        Returns
        -------
        pandas dataframe
            Dataframe containing information only for operational sites.

        """
        
        return (
            self.df[~self.df.is_decommissioned].drop(
                ['date_decommissioned', 'is_decommissioned'], axis=1)
            )         
#------------------------------------------------------------------------------        

#------------------------------------------------------------------------------