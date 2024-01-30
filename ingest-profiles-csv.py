"""
This script imports the User Profiles from the Yinzcam API, and stores them in the ADL
in CSV format. Imported fields include: 'yinzid', 'email', 'first_name', 'last_name',
'id_global', 'firstLogin', 'lastLogin, 'clientId'. The data is imported to the Azure
Data Lake, to the following file path: '/raw_data/yinzcam/{team}_tor/users/'.

Usage:
    python3 ingest-profiles-csv.py

Options:
    --config    the name of a configuration file.
                The configuration file must contain the following information:

                [YinzCamUsersNHL]
                username =
                password =

                [YinzCamUsersNBA]
                username =
                password =

                [YinzCamUsersTFC]
                username =
                password =

                [ADL]
                tenant_id =
                client_secret =
                client_id =
                store_name =
"""

# Module Imports
import configparser                 # Basic configuration file parser language
import argparse                     # Parses predefined arguments out of sys.argv
import logging                      # Implement a flexible event logging system
from time import time               # Provides various time related functions
from datetime import datetime       # Supplies classes for manipulating dates and time

import requests                     # An elegant and simple HTTP library for Python
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from azure.datalake.store import core, lib, multithread
                                    # Python filesystem library for Azure Data Lake Store

import os                           # Portable way of using operating system functions
import sys                          # Provides access to variables used by the interpreter
import json                         # JSON encoder and decoder

import pandas as pd                 # Data analysis and manipulation tool for Python
import numpy as np                  # Core library for scientific computing with Python


# Module metadata
__author__  = "Nicole Ridout"
__email__   = "Nicole.Ridout@MLSE.com"
__version__ = "1.0"
__date__    = "March 31, 2020"
__status__  = "Prototype"


# =============================================================================
#   IngestUserProfiles(team, page=0, limit=10000)                             =
#                                                                             =
#   Accepts `team` arg value, which is one of NHL, MLS, or NBA.               =
#   Creates User Table Dataframe by ingesting the User Profiles from YinzCam. =
#   Response payload limited to 10,000 users per call. Passes raw JSON user   =
#   profiles to `FormatUsers`, then converts to DataFrame. Concats DF from    =
#   each call. Passes complete DF to ExportCSVtoADL.                          =
# =============================================================================

def IngestUserProfiles(team, page=0, limit=10000):
    start_ingest = time()
    logging.info("Starting IngestUserProfiles for {team}.".format(team=team))

    # Empty DF to hold users
    UserDF = pd.DataFrame()

    # Assign mobile app address based on team arg value
    if team == "MLS":
        team_mob = "mobile.torontotilidie.com"
    elif team == "NBA":
        team_mob = "mobile.northside4life.com"
    elif team == "NHL":
        team_mob = "mobile.leafsnation.com"

    # Get authenication info for YinzCam API
    user, pssd = get_yinz_conf(team)

    # Set up Requests retry function to retry call on server error
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    # Call the YinzCam API, looping through pages of 10,000 until complete
    while True:
        t1 = time()
        logging.info("Creating API call for page {page}.".format(page=page))

        # Format the API url
        url = ('https://ydp-api.yinzcam.com/profiles/JANRAIN?page={page}&limit={limit}'\
                    .format(page=page, limit=10000))

        # Try the API call
        try:
            logging.info("Sending GET request to YinzCam API.")
            response = http.get(url=url, auth=(user,pssd), timeout=240)
            rsp_code = response.status_code
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error("GET request to API failed. Response code: {response_code}. \
                Error: {error}.".format(response_code=rsp_code, error=str(e)))
            raise
        else:
            logging.info("Response code: {response_code}.".format(response_code=rsp_code))
            users_raw = response.json()['Users']
            logging.info("Page {page_number} has {total_users} records."\
                    .format(page_number=page,total_users=len(users_raw)))

            # Format user profiles and convert to DF
            users_json = FormatUsers(users_raw, team_mob)
            UserDF_add = pd.DataFrame(users_json)
            # Concat DF with previous user DF
            UserDF = pd.concat([UserDF, UserDF_add], ignore_index = True)

            page += 1
            t2 = time()
            logging.info("It took {0:.2} min to get {1} records"\
                .format(float(t2-t1)/60.0,len(users_json)))

            # If the resonse is less than 10,000, all records have been ingested
            if len(users_json) < 10000:
                logging.info("There are no more records. End Call.")
                break

    end_ingest = time() - start_ingest
    logging.info("IngestUserProfiles took {0:.2} min to pull the data"\
        .format(float(end_ingest)/60.0))
    logging.info("Total users from this call = {0}".format(len(UserDF)))

    # Export DF to ADL
    ExportCSVtoADL(team, UserDF)


# =============================================================================
#   FormatUsers(users, team_mob)                                              =
#                                                                             =
#   Accepts raw user JSON from API. Loops through each profile entry,         =
#   reformatting the data into key-value pairs, and retaining the specified   =
#   fields for each user profile. Checks the data integrity -> if first user  =
#   entry does not contain a yinzid, exit the program. Returns a list of      =
#   cleaned user entries.                                                     =
# =============================================================================

def FormatUsers(users, team_mob):
    start_format = time()
    logging.info("Formatting JSON for users in this API call.")

    user_list = []

    # Loop through each user entry
    for user_entry in users:
        user = {'yinzid': None, 'email': None, 'first_name': None, 'last_name': None,
        'id_global': None, 'firstLogin': None, 'lastLogin': None, 'clientId': None}

        # Loop through each key-value pair entry
        for entry in user_entry['Entry']:
            # Reformat the entries to key-value pairs
            formatted_entry = prettify_json(entry)
            entry_key = list(formatted_entry.keys())[0]
            entry_value = list(formatted_entry.values())[0]
            # Process jainrain_clients info if user has it
            if entry_key == 'janrain_clients' and entry_value != '[]':
                janrain_client_info = json.loads(formatted_entry['janrain_clients'])
                for record in janrain_client_info:
                    # Only record the entry for the team being processed
                    if record['clientId'] == team_mob:
                        user['clientId'] = team_mob
                        user['firstLogin'] = record['firstLogin']
                        user['lastLogin'] = record['lastLogin']
            # If entry key contained in user_json, update user_json value
            elif entry_key in user.keys():
                user.update(formatted_entry)

        # Check the integrity of the data
        if user['yinzid'] == None:
            logging.info("This user appears to be missing yinzid: {user}"\
                .format(user=user))
            raise Exception("Something is wonky with this API call, retry.")

        # Add each formatted user to the user list
        user_list.append(user)

    end_format = time() - start_format
    logging.info("Formatting this batch took {0:.2} min."\
        .format(float(end_format)/60.0))

    return user_list


# =============================================================================
#   ExportCSVtoADL(team, UserDF)                                              =
#                                                                             =
#   Accepts the user DataFrame and the team value. Creates an ADL file        =
#   system client. Defines the data lake path and file name. Exports DF to    =
#   ADL.                                                                      =
# =============================================================================

def ExportCSVtoADL(team, UserDF):
    start_export = time()
    logging.info("Starting Export to ADL.")

    # Get ADL credentials to create file system client
    adlCreds, store_name = get_adl_conf()
    adlsFileSystemClient = core.AzureDLFileSystem(adlCreds, store_name=store_name)

    # Define data lake file path
    data_lake_directory = '/yinz_cam/{team}_tor/users/'.format(team=team.lower())
    file_name = '{team}_yinzcam_users.csv'.format(team=team.lower())

    # Write file to ADL
    logging.info("Writing UserDF to: {directory}{file}."\
        .format(directory=data_lake_directory, file=file_name))
    with adlsFileSystemClient.open(data_lake_directory + file_name, 'wb') as f:
        f.write(str.encode(UserDF.to_csv(index = False)))

    end_export = time() - start_export
    logging.info("Exporting the file to ADL took {0:.2} min."\
        .format(float(end_export)/60.0))


# =============================================================================
#   Helper Functions                                                          =
#                                                                             =
# =============================================================================

# Get Yinzcam API credentials
def get_yinz_conf(team):
    teamuser = 'YinzCamUsers' + team
    user = config.get(teamuser, 'username')
    pssd = config.get(teamuser, 'password')
    return user, pssd

# Get Azure Data Lake Credentials
def get_adl_conf():
    tenant_id = config.get('ADL', 'tenant_id')
    client_secret = config.get('ADL', 'client_secret')
    client_id = config.get('ADL', 'client_id')
    store_name = config.get('ADL', 'store_name')
    adlCreds = lib.auth(tenant_id = tenant_id,
                        client_secret = client_secret,
                        client_id = client_id,
                        resource = 'https://datalake.azure.net/')
    return adlCreds, store_name

# Restuctures User Profile JSON into correct key:value pairs
def prettify_json(entry):
    processed_entry_pair = {}
    # Flatten the JSON (removes a level of nesting)
    flat_entry = flatten_json(entry)
    # Pull the value pairs into a new key-value pair & insert into dict
    key = list(flat_entry.values())[0]
    value = list(flat_entry.values())[1]
    processed_entry_pair[key] = value
    return processed_entry_pair

# Flattens nested JSON structures
def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


# =============================================================================
#   Main                                                                      =
#                                                                             =
#   Accepts 'team' value from the console and defines the team mobile         =
#   address. Gets credentials from config file and sets authentication for    =
#   YinzCam API and ADL. Passes the team value to CreateUserTable method.     =
# =============================================================================

if __name__ == '__main__':

    # Start program run time
    program_start = time()

    # Collect imput from the console
    parser = argparse.ArgumentParser(description='This script will ingest data\
        from YinzCam API User Profiles')
    parser.add_argument('team', help="the team being ingested", type=str)
    # Set team value from args
    args = parser.parse_args()
    team = args.team.upper()

    # Process the config file
    config_file = ('config')
    config = configparser.ConfigParser()
    if os.path.isfile(config_file):
        config.read(config_file)
    else:
        raise IOError('Please make sure you have the configuration file in correct path.')

    # Set up logging for debugging purpose
    logging.getLogger().setLevel(logging.INFO)

    # Start main module to ingest user profiles
    IngestUserProfiles(team)

    # End program run time
    program_end = time() - program_start
    logging.info("Program Complete. Run time: {0:.2} min."\
        .format(float(program_end )/60.0))