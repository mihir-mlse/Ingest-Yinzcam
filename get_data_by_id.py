"""
Get Data by Id

Gets data from the Yinzcam Realtime API

Usage:
    python get_data_by_id.py -v -gp mls_tor 2021-12-23T17:15:09-05:00
"""

import requests, logging, configparser, argparse, os, sys
import matplotlib
matplotlib.use('Agg')

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from time import time
from datetime import datetime, timezone
from dateutil import tz

from azure.datalake.store import core, lib, multithread
# from azure.storage.blob import BlockBlobService


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
MAX_RECORDS_PER_FILE = 1000000


def get_right_format(time):
    utc = datetime.strptime(time,'%Y-%m-%dT%H:%M:%S%z')
    to_zone =  tz.gettz('America/Toronto')
    time_est = utc.astimezone(to_zone)
    str_time_est = time_est.strftime('%Y-%m-%d_%H_%M_%S')
    return str_time_est


def get_data(maxId, auth, max_value=100000000, verbose=False):
    """Gets data from the Yinzcam Realtime API
    """
    newMaxId = maxId + 1
    value_count = 0
    limit = 250
    maxRecords = 250
    max_value_count = max_value
    actions_l = []
    sessions_l = []
    geoip_l = []
    hardware_l = []
    number_of_calls = 1

    # Calls the API to collect records in batches of 100
    while maxRecords >= limit and value_count < max_value_count:
        url = 'https://mlse-analytics-api.yinzcam.com/analytics/raw/actions?startId=' \
            + str(newMaxId) + '&maxRecords='+str(maxRecords)+'&sortBy=id'
        retry = 5
        response = None
        status_code = None
        content = None
        timeout = False

        # API retry loop handles timeouts and no content issues
        while retry > 0:
            # Try the API call and handle timeout (60 sec) if applicable
            try:
                response = requests.get(url, auth=auth, timeout=240)
                status_code = response.status_code
                logging.info("Call #{number_of_calls}: The status call of this call is {status_code}".format(
                    number_of_calls=number_of_calls, status_code=status_code))
            except Exception as e:
                timeout = True
                logging.info("Timeout reached, error details: {e}".format(e=e))

            if timeout:
                # Keep retrying timed out call max 5 times
                retry -= 1
                logging.info("Call #{number_of_calls} timed-out, number of retries left {retry}...".format(
                    number_of_calls=number_of_calls, retry=retry))
                timeout = False
            else:
                # Check that the call returned content, and if not, retry
                try:
                    content = response.json()
                except Exception as e:
                    retry -= 1
                    logging.info("Error with content: {e}".format(e=e))
                    logging.info("Retrying call #{number_of_calls}...".format(
                        number_of_calls=number_of_calls))
                else:
                    retry = 0

        # If content received from call, process
        if content:
            logging.info("Content received, processing.")
            actions = pd.DataFrame(content['actions'])
            maxRecords = len(actions)
            if maxRecords > 0:
                value_count += maxRecords
                newMaxId = actions.id.astype(int).max() + 1
                actions_l.append(actions)
                sessions_l.append(pd.DataFrame(content['sessions']))
                geoip_l.append(pd.DataFrame(content['geoip']))
                hardware_l.append(pd.DataFrame(content['hardware']))
                number_of_calls += 1
                logging.info("New Max ID: {0}".format(newMaxId))
                logging.info('Number of calls {0}, and status code {1}, maxRecords = {2}, and number of records = {3}'.format(
                    number_of_calls, status_code, maxRecords, value_count))
        # If max retries made and no content recieved, exit the API call loop
        else:
            logging.info("Max retries met, no data from this call. Exiting.")
            break

    logging.info("Finished Getting Data...")

    if value_count > 0:
        return (value_count,
                {'actions': pd.concat(actions_l, axis=0),
                 'sessions': pd.concat(sessions_l, axis=0),
                 'geoip': pd.concat(geoip_l, axis=0),
                 'hardware': pd.concat(hardware_l, axis=0)}
                )
    else:
        return 0, None


def get_max_id_adl(team, adlsFileSystemClient, verbose = False):
    data_lake_directory = '/yinz_cam/' + team +  '/realtime_api/actions'

    if adlsFileSystemClient.exists(data_lake_directory):
        listFiles = sorted(adlsFileSystemClient.listdir(data_lake_directory ))
    else:
        listFiles = []
    if len(listFiles) > 0:
        last_file = listFiles[-1]
        with adlsFileSystemClient.open(last_file, 'rb') as f:
            df = pd.read_csv(f)
        maxId = df.id.astype(int).max()
        if verbose:
            logging.info("The max ID from actions in ADL is {0}!".format(maxId))
        return maxId
    else:
        if verbose:
            logging.info("The min ID is 1")
        return 0

def push_to_adl(team, adlsFileSystemClient, collection, df, currentRunTime, minAct, maxAct, verbose = False):
    data_lake_directory = '/yinz_cam/' + team +  '/realtime_api/' + collection + '/'
    fileName = get_right_format(currentRunTime) + '_' + str(minAct) + '_' + str(maxAct) + '.csv'
    with adlsFileSystemClient.open(data_lake_directory + fileName, 'wb') as f:
        if verbose:
            logging.info("Writing {0} to ADL".format(data_lake_directory + fileName))
        f.write(str.encode(df.to_csv(index = False)))


def main():

    parser = argparse.ArgumentParser(description='Fetch API data from Yinz Cam API')
    parser.add_argument('team', help="the team you want to fetch data from, nhl_tor, nba_tor or mls_tor", type=str)
    parser.add_argument('start_time', help="the start date time of the task", type=str)
    parser.add_argument('-v','--verbose', action='store_true',help='prints output to the stdout, such as timing and what it is doing')
    parser.add_argument('-gp','--generate_plot',action='store_true',help='creates a histogram of the pull by month')

    args = parser.parse_args()
    team = args.team
    currentRunTime = args.start_time

    #read config file
    config_file = ('./config')
    config = configparser.ConfigParser()
    if os.path.isfile(config_file):
        config.read(config_file)
    else:
        raise IOError('Please make sure you have the configuration file in correct path.')

    #start reading the keys from the config file
    key = config.get('APIConfig', team + '_key')
    auth = (team.lower(), key)
    
    #read the keys form config file for ADL
    tenant_id = config.get('ADL', 'tenant_id')
    client_secret = config.get('ADL', 'client_secret')
    client_id = config.get('ADL', 'client_id')
    store_name = config.get('ADL', 'store_name')

    adlCreds = lib.auth(tenant_id = tenant_id, 
                        client_secret = client_secret, 
                        client_id = client_id, 
                        resource = 'https://datalake.azure.net/')

    adlsFileSystemClient = core.AzureDLFileSystem(adlCreds, store_name = store_name)

    # maxId =  1340320433 # 267450897
    num_of_records = 1000000
    while num_of_records == MAX_RECORDS_PER_FILE:
        t1 = time()
        maxId = get_max_id_adl(
            team, adlsFileSystemClient, verbose=args.verbose)
        t2 = time()
        if args.verbose:
            logging.info(
                "It took {0} min to read the maxID from ADL".format(float(t2 - t1)/60))
        num_of_records, dfs = get_data(
            maxId, auth, max_value=MAX_RECORDS_PER_FILE, verbose=True)
        t3 = time()

        if num_of_records > 0:
            if args.verbose:
                logging.info(
                    "It took {0} min to get the data from YinzCam".format(float(t3 - t2)/60))
                logging.info("Number of actions = {0}".format(
                    len(dfs['actions'])))
                logging.info("Number of sessions = {0}".format(
                    len(dfs['sessions'])))
                logging.info(
                    "Number of geoips = {0}".format(len(dfs['geoip'])))
                logging.info("Number of hardwares = {0}".format(
                    len(dfs['hardware'])))

            dfs['actions'].id = dfs['actions'].id.astype(int)

            minAct = dfs['actions'].id.min()
            maxAct = dfs['actions'].id.max()

            if args.verbose:
                logging.info("Min = {0} and Max = {1} from this Yinzcam call at {2}".format(
                    minAct,
                    maxAct,
                    get_right_format(currentRunTime)))

                t1 = time()

            dfs['sessions'] = dfs['sessions'].drop_duplicates(keep='last')
            dfs['actions'] = dfs['actions'].drop_duplicates(keep='last')
            dfs['geoip'] = dfs['geoip'].drop_duplicates(keep='last')
            dfs['hardware'] = dfs['hardware'].drop_duplicates(keep='last')
            # Organize columns 
            actions_cols = ['id', 'in_venue', 'invisible_date_time', 'request_date_time', 'resource_major', 'resource_minor', 'session_id', 'sort_order', 'type_major', 'type_minor','yinzid']
            sessions_cols = ['actions', 'app_id', 'app_version','carrier','device_adid','device_generated_id','device_id','end_date_time','hardware_device_id','id','mcc','mdn','mnc','os_version','start_date_time']
            geoip_cols = ['city_geoname_id','city_name','continent_code','continent_geoname_id','continent_name','country_code','country_geoname_id','country_name','id','postal_code','session_device_generated_id','subdivision1_code','subdivision1_geoname_id','subdivision1_name','subdivision2_code','subdivision2_geoname_id','subdivision2_name','subdivision3_code','subdivision3_geoname_id','subdivision3_name','subdivision4_code','subdivision4_geoname_id','subdivision4_name','time_zone']
            hardware_cols = ['id','manufacturer','model','platform','screen_width','screen_height']
            dfs['actions'] = dfs['actions'][actions_cols]
            dfs['sessions'] = dfs['sessions'][sessions_cols]
            try:
                dfs['geoip'] = dfs['geoip'][geoip_cols]
            except:
                print(dfs['geoip'].columns.values)
            dfs['hardware'] = dfs['hardware'][hardware_cols]

            if args.verbose:
                t2 = time()
                logging.info(
                    "It took {0} min to get rid of duplicates".format(float(t2 - t1)/60))

                logging.info("Number of actions = {0}".format(
                    len(dfs['actions'])))
                logging.info("Number of sessions = {0}".format(
                    len(dfs['sessions'])))
                logging.info(
                    "Number of geoips = {0}".format(len(dfs['geoip'])))
                logging.info("Number of hardwares = {0}".format(
                    len(dfs['hardware'])))

            t1 = time()
            for collection in ['actions', 'sessions', 'geoip', 'hardware']:
                print('Pushing {0} to ADL'.format(collection))
                push_to_adl(team, adlsFileSystemClient, collection,
                            dfs[collection], currentRunTime, minAct, maxAct, verbose=args.verbose)
            t2 = time()
            if args.verbose:
                logging.info(
                    "It took {0} min to write Pandas DF to ADL".format(float(t2 - t1)/60))

            if args.generate_plot:
                t1 = time()
                dfs['actions'].request_date_time = pd.to_datetime(
                    dfs['actions'].request_date_time)
                dfs['actions']['date'] = dfs['actions'].request_date_time.dt.date
                dfs['actions']['month'] = dfs['actions'].request_date_time.apply(
                    lambda x: x.strftime('%Y-%m'))
                grouped = dfs['actions'].groupby('month').size()
                ax = (np.log10(grouped)).plot(kind='bar', figsize=(8, 8))
                ax.set_xlabel('Month')
                ax.set_ylabel('log10 Count')
                ax.grid()
                fileNamePlot = "update_" + \
                    get_right_format(currentRunTime) + '.pdf'
                data_lake_directory = '/yinz_cam/' + team + '/realtime_api/figure_checks/'
                plt.savefig('/tmp/' + fileNamePlot)
                multithread.ADLUploader(adlsFileSystemClient, lpath='/tmp/' + fileNamePlot,
                                        rpath=data_lake_directory+fileNamePlot,
                                        nthreads=64, overwrite=True,
                                        buffersize=4194304, blocksize=4194304)
                t2 = time()
                os.remove('/tmp/' + fileNamePlot)
                if args.verbose:
                    logging.info(
                        "It took {0} min to generate the checkup plot".format(float(t2 - t1)/60))
                    logging.info(grouped.sort_values(ascending=False))

        else:
            logging.info("No new records to process.")
            break


if __name__ == "__main__":
    elapsed = time()
    main()
    elapsed = time() - elapsed
    logging.info("It took {0:.2f} min to call YinzCam".format(float(elapsed)/60))