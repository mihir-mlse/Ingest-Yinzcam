import requests, configparser, os, logging, sys

from requests.auth import HTTPBasicAuth
from time import time

from azure.datalake.store import core, lib, multithread

def main(logger):

    # Configure access to config file
    config_file = ('./config')
    config = configparser.ConfigParser()
    if os.path.isfile(config_file):
        config.read(config_file)
    else:
        raise IOError('Please make sure you have the configuration file in correct path.')

    username = config.get('YinzCamDaily', 'username')
    password = config.get('YinzCamDaily', 'password')

    # Set up credentials for data lake
    tenant_id = config.get('ADL', 'tenant_id')
    client_secret = config.get('ADL', 'client_secret')
    client_id = config.get('ADL', 'client_id')
    store_name = config.get('ADL', 'store_name')

    adlCreds = lib.auth(tenant_id = tenant_id,
                        client_secret = client_secret,
                        client_id = client_id,
                        resource = 'https://datalake.azure.net/')
    adlsFileSystemClient = core.AzureDLFileSystem(adlCreds, store_name = store_name)

    data_lake_directory = '/yinz_cam/cards_content/'

    # List of url endpoints to be ingested
    url_list = ['http://data-ftp.yinzcam.com/mlse/meta/meta-push.csv',
        'http://data-ftp.yinzcam.com/mlse/meta/meta-media-mls.csv',
        'http://data-ftp.yinzcam.com/mlse/meta/meta-media-nhl.csv',
        'http://data-ftp.yinzcam.com/mlse/meta/meta-media-nba.csv',
        'http://data-ftp.yinzcam.com/mlse/meta/meta-card-views.csv']

    # Transfer files from yinz_cam http server to data lake
    for url in url_list:

        file_size_datalake = 0

        t1 = time()
        response = requests.get(url, auth=(username, password))
        file_name = url.split('meta/')[1]

        # Get current CSV file length from data lake
        if adlsFileSystemClient.exists(data_lake_directory + file_name):
            file_info = adlsFileSystemClient.info(data_lake_directory + file_name)
            file_size_datalake = file_info['length']

        file_size_server = int(response.headers['Content-length'])

        # Check if there have been updates to the file and only download updated files
        if file_size_server > file_size_datalake:
            print("Updating: " + file_name)
            # Collect text from response and write to adl
            data = response.text
            with adlsFileSystemClient.open(data_lake_directory + file_name, 'wb') as f:
                f.write(str.encode(data))
            print(file_name + " has been updated.")
        else:
            print("{file} in the datalake has the same size as file from the server".format(file=file_name))

        t2 = time() - t1
        logger.info("Total elapsed time to process {file} = {0:.2f} sec".format(float(t2), file=file_name))


if __name__ == "__main__":

	elapsed = time()

	logging.basicConfig(level=logging.INFO)
	logger = logging.getLogger(__name__)

	main(logger)
	elapsed = time() - elapsed
	logger.info("Total elapsed time for the script = {0:.2f} sec".format(float(elapsed)))