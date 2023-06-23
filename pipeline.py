
import requests
from bs4 import BeautifulSoup
import zipfile
import re
import io
import os
import csv
import pandas as pd
import sqlite3 as sql
import tqdm

class AISdatetGenerator(object):
    def __init__(self,timestart:str='2023-06-05', timeend:str=None, dataDirectory:str='.', databaseDirectory:str='.', databaseName:str='aisPlay.db') -> None:
        self.url = "http://web.ais.dk/aisdata/" 
        self.timestart = timestart
        self.timeend = timeend
        self.dataDirectory = dataDirectory
        self.databaseDirectory = databaseDirectory
        
        # move syntaxcheck of input timestart and timeend up here

        self.data = self.crawl(self.url)
        self.csv_filenames = []
        


    def crawl(self, url):
        ### GET THE ZIP FILES FOR OPTION ###
        # Send a GET request to retrieve the website content
        response = requests.get(url)

        # Create a BeautifulSoup object to parse the HTML
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <a> tags containing .zip files and extract their href attribute
        self.zip_names = soup.find_all('a', href=lambda href: href.endswith('.zip'))
        

           
        ### CHOSE ZIP FILES using regex ###
        # check if zips are already in directory
        AIS_dates_in_directory = [file[6:-4] for file in os.listdir(self.dataDirectory) if file.endswith(".csv")]


        # extract only timestamps
        self.zip_names = [x['href'][6:-4] for x in self.zip_names] 
        
        if self.timeend is None: #single file
            self.desired_date = list(filter(lambda x: x == self.timestart, self.zip_names))
            if self.desired_date[0] in AIS_dates_in_directory: # check if already downloaded 
                print("The AIS data for the desired timeframe is already downloaded. Data is ready for filtering.")
                return
            

            else:
                print("Proceeding with preparing the download for the single day requested.")
                selected_zips = ['aisdk-' + self.desired_date[0] + '.zip']

        else: # multiple files
            desired_dates = list(filter(lambda x: x >= self.timestart and x <= self.timeend, self.zip_names))
            for date in desired_dates: 
                if date in AIS_dates_in_directory:
                    desired_dates.remove(date)
            print(f'There are {len(desired_dates)} zip files not downloaded. This is estimated to take {len(desired_dates) * 10} Minutes. Do you want to process with the download? (y/n)')
            ret = input(f'Download?')
            if ret == 'n':
                print(f'data set creation stopped.')
                return
            elif ret == 'y':
                print('Download will beginn shortly.')
            else:
                print('counld not understand input. Will continue to download.')
            selected_zips = ['aisdk-' + desired_date + '.zip' for desired_date in desired_dates]
        
        print('Zip files for download chosen: ', selected_zips)
        

        #### DOWNLOAD chosen zip files and EXTRACT the zip file into the directory specified ####

        for zip_name in selected_zips:
            zip_url = self.url + zip_name    # Construct the absolute URL of the zip file
            print(f'Begining Download of : {zip_name}')

            # Send a GET request to retrieve the zip file
            response = requests.get(zip_url) #dauert etwa 11 Minuten! # download cannot be accellerated. No filtering online possible
            
            ### PROGRESS BAR CAUSES ERROR AS response is alreay been used ###
            # total_size_in_bytes= int(response.headers.get('content-length', 0))
            # block_size = 1024 #1 Kibibyte
            # progress_bar = tqdm.tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
            # with open('test.dat', 'wb') as file:
            #     for data in response.iter_content(block_size):
            #         progress_bar.update(len(data))
            #         file.write(data)
            # progress_bar.close()
            # if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            #     print("ERROR, something went wrong")
            
            ## Save as zip file            
            with open(zip_name,'wb') as output_file:
                output_file.write(response.content)
            
            # Extract the zip file
            with zipfile.ZipFile(zip_name,"r") as zip_ref:
                zip_ref.extractall(self.dataDirectory)
            print(f'Zip file {zip_name} extracted successfully')

        print('Desired data is ready for filtering.')
    
    def connectSQLite(self):
        self.connection = sql.connect('../ais.db')
        print(self.connection.total_changes) # test if connection is successful
        self.cursor = self.connection.cursor()

       



    def updateMMSIs(self):
        '''
        Filter data on MMSI per class using SQL
        '''
        ## TODO: CHECK IF CONNECTION EXISTS, else connect
        ## Drop temp tables if exist
        self.cursor.execute('DROP TABLE IF EXISTS tempfishingMMSI;')
        self.cursor.execute('DROP TABLE IF EXISTS temppassengerMMSI;')
        self.cursor.execute('DROP TABLE IF EXISTS tempmerchantMMSI;')


        ## Create temp Tables
        self.cursor.execute('CREATE TABLE tempfishingMMSI(MMSI UNIQUE, NavStatus TEXT);')
        self.cursor.execute('CREATE TABLE temppassengerMMSI(MMSI UNIQUE, ShipType TEXT);')
        self.cursor.execute('CREATE TABLE tempmerchantMMSI(MMSI UNIQUE, CargoType TEXT);')

        ## Import existing and newly downloaded dataset and alter to aisPlay format
        #users = pd.read_csv('users.csv')
        #users.to_sql('users', conn, if_exists='append', index = False, chunksize = 10000)


        ## Alter tables into desired table structure

        ## filter MMSIs for the three defined classes 
        self.cursor.execute(f'INSERT INTO tempfishingMMSI SELECT DISTINCT(MMSI), NavStatus FROM tempAIS WHERE NavStatus LIKE "%fishing%" OR "ShipType" LIKE "%ishing%";')
        self.cursor.execute(f'INSERT INTO temppassengerMMSI SELECT DISTINCT(MMSI), "ShipType" FROM tempAIS WHERE "ShipType" LIKE "%passenger%";')
        self.cursor.execute(f'INSERT INTO tempmerchantMMSI SELECT DISTINCT(MMSI), NULL FROM tempAIS WHERE CargoType NOT NULL;')

        ## Merge tempMMSIs into the OG MMSI file
        # TODO: CHECK IF MMSI FILES exist: else: print('make sure you are connected to the correct database')
        self.cursor.execute(f'INSERT INTO fishingMMSI SELECT tempfishingMMSI.MMSI, tempfishingMMSI.NavStatus FROM tempfishingMMSI WHERE tempfishingMMSI.MMSI NOT IN (SELECT fishingMMSI.MMSI from fishingMMSI);')
        self.cursor.execute(f'passengerMMSI SELECT temppassengerMMSI.MMSI, temppassengerMMSI.ShipType FROM temppassengerMMSI WHERE temppassengerMMSI.MMSI NOT IN (SELECT passengerMMSI.MMSI from passengerMMSI);')
        self.cursor.execute(f'INSERT INTO merchantMMSI SELECT tempmerchantMMSI.MMSI, tempmerchantMMSI.CargoType FROM tempmerchantMMSI WHERE tempmerchantMMSI.MMSI NOT IN (SELECT merchantMMSI.MMSI from merchantMMSI);')

        
        ## drop temporary MMSI files
        self.cursor.execute('DROP TABLE IF EXISTS tempfishingMMSI;')
        self.cursor.execute('DROP TABLE IF EXISTS temppassengerMMSI;')
        self.cursor.execute('DROP TABLE IF EXISTS tempmerchantMMSI;')

        pass


    def extractCSV(self, vesselType:str='fishing'):
        '''
        Filter data on MMSI per class using SQL
        i: vesselType: str -> {fishing, passenger, merchant}
        o: csv file
        '''
        ## TODO: CHECK IF CONNECTION EXISTS, else connect

        ### create OUTPUT file

        pass