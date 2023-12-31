
import requests
from bs4 import BeautifulSoup
import zipfile
import re
import io
import os
import csv
import pandas as pd
import sqlite3 as sql
import subprocess
import tqdm
import rasterio

class AISdatetGenerator(object):
    def __init__(self,timestart:str='2023-06-05',
                 timeend:str=None, 
                 dataDirectory:str='./', 
                 databaseDirectory:str='./', 
                 databaseName:str='aisPlay.db',
                 add_waterdepth:bool=True,
                 unrealistic_location:bool=False,
                 unrealistic_speeds:bool=False,
                 unrealistic_mmsi:bool=False,
                 fill_statics:bool=False,
                 comp_distance:bool=False,
                 comp_timedelta:bool=False,
                 comp_speed:bool=False,
                 drop_list:list=None,
                 latMinBoundary:int=54,
                 latMaxBoundary:int=59,
                 lonMinBoundary:int=3,
                 lonMaxBoundary:int=17,
                 lenMMSI:int=9) -> None:
        
        self.url = "http://web.ais.dk/aisdata/" #
        self.bathymetry_file = 'bathymetry.tif'

        self.timestart = timestart
        self.timeend = timeend
        self.dataDirectory = dataDirectory
        self.databaseDirectory = databaseDirectory



        # move syntaxcheck of input timestart and timeend up here

        self.data = self.crawl(self.url)
        self.csv_filenames = []
        
        self.updateMMSIs()
        self.drop_unrealistic_courses()

        if unrealistic_location: self.drop_unrealistic_loc(latMinBoundary, latMaxBoundary, lonMinBoundary, lonMaxBoundary)
        if unrealistic_speeds: self.drop_unrealistic_speeds()
        if unrealistic_mmsi: self.drop_unrealistic_mmsi(lenMMSI)
        if fill_statics: self.fill_statics()
        if comp_distance: self.comp_distance()
        if comp_timedelta: self.comp_timedelta()
        if comp_speed: self.comp_speed()
        if drop_list: self.drop_list(drop_list)

        if add_waterdepth: self.add_waterdepth()


    def crawl(self, url):
        ### GET THE ZIP FILES FOR OPTION ###
        # Send a GET request to retrieve the website content
        response = requests.get(url)

        # Create a BeautifulSoup object to parse the HTML
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all <a> tags containing .zip files and extract their href attribute
        self.zip_names = soup.find_all('a', href=lambda href: href.endswith('.zip'))
        # extract only timestamps
        self.zip_names = [x['href'][6:-4] for x in self.zip_names] 

           
        ### CHOSE ZIP FILES ###

        # collect all timestamp already in directory
        AIS_dates_in_directory = [file[6:-4] for file in os.listdir(self.dataDirectory) if file.endswith(".csv")]
        print('dates in dataDirectory: ', AIS_dates_in_directory)

        if self.timeend is None: #single file
            #self.desired_date = list(filter(lambda x: x == self.timestart, self.zip_names))
            # TODO Test if wrong directory -> IndexError  when [0] to access empty self.desired_dates
            # check if already downloaded 
            if self.timestart not in AIS_dates_in_directory:
                print("Proceeding with preparing the download for the single day requested.")
                selected_zips = ['aisdk-' + self.timestart + '.zip']    

            else:
                print("Selected AIS data already downloaded. Ready for filtering.")
                return
            
            
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
        self.conn = sql.connect(self.databaseDirectory + 'ais.db')
        print('Total changes in DB: ', self.conn.total_changes) # test if connection is successful
        self.cursor = self.conn.cursor()

        # Output basic information about the database
        self.cursor.execute('SELECT name FROM sqlite_schema WHERE type="table" AND name NOT LIKE "sqlite_%";')
        all_tables = self.cursor.fetchall()
        print(f'The following tables are in Database {all_tables}')

    def disconnectSQLite(self):
        if self.conn: self.cursor.close()

    def updateMMSIs(self):
        '''
        Filter data on MMSI per class using SQL
        '''
        import_link = self.dataDirectory + f'/aisdk-{self.timestart}.csv'
        self.tablenameAISdata = 'XYZ'
        self.connectSQLite()

        ## TODO: CHECK IF CONNECTION EXISTS, else connect
        ## Drop temp tables if exist
        self.cursor.execute('DROP TABLE IF EXISTS tempfishingMMSI;')
        self.cursor.execute('DROP TABLE IF EXISTS temppassengerMMSI;')
        self.cursor.execute('DROP TABLE IF EXISTS tempmerchantMMSI;')

        # Remove all rows in XYZ table
        self.cursor.execute('DELETE FROM XYZ WHERE SOG NOT NULL;')

        ## Create temp Tables
        self.cursor.execute('CREATE TABLE tempfishingMMSI(MMSI UNIQUE, NavStatus TEXT);')
        self.cursor.execute('CREATE TABLE temppassengerMMSI(MMSI UNIQUE, ShipType TEXT);')
        self.cursor.execute('CREATE TABLE tempmerchantMMSI(MMSI UNIQUE, CargoType TEXT);')
        
        print('temp MMSI files created successfully.')

        self.cursor.execute(f'SELECT COUNT(*) FROM {self.tablenameAISdata};')
        all_tables = self.cursor.fetchall()
        print(f'{all_tables} Samples in XYZ')
        self.cursor.close()

        # if time range and multiple tables: loop over list where updates import_link
        
        subprocess.call(['sqlite3', "ais.db", "-cmd", ".mode csv", f".import {import_link} {self.tablenameAISdata}"])#, ".mode markdown", "SELECT COUNT(*) FROM XYZ;"])
        #os.system("sqlite3 ../ais.db;import subprocess .mode csv; .quit;")
        
        print('import to sqlite3 command ran through')
        self.cursor = self.conn.cursor()

        # Check if the import was successful
        self.cursor.execute(f'SELECT COUNT(*) FROM {self.tablenameAISdata};')
        sampleSizeAIS = self.cursor.fetchall()
        print(f'{sampleSizeAIS} Samples in {self.tablenameAISdata}')

        self.disconnectSQLite()
        return
        ## Import by importing to python and then to mysql :-1:
        ## Import existing and newly downloaded dataset and alter to aisPlay format
        #users = pd.read_csv(self.dataDirectory +'/aisdk-' + self.desired_date[0]+'.csv')
        #users.to_sql('tempAIS', self.cursor, if_exists='append', index = False, chunksize = 10000)
        #print(f'{self.desired_date} importe into database')

        ## Alter tables into desired table structure

        
        ## filter MMSIs for the three defined classes 
        self.cursor.execute(f'INSERT INTO tempfishingMMSI SELECT DISTINCT(tempfishingMMSI.MMSI), NavStatus FROM tempAIS WHERE NavStatus LIKE "%fishing%" OR "ShipType" LIKE "%ishing%";')
        self.cursor.execute(f'INSERT INTO temppassengerMMSI SELECT DISTINCT(temppassengerMMSI.MMSI), "ShipType" FROM tempAIS WHERE "ShipType" LIKE "%passenger%";')
        self.cursor.execute(f'INSERT INTO tempmerchantMMSI SELECT DISTINCT(tempmerchantMMSI.MMSI), NULL FROM tempAIS WHERE CargoType NOT NULL;')
 
        
        ## Merge tempMMSIs into the OG MMSI file
        # TODO: CHECK IF MMSI FILES exist: else: print('make sure you are connected to the correct database')
        self.cursor.execute(f'INSERT INTO fishingMMSI SELECT tempfishingMMSI.MMSI, tempfishingMMSI.NavStatus FROM tempfishingMMSI WHERE tempfishingMMSI.MMSI NOT IN (SELECT fishingMMSI.MMSI from fishingMMSI);')
        self.cursor.execute(f'passengerMMSI SELECT temppassengerMMSI.MMSI, temppassengerMMSI.ShipType FROM temppassengerMMSI WHERE temppassengerMMSI.MMSI NOT IN (SELECT passengerMMSI.MMSI from passengerMMSI);')
        self.cursor.execute(f'INSERT INTO merchantMMSI SELECT tempmerchantMMSI.MMSI, tempmerchantMMSI.CargoType FROM tempmerchantMMSI WHERE tempmerchantMMSI.MMSI NOT IN (SELECT merchantMMSI.MMSI from merchantMMSI);')

        
        ## drop temporary MMSI files
        self.cursor.execute('DROP TABLE IF EXISTS tempfishingMMSI;')
        self.cursor.execute('DROP TABLE IF EXISTS temppassengerMMSI;')
        self.cursor.execute('DROP TABLE IF EXISTS tempmerchantMMSI;')

    def drop_unrealistic_courses(self):
        self.connectSQLite()
        self.cursor.execute(f'SELECT COUNT(*) FROM {self.tablenameAISdata};')
        beforesampleSize = self.cursor.fetchall()

        self.cursor.execute(f'DELETE FROM {self.tablenameAISdata} WHERE COG < 0 OR COG > 360;')
        
        # Check if the import was successful
        self.cursor.execute(f'SELECT COUNT(*) FROM {self.tablenameAISdata};')
        aftersampleSizeAIS = self.cursor.fetchall()
        print(f'from {beforesampleSize} to {aftersampleSizeAIS} Samples in {self.tablenameAISdata} by dropping COG < 0 OR COG > 360;')
        self.disconnectSQLite()

    def get_depth(self, longitude, latitude):
        # Open the bathymetry GeoTIFF file
        bathymetry_data = rasterio.open(self.bathymetry_file)

        row, col = bathymetry_data.index(longitude, latitude)
        depth = bathymetry_data.read(1)[row, col]
        return depth

    def add_waterdepth(self):
        # Loop through remaining samples in database
        # use execute_many
        pass

    def drop_unrealistic_loc(self, latMinBoundary, latMaxBoundary, lonMinBoundary, lonMaxBoundary):
        self.connectSQLite()
        self.cursor.execute(f'SELECT COUNT(*) FROM {self.tablenameAISdata};')
        beforesampleSize = self.cursor.fetchall()

        self.cursor.execute(f'DELETE FROM {self.tablenameAISdata} WHERE Latitude < {latMinBoundary} OR Latidude > {latMaxBoundary} OR Longitude < {lonMinBoundary} OR Longitude > {lonMaxBoundary};')
        
        # Check if the import was successful
        self.cursor.execute(f'SELECT COUNT(*) FROM {self.tablenameAISdata};')
        aftersampleSizeAIS = self.cursor.fetchall()
        print(f'from {beforesampleSize} to {aftersampleSizeAIS} Samples in {self.tablenameAISdata} by dropping outbound locations')

        self.disconnectSQLite()

    def drop_unrealistic_speeds(self):
        self.connectSQLite()
        # CODE
        self.disconnectSQLite()
 
    def drop_unrealistic_mmsi(self, lenMMSI):
        self.connectSQLite()
        self.cursor.execute(f'SELECT COUNT(*) FROM {self.tablenameAISdata};')
        beforesampleSize = self.cursor.fetchall()

        self.cursor.execute(f'DELETE FROM {self.tablenameAISdata} WHERE length(MMSI) <> {lenMMSI};')
        
        # Check if the import was successful
        self.cursor.execute(f'SELECT COUNT(*) FROM {self.tablenameAISdata};')
        aftersampleSizeAIS = self.cursor.fetchall()
        print(f'from {beforesampleSize} to {aftersampleSizeAIS} Samples in {self.tablenameAISdata} by dropping unrealistc MMSI of not {lenMMSI}')

        self.disconnectSQLite()
 
    def fill_statics(self):
        self.connectSQLite()
        # CODE
        self.disconnectSQLite()
 
    def comp_distance(self):
        self.connectSQLite()
        # CODE
        self.disconnectSQLite()
    
    def comp_timedelta(self):
        self.connectSQLite()
        # CODE
        self.disconnectSQLite()
 
    def comp_speed(self):
        self.connectSQLite()
        # CODE
        self.disconnectSQLite()
 
 
    def drop_list(self, drop_list):
         self.connectSQLite()
         for column in drop_list:
             self.cursor.execute(f'ALTER TABLE {self.tablenameAISdata} DROP IF EXISTS {column};')
         self.disconnectSQLite()


    def extractCSV(self, vesselType:str='fishing'):
        '''
        Filter data on MMSI per class using SQL
        i: vesselType: str -> {fishing, passenger, merchant}
        o: csv file
        '''
        ## TODO: CHECK IF CONNECTION EXISTS, else connect

        ### create OUTPUT file

        pass