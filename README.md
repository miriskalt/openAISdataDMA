# openAISdataDMA
A pipeline for generating AIS data sets with historic messages from a selected vessel class
The data is openly available at http://web.ais.dk/aisdata/. 
The corresponding paper to this work is: 
Mirjam Bayer, Tabea Fry, Sören Dethlefsen, and Daniyal Kazempour. 2023. Pipeline for open AIS data with filtering based on vessel class. In Proceedings of the 1st ACM SIGSPATIAL International Workshop on AI-driven Spatio-temporal Data Analysis for Wildlife Conservation (GeoWildLife '23). Association for Computing Machinery, New York, NY, USA, 21–24. https://doi.org/10.1145/3615893.3628758 

### Please keep in mind
When you download data, please note that there are some limitations to the upload capacity from the server. This means that if someone is trying to download all available data at once, the system might "stall".

## The features of AIS messages

Columns in *.csv file			Format
----------------------------------------------------------------------------------------------------------------------------------------------------
1.	Timestamp			Timestamp from the AIS basestation, format: 31/12/2015 23:59:59	
2.	Type of mobile			Describes what type of target this message is received from (class A AIS Vessel, Class B AIS vessel, etc)
3.	MMSI				MMSI number of vessel
4.	Latitude			Latitude of message report (e.g. 57,8794)
5.	Longitude			Longitude of message report (e.g. 17,9125)
6.	Navigational status		Navigational status from AIS message if available, e.g.: 'Engaged in fishing', 'Under way using engine', mv.
7.	ROT				Rot of turn from AIS message if available
8.	SOG				Speed over ground from AIS message if available
9.	COG				Course over ground from AIS message if available
10.	Heading			Heading from AIS message if available
11.	IMO				IMO number of the vessel
12.	Callsign			Callsign of the vessel 
13.	Name				Name of the vessel
14.	Ship type			Describes the AIS ship type of this vessel 
15.	Cargo type			Type of cargo from the AIS message 
16.	Width				Width of the vessel
17.	Length				Lenght of the vessel 
18.	Type of position fixing device	Type of positional fixing device from the AIS message 
19.	Draught			Draugth field from AIS message
20.	Destination			Destination from AIS message
21.	ETA				Estimated Time of Arrival, if available  
22.	Data source type		Data source type, e.g. AIS
23. Size A				Length from GPS to the bow
24. Size B				Length from GPS to the stern
25. Size C				Length from GPS to starboard side
26. Size D				Length from GPS to port side

Source: http://web.ais.dk/aisdata/

## The flags for the pipeline

#### Crawling
Start date: str format 'YYYY-MM-DD'
End date: str format 'YYYY-MM-DD' default None


#### Filtering
Class: str {'fishing', 'passenger', 'merchant'}



#### Preprocessing/ Cleaning

