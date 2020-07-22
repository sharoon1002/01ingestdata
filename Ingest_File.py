import urllib.request
import ssl
import os     # To read operating system.
import csv  # To import csv file.
import zipfile
import os
import glob
import datetime as dt
import os
import argparse
from google.cloud import storage
from flask import escape

#STEP1 #Download data from the BTS website to a local file.
def download(Year,Month):
    ctx_no_secure = ssl.create_default_context()
    ctx_no_secure.set_ciphers('HIGH:!DH:!aNULL') # 'openssl' cmd ciphers to see list of ciphers
    ctx_no_secure.check_hostname = False   #Set True to verify Hostname
    ctx_no_secure.verify_mode = ssl.CERT_NONE    # CERT_REQUIRED to verify Hostname

    url="https://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=236&Has_Group=3&Is_Zipped=0"
    data="UserTableName=On_Time_Performance&DBShortName=&RawDataTable=T_ONTIME&sqlstr=+SELECT+FL_DATE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CCARRIER%2CFL_NUM%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CCRS_DEP_TIME%2CDEP_TIME%2CDEP_DELAY%2CTAXI_OUT%2CWHEELS_OFF%2CWHEELS_ON%2CTAXI_IN%2CCRS_ARR_TIME%2CARR_TIME%2CARR_DELAY%2CCANCELLED%2CCANCELLATION_CODE%2CDIVERTED%2CDISTANCE+FROM++T_ONTIME+WHERE+Month+%3D{1}+AND+YEAR%3D{0}&varlist=FL_DATE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CCARRIER%2CFL_NUM%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CCRS_DEP_TIME%2CDEP_TIME%2CDEP_DELAY%2CTAXI_OUT%2CWHEELS_OFF%2CWHEELS_ON%2CTAXI_IN%2CCRS_ARR_TIME%2CARR_TIME%2CARR_DELAY%2CCANCELLED%2CCANCELLATION_CODE%2CDIVERTED%2CDISTANCE&grouplist=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time=March&timename=Month&GEOGRAPHY=All&XYEAR={0}&FREQUENCY=3&VarDesc=Year&VarType=Num&VarDesc=Quarter&VarType=Num&VarDesc=Month&VarType=Num&VarDesc=DayofMonth&VarType=Num&VarDesc=DayOfWeek&VarType=Num&VarName=FL_DATE&VarDesc=FlightDate&VarType=Char&VarName=UNIQUE_CARRIER&VarDesc=UniqueCarrier&VarType=Char&VarName=AIRLINE_ID&VarDesc=AirlineID&VarType=Num&VarName=CARRIER&VarDesc=Carrier&VarType=Char&VarDesc=TailNum&VarType=Char&VarName=FL_NUM&VarDesc=FlightNum&VarType=Char&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarDesc=OriginCityName&VarType=Char&VarDesc=OriginState&VarType=Char&VarDesc=OriginStateFips&VarType=Char&VarDesc=OriginStateName&VarType=Char&VarDesc=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID&VarType=Num&VarName=DEST&VarDesc=Dest&VarType=Char&VarDesc=DestCityName&VarType=Char&VarDesc=DestState&VarType=Char&VarDesc=DestStateFips&VarType=Char&VarDesc=DestStateName&VarType=Char&VarDesc=DestWac&VarType=Num&VarName=CRS_DEP_TIME&VarDesc=CRSDepTime&VarType=Char&VarName=DEP_TIME&VarDesc=DepTime&VarType=Char&VarName=DEP_DELAY&VarDesc=DepDelay&VarType=Num&VarDesc=DepDelayMinutes&VarType=Num&VarDesc=DepDel15&VarType=Num&VarDesc=DepartureDelayGroups&VarType=Num&VarDesc=DepTimeBlk&VarType=Char&VarName=TAXI_OUT&VarDesc=TaxiOut&VarType=Num&VarName=WHEELS_OFF&VarDesc=WheelsOff&VarType=Char&VarName=WHEELS_ON&VarDesc=WheelsOn&VarType=Char&VarName=TAXI_IN&VarDesc=TaxiIn&VarType=Num&VarName=CRS_ARR_TIME&VarDesc=CRSArrTime&VarType=Char&VarName=ARR_TIME&VarDesc=ArrTime&VarType=Char&VarName=ARR_DELAY&VarDesc=ArrDelay&VarType=Num&VarDesc=ArrDelayMinutes&VarType=Num&VarDesc=ArrDel15&VarType=Num&VarDesc=ArrivalDelayGroups&VarType=Num&VarDesc=ArrTimeBlk&VarType=Char&VarName=CANCELLED&VarDesc=Cancelled&VarType=Num&VarName=CANCELLATION_CODE&VarDesc=CancellationCode&VarType=Char&VarName=DIVERTED&VarDesc=Diverted&VarType=Num&VarDesc=CRSElapsedTime&VarType=Num&VarDesc=ActualElapsedTime&VarType=Num&VarDesc=AirTime&VarType=Num&VarDesc=Flights&VarType=Num&VarName=DISTANCE&VarDesc=Distance&VarType=Num&VarDesc=DistanceGroup&VarType=Num&VarDesc=CarrierDelay&VarType=Num&VarDesc=WeatherDelay&VarType=Num&VarDesc=NASDelay&VarType=Num&VarDesc=SecurityDelay&VarType=Num&VarDesc=LateAircraftDelay&VarType=Num&VarDesc=FirstDepTime&VarType=Char&VarDesc=TotalAddGTime&VarType=Num&VarDesc=LongestAddGTime&VarType=Num&VarDesc=DivAirportLandings&VarType=Num&VarDesc=DivReachedDest&VarType=Num&VarDesc=DivActualElapsedTime&VarType=Num&VarDesc=DivArrDelay&VarType=Num&VarDesc=DivDistance&VarType=Num&VarDesc=Div1Airport&VarType=Char&VarDesc=Div1AirportID&VarType=Num&VarDesc=Div1AirportSeqID&VarType=Num&VarDesc=Div1WheelsOn&VarType=Char&VarDesc=Div1TotalGTime&VarType=Num&VarDesc=Div1LongestGTime&VarType=Num&VarDesc=Div1WheelsOff&VarType=Char&VarDesc=Div1TailNum&VarType=Char&VarDesc=Div2Airport&VarType=Char&VarDesc=Div2AirportID&VarType=Num&VarDesc=Div2AirportSeqID&VarType=Num&VarDesc=Div2WheelsOn&VarType=Char&VarDesc=Div2TotalGTime&VarType=Num&VarDesc=Div2LongestGTime&VarType=Num&VarDesc=Div2WheelsOff&VarType=Char&VarDesc=Div2TailNum&VarType=Char&VarDesc=Div3Airport&VarType=Char&VarDesc=Div3AirportID&VarType=Num&VarDesc=Div3AirportSeqID&VarType=Num&VarDesc=Div3WheelsOn&VarType=Char&VarDesc=Div3TotalGTime&VarType=Num&VarDesc=Div3LongestGTime&VarType=Num&VarDesc=Div3WheelsOff&VarType=Char&VarDesc=Div3TailNum&VarType=Char&VarDesc=Div4Airport&VarType=Char&VarDesc=Div4AirportID&VarType=Num&VarDesc=Div4AirportSeqID&VarType=Num&VarDesc=Div4WheelsOn&VarType=Char&VarDesc=Div4TotalGTime&VarType=Num&VarDesc=Div4LongestGTime&VarType=Num&VarDesc=Div4WheelsOff&VarType=Char&VarDesc=Div4TailNum&VarType=Char&VarDesc=Div5Airport&VarType=Char&VarDesc=Div5AirportID&VarType=Num&VarDesc=Div5AirportSeqID&VarType=Num&VarDesc=Div5WheelsOn&VarType=Char&VarDesc=Div5TotalGTime&VarType=Num&VarDesc=Div5LongestGTime&VarType=Num&VarDesc=Div5WheelsOff&VarType=Char&VarDesc=Div5TailNum&VarType=Char".format(Year,Month)

    if os.path.exists('/tmp'):
        print("Download File Path exist")
    else:
        os.mkdir('/tmp')
    filename= os.path.join('/tmp',"{}{}.zip".format(Year, Month))  #Giving name to file and directory path
    with open(filename,'wb') as fil:  # withos.mkdir('C:\WEBDOWNLOADFILE')command no need to close file and wb is used to create and edit file.
        response = urllib.request.urlopen(url,data.encode("utf-8"),context=ctx_no_secure) #urlopen to invoke and extract data.
        fil.write(response.read())  # writing into file. 
        


# In[137]:

#STEP2#Here’s how to unzip the file and extract the CSV contents:
def unzipfile(Year,Month,unzipfldr):
    filename= os.path.join('/tmp',"{}{}.zip".format(Year, Month))
    with zipfile.ZipFile(filename,'r') as zpfile:           #Read ZipFile using function ZipFile
        if os.path.exists(unzipfldr):
            print("Unzip File Path exist")
        else:
            os.mkdir(unzipfldr)
        zpfile.extractall(unzipfldr)    #unzip file tctime)


# In[138]:

#STEP3#Here’s how to remove quotes and the trailing comma from csv files and store it in separate folder:
def removefilequotes(Year,Month,unzipfldr,remquotes):
    listfile = glob.glob(os.path.join(unzipfldr,'*.csv')) #glob function return the specified extension of file list.
    latest_file = max(listfile , key = os.path.getctime) #
    if os.path.exists(remquotes):
        print("Removequotes File Path Exist")            
    else:
        os.mkdir(remquotes)
    outfile = os.path.join(remquotes,'{}{}.csv'.format(Year,Month))  # Data to be cleaned and stored in destination director.
    print("The Path of CSV FILE IS :",latest_file)
    with open(outfile,'w') as wrtcsvfile:   # Writing into destination directory
        with open(latest_file,'r') as rdcsvfile:
            for lines in rdcsvfile:
                removestrips = lines.rstrip().rstrip(',').translate('"')
                wrtcsvfile.write(removestrips)
                wrtcsvfile.write('\n')                


# In[139]:


#STEP4#Verify (1) that it has more than one line, #(and (2) that the CSV file has the exact header we expect:)
def verifyfile(remquotes):
    listfile = glob.glob(os.path.join(remquotes,'*.csv')) #glob function return the specified extension of file list.
    latest_file = max(listfile , key = os.path.getctime)
  
    print(latest_file)
    filesize =os.path.getsize(latest_file)  # get file size.
    print(filesize)
    csvfilesize = os.stat(latest_file).st_size > 1024   #Returns true or false# 1kb=1024 bytes
    print(csvfilesize)
    if(csvfilesize is True):
        print("File is not empty")
    else:
        print("File is empty")
        os.remove(latest_file)
        print("File removed successfully")

# In[140]:

#STEP5#Here’s the code to upload the CSV file for a given month to Cloud Storage:
##client = storage.Client()
def uploadfile(Year,Month,remquotes):
    csvfile = os.path.join(remquotes,"{}{}.csv".format(Year,Month))    
    if os.path.exists(csvfile):
        print("Start uploading")
        client = storage.Client()
        #from_service_account_json("My Project 40276-d203c8401ab2.json") 
        #Above file is created from GCP Service account and require when run on local machine.
        bucketname = client.get_bucket('haroondatacsv_01') #extract the bucket name.
        print("Bucket Name :" ,bucketname)
        blob = storage.Blob('Practise/{}{}.csv'.format(Year,Month),bucketname)  # blobname and bucketname.
        blob.upload_from_filename(csvfile) # unzip and quote removed downloaded file.
        print("successfully uploaded")
    else:
        print("File is Empty and removed")
# %%
#Step 6# Execute ALL 5 step in ONE function:
def ingest_upload(Year,Month):
    unzipfldr = '/tmp/ZIPTEST' #destination unzip directory.
    remquotes = '/tmp/REMOVEQUOTES'
    download(Year,Month)
    unzipfile(Year,Month,unzipfldr)
    removefilequotes(Year,Month,unzipfldr,remquotes)
    verifyfile(remquotes)
    return uploadfile(Year,Month,remquotes) # To define step7 return is required

#Step7# Main Program executed if this is run on the command line:
if __name__ == '__main__':
    Parser = argparse.ArgumentParser()
    Parser.add_argument('--Year')
    Parser.add_argument('--Month') 
    try:
        args=Parser.parse_args()
        if args.Year is None or args.month is None:
            d1 = dt.date.today()
            Year =d1.year 
            Backdated_Month = d1.month - 3  # Minus 3 Month File from date
            Month='%02d'%Backdated_Month    # Leading zero 
        else:
            Year =args.Year
            Month =args.Month
        ingest_upload(Year,Month)
        print ('Success ... ingested to {}{}'.format(Year,Month))
    except Exception as e:
        print ('Try again later: {}'.format(e))
        
#Main Program executed if this program is run on Cloud Function.
def flask_request(request):
    try:
        json = request.get_json(silent=True,force=True)  #Silent parsing otherwise none and force ignore the content type.
        request_args = request.args

        if json and 'Year' and 'Month' in json:    # This require for JSON content
            Year = escape(json['Year'])
            Month = escape(json['Month'])
        elif request_args and 'Year' and 'Month' in request_args: # This requires for String url
             Year = request_args['Year']
             Month = request_args['Month']
        else:
            d1 = dt.date.today()    # Else any of above is not passed then else condition
            d1Year =d1.year 
            Backdated_Month = d1.month - 3  # Minus 3 Month File from date
            d1Month='%02d'%Backdated_Month    # Leading zero 
            Year,Month  = d1Year,d1Month    

        ingest_upload(Year,Month)
        print ('Success ... ingested to {}{}'.format(Year,Month))
    except Exception as e:      
        print ('Try again later using flask: {}'.format(e))
