##############################################################################
#
#  create_participant_file.py
#
#  This file is supposed to take data from an aware_dashboard database's 
#  aware_device table, and create a participant.csv file that will be able
#  to be read by the rapids data analysis program.  
#
#  Created by Douglas Bellew for the TTRU group of NIDA/NIH
#  Use this file at your own risk.  The author makes no claims about suitablilty
#  for any purpose real or imagined.
#
################################################################################

import sys
import os
import getopt
import datetime as dt
from pathlib import Path
import sqlalchemy as db
import pandas as pd
import csv

def usage():
    print("Error: Unknow options.  Please run the following to set the environment and correct arguments:")
    print("conda activate rapids_r4_0")
    print("python create_rapids_participant_file.py  --database <database name> --source_table <tablename> ")
    print("                                         [--mysqlconfig <.my.cnf location>] [--destination_file <full path of output file>]")
    
    
def main():

    try:
        optlist, args = getopt.getopt(sys.argv[1:], "", ["mysqlconfig=", "database=", "source_table=", "destination_file="])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
        
    csv.field_size_limit(sys.maxsize)
    now = dt.datetime.now()
    print("Starting create_rapids_participant_file.py: "+str(now))
    options = {}
    options["mysqlconfig"] = str(str(Path.home()) + '/.my.cnf')
    #options["database"] = "douglasvbellew"
    #options["source_table"] = 'aware_device_may'
    options["destination_file"] = "../../data/external/participant_data.csv"
    
    # OVERRIDE GENERAL DEFAULTS WITH COMMAND LINE ARGUMENTS
    for option_tuple in optlist:    
        if (option_tuple[0] == "--mysqlconfig"):
            options["mysqlconfig"] = option_tuple[1]
        elif (option_tuple[0] == "--database"):
            options["database"] = option_tuple[1]        
        elif (option_tuple[0] == "--source_table"):
            options["source_table"] = option_tuple[1]
        elif (option_tuple[0] == "--destination_file"):
            options["destination_file"] = option_tuple[1]
    
    if (not "database" in options or not "source_table" in options):
        usage()
        exit(2)    
        
        
    # chokes on "localhost" (Possible remote ssh issue?).  Force Connection type by using Loopback IP address.
    myDB = db.engine.url.URL(drivername="mysql",
                            host="127.0.0.1",
                            database = options["database"],
                            query={"read_default_file" : options["mysqlconfig"]})

    #engine = db.create_engine(name_or_url=myDB, pool_pre_ping=True)            
    engine = db.create_engine(myDB, pool_pre_ping=True)
    
    result_set = {}
    with engine.connect() as connection:
        metadata  = db.MetaData()
        #orig_data = db.Table('kb_test_data', metadata, autoload=True, autoload_with=engine)  
        data_table = db.Table(str(options["source_table"]), metadata, autoload=True, autoload_with=engine)  
        #FBCHANGE NO ID COLUMN       
        #query = db.select([orig_data]).where(orig_data.columns._id > options["last_processed_id"]).offset(options["batches_complete"] * options["batch_fetch"]).limit(options["batch_fetch"])
        query = db.select([data_table])
        result_proxy = connection.execute(query)
        result_set = result_proxy.fetchall()
    
    
    if (len(result_set) > 0):
    # Put Result into PANDAS dataframe
        pd.set_option("max_colwidth",30)
        pd.set_option("large_repr", "truncate")
        pd.set_option("display.width", None)
        df = pd.DataFrame(result_set)
        df.columns = result_set[0].keys()
        print("Retrieved "+ str(len(df))+ " aware_device table rows.")
        
        participants = {}
        combined = 0
        for current_row in range(len(df)):
            label = str(df.at[current_row, "label"]).replace("'","")
            if label in participants:
                participants[label]["device_id"] = participants[label]["device_id"] + ";" + df.at[current_row, "device_id"]
                combined = combined + 1
            else:
                participant = {}
                participant["device_id"] = df.at[current_row, "device_id"]
                participant["fitbit_id"] = ""
                participant["empatica_id"] = ""
                participant["pid"] = str(df.at[current_row, "label"]).replace("'","")
                participant["label"] = str(df.at[current_row, "label"]).replace("'","")
                if (df.at[current_row, "model"] == "iPhone"):
                    participant["platform"] = "ios"
                else:
                    participant["platform"] = "android"
                participant["start_date"] =  dt.datetime.fromtimestamp(int(df.at[current_row, "timestamp"]//1000)).strftime("%Y-%m-%d")
                participant["end_date"] = now.strftime("%Y-%m-%d")
                participants[label] = participant
        print("Combined "+str(combined)+" rows due to label matches")
        path,filename = os.path.split(options["destination_file"])
        # If output directory doesn't exist... make it.
        if not os.path.exists(path):
            print("Warning - directory: "+path+ " does not exist.  Creating it.")
            os.makedirs(path)
            
        # Rapids participant aware_csv file format:
        #   device_id,fitbit_id,empatica_id,pid,label,platform,start_date,end_date
        # From Sal Requirements:
        #   device_id a list of device-ids per person separated with ;
        #   platform will be either ios or android
        #   pid the label column from aware_device
        #   label the label column for aware_device
        
        with open(options["destination_file"],'w') as csvout:
            writer = csv.writer(csvout,delimiter=',')
            writer.writerow(["device_id","fitbit_id","empatica_id","pid","label","platform","start_date","end_date"])
            write_count = 0
            for participant in participants:
                write_count = write_count + 1
                writer.writerow([participants[participant]["device_id"],
                                participants[participant]["fitbit_id"],
                                participants[participant]["empatica_id"],
                                str(participants[participant]["pid"]).replace("'",""),
                                str(participants[participant]["label"]).replace("'",""),
                                participants[participant]["platform"],
                                participants[participant]["start_date"],
                                participants[participant]["end_date"]])
            print("Created "+str(len(participants.keys()))+" participant entries.")
            print("Wrote "+str(write_count)+" participant entries.")
if __name__ == "__main__":

    main()

                
   
   