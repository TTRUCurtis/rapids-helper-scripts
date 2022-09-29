##############################################################################
#
#  create_multiple_timezones.py
#
#  This file is supposed to take data from an aware_dashboard database's 
#  aware_device table and a timezones_surveys table 
# (perhaps taken from Qualtrics), and create a multiple_timezones.csv that will 
# be able to be read by the RAPIDS data analysis program for specifying multiple
# timezones for different participants.    
# 
# 
#  Created by Zachary Fried for the TTRU group of NIDA/NIH
#  Use this file at your own risk.  The author makes no claims about suitablilty
#  for any purpose real or imagined.
#
################################################################################


# import the modules
import pandas as pd
import sqlalchemy as db
import numpy as np
from pathlib import Path
import getopt
import sys
import os 

def usage():
    print("Error: Unknow options.  Please run with the correct arguments:")
    print("python create_multiple_timezones.py  --database <database name> --device_source_table <tablename> --survey_source_table <tablename> --survey_col_name <name of column in timezones_survey that matches label column of aware_device table> ")
    print("                                         [--mysqlconfig <.my.cnf location>] [--destination_file <full path of output file>] [--default_tz <time zone setting>], [--participant_directory <full path of participant_files directory>]")


def main():

    try:
        optlist, args = getopt.getopt(sys.argv[1:], "", ["mysqlconfig=", "database=", "device_source_table=", "survey_source_table=", "survey_col_name=", "destination_file=", "default_tz=", "participant_directory="])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    #Set default options   
    options = {}
    options["mysqlconfig"] = str(str(Path.home()) + '/.my.cnf')
    options["destination_file"] = "../../../data/external/multiple_timezones.csv"
    options["default_tz"] = "no_default"
    options["participant_directory"] = "../../../data/external/participant_files/"
    custom_tz_message = ""
    
    # OVERRIDE GENERAL DEFAULTS WITH COMMAND LINE ARGUMENTS
    for option_tuple in optlist:    
        if (option_tuple[0] == "--mysqlconfig"):
            options["mysqlconfig"] = option_tuple[1]
        elif (option_tuple[0] == "--database"):
            options["database"] = option_tuple[1]        
        elif (option_tuple[0] == "--device_source_table"):
            options["device_source_table"] = option_tuple[1]
        elif (option_tuple[0] == "--survey_source_table"):
            options["survey_source_table"] = option_tuple[1]
        elif (option_tuple[0] == "--survey_col_name"):
            options["survey_col_name"] = option_tuple[1]
        elif (option_tuple[0] == "--destination_file"):
            options["destination_file"] = option_tuple[1]
        elif (option_tuple[0] == "--default_tz"):
            options["default_tz"] = option_tuple[1]
        elif (option_tuple[0] == "--participant_directory"):
            options["participant_directory"] = option_tuple[1]

    if (not "database" in options or not "device_source_table" in options or not "survey_source_table" in options or not "survey_col_name" in options):
        usage()
        exit(2)    

    # Force Connection type by using Loopback IP address.
    myDB = db.engine.URL.create(drivername="mysql", host="127.0.0.1", database = options["database"], query={"read_default_file" : options["mysqlconfig"]})

    #engine = db.create_engine(name_or_url=myDB, pool_pre_ping=True)            
    engine = db.create_engine(myDB, pool_pre_ping=True)

    # create PANDAS dataframes from SQL tables, converting time_zone column to stings for next step
    device_df = pd.read_sql_table(options["device_source_table"], engine)
    survey_df = pd.read_sql_table(options["survey_source_table"], engine)
    joined_df = pd.merge(device_df, survey_df, left_on='label', right_on=options["survey_col_name"])
    joined_df['time_zone'] = joined_df['time_zone'].astype(str)

    #time_zone column which represents timezones as integers formated as strings is used to create column tzcode
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('1'), 'America/Puerto_Rico', 'TBD')
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('2'), 'America/New_York', joined_df['tzcode'])
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('3'), 'America/Chicago', joined_df['tzcode'])
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('4'), 'America/Denver', joined_df['tzcode'])
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('5'), 'America/Los_Angeles', joined_df['tzcode'])
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('6'), 'America/Anchorage', joined_df['tzcode'])
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('7'), 'Pacific/Honolulu', joined_df['tzcode'])
    if options["default_tz"] == "no_default": #User wants to pass all participants to RAPIDS and assumes there are no missing time zone data. Exception raised if there are missing data, passes if there are none. 
        if joined_df["tzcode"].str.contains('TBD').sum() > 0:
            print("ERROR: There was at least one missing value in the time_zone column. You have not set a default time zone to replace null values. Please set a default TZ database name to replace null values or use argument 'ignore' to skip participants with no time_zone data. A list of TZ database names can be found at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones")
            exit(2)
        else:
            pass
    elif options["default_tz"] == "ignore": #User wants to use RAPIDS while ignoring participants for which there is missing time zone data. Deletes .yaml files in participant_files folder for these participants. 
        to_delete = [] 
        for index, row in joined_df.iterrows():
            if row['tzcode'] == 'TBD':
                to_delete.append(row['label'])
        for i in to_delete:
            file_name = options["participant_directory"] + i + ".yaml"
            if os.path.exists(file_name):
                os.remove(file_name)
            else:
                pass
        print("Ignored " + str(len(to_delete)) + " participants without time zone data. Their participant files have been removed. Please re-run create_participant_files.py if you wish to include these participants in the future.")
    else: #User is specififying a custom TZ database name to use for participants with missing time zone data.
        custom_tz_message = "You have set custom TZ database name of: " + options["default_tz"] + ". Please ensure this is a valid time zone in the TZ database before proceeding to use of RAPIDS. A list of TZ database names can be found at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"
        joined_df['tzcode'] = np.where(joined_df['tzcode'].str.startswith('TBD'), options["default_tz"], joined_df['tzcode'])

    #A dataframe is created with the columns of interest for RAPIDS and is exported as a csv to '/rapids/data/external/multiple_timezones.csv'
    final_df = joined_df[['device_id', 'tzcode', 'timestamp']]
    final_df.to_csv(options["destination_file"], index=False)
    if len(custom_tz_message) > 0:
        print(custom_tz_message)
    print("Successfully created /rapids/data/external/multiple_timezones.csv. Please change file path field in [TIMEZONE][TZCODES_FILE] config.yaml as needed.")

if __name__ == "__main__":

    main()