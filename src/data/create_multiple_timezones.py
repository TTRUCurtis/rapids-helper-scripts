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

def usage():
    print("Error: Unknow options.  Please run with the correct arguments:")
    print("python create_multiple_timezones.py  --database <database name> --device_source_table <tablename> --survey_source_table <tablename> --survey_col_name <name of col in survey source table that matches 'label' col of device source table> ")
    print("                                         [--mysqlconfig <.my.cnf location>] [--destination_file <full path of output file>] ")
    print("                                         [--tz_default <default for missing time zone data>] [--participant_input <path to participant file CSV>] [--participant_output <desired path for modified participant CSV>]")


def main():

    try:
        optlist, args = getopt.getopt(sys.argv[1:], "", ["mysqlconfig=", "database=", "device_source_table=", "survey_source_table=", "survey_col_name=", "destination_file=", "tz_default=", "participant_input=", "participant_output="])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    # Set default options
    print("Creating multiple_timezones.csv...")    
    options = {}
    options["mysqlconfig"] = str(str(Path.home()) + '/.my.cnf')
    options["destination_file"] = "../../../data/external/multiple_timezones.csv"
    options["tz_default"] = "remove"
    options["participant_input"] = "../../../data/external/participant_file.csv"
    options["participant_output"] = "../../../data/external/participant_file_modified.csv"
    
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
        elif (option_tuple[0] == "--tz_default"):
            options["tz_default"] = option_tuple[1]
        elif (option_tuple[0] == "--participant_input"):
            options["participant_input"] = option_tuple[1]
        elif (option_tuple[0] == "--participant_output"):
            options["participant_output"] = option_tuple[1]

    if (not "database" in options or not "device_source_table" in options or not "survey_source_table" in options or not "survey_col_name" in options):
        usage()
        exit(2)    

    # Force Connection type by using Loopback IP address.
    myDB = db.engine.URL.create(drivername="mysql", host="127.0.0.1", database = options["database"], query={"read_default_file" : options["mysqlconfig"]})

    # engine = db.create_engine(name_or_url=myDB, pool_pre_ping=True)            
    engine = db.create_engine(myDB, pool_pre_ping=True)

    # create PANDAS dataframes from SQL tables
    survey_df = pd.read_sql_table(options["survey_source_table"], engine) # particiapt label, time_zone integer
    participant_df = pd.read_csv(options["participant_input"]) #  participant label, device_id
    device_df = pd.read_sql_table(options["device_source_table"], engine, columns=['device_id', 'timestamp']) # device_id, timestamp

    # Create table with device_id, participant label, and time_zone integer
    joined_nostamp_df = pd.merge(participant_df, survey_df, left_on="label", right_on=options["survey_col_name"], how='left')

    # Address the fact that some rows in joined_nostamp_df have multiple device_ids per row by creating a dataframe with one row per device_id which is then appended to joined_nostamp_df. Duplicates deleted later. 
    to_add_df = pd.DataFrame(columns=list(joined_nostamp_df.columns))
    for index, row in joined_nostamp_df.iterrows():
        id_list = row["device_id"].split(';')
        if len(id_list) > 1:
            for id in id_list:
                to_add_df = to_add_df.append({'device_id' : id, 'pid' : row['pid'], 'label' : row['label'], 'platform' : row['platform'], 'start_date' : row['start_date'], 'end_date' : row['end_date'], 'eid' : row['eid'], 'time_zone' : row['time_zone'], 'time_zone_text' : row['time_zone_text']}, ignore_index = True)
        else:
            pass
    joined_nostamp_df = joined_nostamp_df.append(to_add_df)
    
    # Adds timestamp data from device_df 
    joined_df = pd.merge(joined_nostamp_df, device_df, how='left', on='device_id') 

    # time_zone integers converted to strings
    joined_df['time_zone'] = joined_df['time_zone'].astype(str)

    # Column tzcode is populated with tz codes based on value of time_zone
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('1'), 'America/Puerto_Rico', 'TBD')
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('2'), 'America/New_York', joined_df['tzcode'])
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('3'), 'America/Chicago', joined_df['tzcode'])
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('4'), 'America/Denver', joined_df['tzcode'])
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('5'), 'America/Los_Angeles', joined_df['tzcode'])
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('6'), 'America/Anchorage', joined_df['tzcode'])
    joined_df['tzcode'] = np.where(joined_df['time_zone'].str.startswith('7'), 'Pacific/Honolulu', joined_df['tzcode'])

    # tz_default option applied in following if loop:
    # Remove participants from participant CSV file that do not have time zone data. Create multiple_timezone.csv including only remaining participants. 
    if options["tz_default"] == "remove": 

        # Create df that acts as a reference of device_ids that have a time zone specified in the survey data
        ids_with_tz = pd.merge(participant_df, survey_df, left_on="label", right_on=options["survey_col_name"])

        # Remove rows from particiant_df where there is a device_id without a time zone code
        participant_df['drop_bool'] = 0
        for index, row in participant_df.iterrows():
            id_list = row["device_id"].split(';')
            for id in id_list:
                if ids_with_tz['device_id'].str.contains(id).any() == False: # now I need to make it so that it checks a dataframe or list of ids that isn't all of participant ids 
                    participant_df.loc[index, 'drop_bool'] += 1
                else:
                    pass
        participant_df = participant_df[participant_df['drop_bool'] == 0]
        participant_df = participant_df.drop('drop_bool', axis = 1)

        # Create new participant file with only participants that have time zones specified in the survey table
        participant_df.to_csv(options["participant_output"], index=False)

        # Remove rows from joined_df where there is a device_id without a time zone code
        joined_df['drop_bool'] = 0
        for index, row in joined_df.iterrows():
            id_list = row["device_id"].split(';')
            for id in id_list:
                if ids_with_tz['device_id'].str.contains(id).any() == False: # now I need to make it so that it checks a dataframe or list of ids that isn't all of joined ids 
                    joined_df.loc[index, 'drop_bool'] += 1
                else:
                    pass
        joined_df = joined_df[joined_df['drop_bool'] == 0]
        joined_df = joined_df.drop('drop_bool', axis = 1).drop_duplicates(['device_id'], keep='first')
        
        message = "A new participant CSV file has been saved as " + options["destination_file"] + ". Indicate this location in config.yaml under [CREATE_PARTICIPANT_FILES][CSV_FILE_PATH] before creating participant files with RAPIDS."

    # Simply print how many participants there are in the participant file with missing time zone data. 
    elif options["tz_default"] == "ignore": 

        #Checks to see if there are any participants with missing tzcodes. 
        if (joined_df["tzcode"].eq('TBD')).any() == True:
            message = "There were " + str(joined_df["tzcode"].eq('TBD').sum()) + " participants with missing tz codes. You will need to change values of [IF_MISSING_TZCODE] and/or [DEFAULT_TZCODE] under [TIMEZONE][MULTIPLE] in config.yaml. Refer to https://www.rapids.science/1.9/setup/configuration/#timezone-of-your-study for reference."
            joined_df = joined_df[joined_df.tzcode != 'TBD']
        else:
            message = "There were no participants with missing tz codes."
        
        #Drop duplicate rows which occurs for participants that responded to the study survey multiple times
        joined_df = joined_df.drop_duplicates(['device_id'], keep='first')

    # Fills missing tzcodes with custom value from user
    else:
        joined_df['tzcode'] = np.where(joined_df['tzcode'].str.startswith('TBD'), options['tz_default'], joined_df['tzcode'])
        message = "You selected to use a time zone of '" + options['tz_default'] + "' for any participants that do not have time zone data."

        # Drop duplicate rows which occurs for participants that responded to the study survey multiple times
        joined_df = joined_df.drop_duplicates(['device_id'], keep='first')
    
    # Removes rows with multiple device_ids per row which were already used to create new rows with one device_id per row earlier
    joined_df = joined_df.dropna(subset=['timestamp'])

    # A dataframe is created with the columns of interest for RAPIDS and is exported as a csv with path specified by options["destination_file"]
    final_df = joined_df[['device_id', 'tzcode', 'timestamp']]
    final_df.to_csv(options["destination_file"], index=False)
    print(message)
    print("Created " + options["destination_file"] + ". Please change file path field in [TIMEZONE][TZCODES_FILE] config.yaml as needed.")

if __name__ == "__main__":

    main()
    