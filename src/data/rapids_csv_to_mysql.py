import argparse
from sqlalchemy import create_engine, text
import os
import pandas as pd


def file_to_sensor(file_name):
    prefix = "phone_"
    suffix = ".csv"

    if file_name.startswith(prefix) and file_name.endswith(suffix):
        result = file_name[len(prefix):-len(suffix)]
        return result

    # Return None if the file name does not match the expected format
    return None

def sensor_to_file(sensor_name):
    prefix = "phone_"
    suffix = ".csv"

    file_name = prefix + sensor_name + suffix
    return file_name

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='database', help='Name of the MySQL database you want to upload to.')
    parser.add_argument('-t', dest='table_name', help='Name the tables that will be created in MySQL, e.g. table_name$feature$level')
    parser.add_argument('--csv', dest='output_path', default='../../../data/processed/', help='Path to features folder containing RAPIDS output (defaults to ../../../data/processed/)')
    parser.add_argument('-g', dest='level', help='Level of analysis done with RAPIDS. E.g. "daily" if time segments set at daily level. Argument only influences naming of table, e.g. table_name$feature$level.')
    parser.add_argument('-f', dest='features', nargs='+', default='search', help='List of behavioral features expected you want uploaded from your RAPIDS output. Leave blank for all features found in the output files.')
    parser.add_argument('-c', dest='collation', help='Desired collation for varchar and text columns.', default='utf8mb4_general_ci')

    args = parser.parse_args()

    if args.table_name == None or args.database == None or args.level == None:
        print("Please ensure table_name (-t), database (-d), and level (-g) are specified. Your arguments were:")
        print('database:', args.database)
        print('table_name:', args.table_name)
        print('output_path:', args.output_path)
        print('level:', args.level)
        print('features:', args.features)
        print('collation:', args.collation)
        exit(2)

    host = '127.0.0.1'  # Specify the desired host
    config_file = '~/.my.cnf'
    database = args.database

    # Construct the connection URL
    connection_url = f'mysql+pymysql://@{host}/{database}'

    # Create the SQLAlchemy engine
    engine = create_engine(connection_url, connect_args={'read_default_file': config_file})

    # Establish a connection
    connection = engine.connect()

    query = text('SELECT * FROM aware_device')

    # Execute a query
    result = connection.execute(query)

    # Fetch the data
    data = result.fetchall()

    # Get participant list
    # Get path for searching for participant names
    directory_path = os.path.join(args.output_path, "features")

    # Get a list of all entries (files and directories) within the features directory
    entries = os.listdir(directory_path)

    # Filter out directories from the list
    directories = [entry for entry in entries if os.path.isdir(os.path.join(directory_path, entry))]
    participants = [x for x in directories if x != 'all_participants']

    # Get list of output files from the first participant in participants list
    rand_participant_path = os.path.join(directory_path, participants[0])
    participant_csv_list = os.listdir(rand_participant_path)

    # Define full feature list 
    filenames = [
    "phone_accelerometer.csv",
    "phone_activity_recognition.csv",
    "phone_applications_foreground.csv",
    "phone_battery.csv",
    "phone_bluetooth.csv",
    "phone_calls.csv",
    "phone_conversation.csv",
    "phone_data_yield.csv",
    "phone_keyboard.csv",
    "phone_light.csv",
    "phone_locations.csv",
    "phone_messages.csv",
    "phone_screen.csv",
    "phone_wifi_connected.csv",
    "phone_wifi_visible.csv"]

    # Create list of feature arguments for use in if loops
    feature_arguments = args.features

    # Add phone_ prefix and .csv ending to feature names
    args.features = [sensor_to_file(i) for i in args.features]

    # Get list of features that were actually computed by RAPIDS
    computed_list = list(set(participant_csv_list) & set(filenames))

    
    # Get list to actually compute (upload list)
    if feature_arguments == 'search':
        upload_list = computed_list
        print("Found the following CSVs which will be uploaded to your MySQL database:", upload_list)
    else:
        if set(args.features) - set(filenames):
            print("Your feature list contains unexpected features/file names.")
            exit(2)
        else:
            upload_list = args.features
            print("The following CSVs which will be uploaded to your MySQL database:", upload_list) # change this, i don't want users to have to phone_.csv
    
    computed_sensors = [file_to_sensor(i) for i in upload_list]
    
    for sensor in computed_sensors:
        print("Uploading:", sensor)
        new_table_name = f'{args.table_name}${sensor}${args.level}'
        drop_query = f'DROP TABLE IF EXISTS {args.database}.{new_table_name};'
        drop_query = text(drop_query)
        connection.execute(drop_query)
        query_start = f'''
        CREATE TABLE {args.database}.{new_table_name} (
        local_segment VARCHAR(255) COLLATE {args.collation},
        local_segment_label VARCHAR(255) COLLATE {args.collation},
        local_segment_start_datetime DATETIME,
        local_segment_end_datetime DATETIME,
        '''
        query_end = f'''
        pid TEXT COLLATE {args.collation}
        );
        '''
        csv_file_path = os.path.join(directory_path, participants[0], f'phone_{sensor}.csv')
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)

        # Print the DataFrame
        old_columns = df.columns
        new_columns = []
        for column in old_columns:
            if column[:5] != "phone":
                continue
            parts = column.split('_')
            provider_name = parts[2]
            column_name = '_'.join(parts[3:])
            new_column = provider_name[0] + '_' + column_name.replace('.', '_')
            new_columns.append(new_column)

        data_type = "DOUBLE"
        query_middle = ""
        for column_name in new_columns:
            query_middle += f"{column_name} {data_type},\n"
        create_query = query_start + query_middle + query_end      
        create_query = text(create_query)
        result = connection.execute(create_query)

        # Insert data for each participant
        for participant in participants:
            df = pd.read_csv(os.path.join(directory_path, participant, f'phone_{sensor}.csv'))
            df.columns = list(df.columns[0:4]) + new_columns
            df['pid'] = participant
            df.to_sql(new_table_name, con=connection, if_exists='append', index=False)
    
    # Commit the changes
    connection.commit()

    # Close the connection
    connection.close()
    

if __name__ == '__main__':
    main()
