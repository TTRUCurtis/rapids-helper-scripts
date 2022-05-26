src/data/create_rapids_participant_file.py:
	This file was created because Rapids removed the automatic pulling of participant files from the “aware_device” table.  It will pull data from an aware_device formatted table and turn it into an aware_csv file that rapids can read.

python create_rapids_participant_file.py --mysqlconfig <.my.cnf location> --database <database name> --source_table <tablename> --destination_file <full path of output file>

	By default it will use your ~/.my.cnf file.  The other 3 options should be filled in by the database and table you want to pull your AWARE participants from, and the destination file should be where you want the resulting .csv file to be located (you will probably want to use “../../” + the string listed in the “CSV_FILE_PATH” in your config.yaml file.