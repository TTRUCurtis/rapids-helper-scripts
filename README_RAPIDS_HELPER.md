# Helper Scripts README

### Creating RAPIDS participant files

`src/data/create_rapids_participant_file.py:`

```python
python create_rapids_participant_file.py --mysqlconfig <.my.cnf location> --database <database name> --source_table <tablename> 
										 --destination_file <full path of output file>
```

- This file was created because Rapids removed the automatic pulling of participant files from the “aware_device” table.  It will pull data from an aware_device formatted table and turn it into an aware_csv file that rapids can read.
- By default it will use your `~/.my.cnf` file.  The other 3 options should be filled in by the database and table you want to pull your AWARE participants from, and the destination file should be where you want the resulting .csv file to be located (you will probably want to use “../../” + the string listed in the “CSV_FILE_PATH” in your `config.yaml` file. 

### Creating RAPIDS TZCODES_FILE (multiple time zones CSV file)

`src/data/create_multiple_timezones.py:`

```python
python create_multiple_timezones.py  --database <database name> --device_source_table <tablename> 
					--survey_source_table <tablename> --survey_col_name <name of label column in survey table>
					[--mysqlconfig <.my.cnf location>] [--destination_file <full path of output file>]
                    [--tz_default <set default for data missing time zones>] 
                    [--participant_input <path to participant file CSV>] 
                    [--participant_output <desired path for modified participant CSV>]
```

- This script creates a TZCODES_FILE (a CSV file containing the time zones in which participants’ devices sensed data) that can be supplied to RAPIDS in the `config.yaml` under `[TIMEZONE][MULTIPLE][TZCODES_FILE]`
- This script requires a survey_source_table that has two columns: a column that will be used to join this table to the aware_device table using the label column, and a column named “time_zone” with integers corresponding to different time zones as outlined by the table below.

| Integer | Time zone | tzcode to be applied |
| --- | --- | --- |
| 1 | Atlantic Standard Time (AST) | America/Puerto_Rico |
| 2 | Eastern Standard Time (EST) | America/New_York |
| 3 | Central Standard Time (CST) | America/Chicago |
| 4 | Mountain Standard Time (MST) | America/Denver |
| 5 | Pacific Standard Time (PST) | America/Los_Angeles |
| 6 | Alaskan Standard Time (AKST) | America/Anchorage |
| 7 | Hawaii–Aleutian Standard Time (HST) | Pacific/Honolulu |

- This script requires specifying the database in which your data is located, the name of the aware_device table, the name of the time zone survey table, and the name of the column in your time zone survey table that matches the “label” column of the aware_device table. 
- Default location of your mysqlconfig is `~/.my.cnf`. Default output destination is `../../../data/external/multiple_timezones.csv`. Default path to participant CSV file created previously by `create_participant_file.py` is `../../../data/external/participant_file.csv`.
- An example survey_source_table with label column “eid” is below:

| eid | time_zone |
| --- | --- |
| AA0913 | 3 |
| AA10307 | 2 |
| AA12108 | 2 |

- Use option `tz_default` to decide how participants whose labels are not found in the time zone survey table are addressed. Default option is `remove`. See table for the possible arguments and their behaviors 

| Argument | Behavior |
| --- | --- |
| remove | Creates TZCODES file with only participants that have time zone data. Also creates new participant file so that participants without time zone data are not included by RAPIDS. |
| ignore | Creates TZCODES file with only participants that have time zone data. Notifies user if there are participants specified in the partipant file without time zone data and instructs user to edit `config.yaml` to set a default time zone for participants with mising data. |
| custom tzcode | User passes a tzcode as the argument (e.g. 'America/New_York') which is applied to participants without time zone data, permitting the creation of a TZCODES file including all participants in the device source table. |

- If using argument `remove` for option `tz_default`, the default output destination for the modified participant file is `../../../participant_file_modified.csv`. A different destination can be specified with option `--participant_output`.
- If using argument `ignore`, user will need to change values of `[IF_MISSING_TZCODE]` and/or `[DEFAULT_TZCODE]` under `[TIMEZONE][MULTIPLE]` in `config.yaml`. Refer to https://www.rapids.science/1.9/setup/configuration/#timezone-of-your-study for reference.

