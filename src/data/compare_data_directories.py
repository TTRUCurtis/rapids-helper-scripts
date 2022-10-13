import sys
import os
import getopt
import pandas as pd
import glob

def main():

    try:
        optlist, args = getopt.getopt(sys.argv[1:], "", [])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
        
    options = {}
    options["dir1"] = "/home/douglasvbellew/Workspaces/rapids/keystroke_prod/rapids/data"
    options["dir2"] = "/home/douglasvbellew/Workspaces/rapids/keystroke_final_test/rapids/data"
    options["compare_raw"] = True
    options["compare_interim"] = True
    
    for option_tuple in optlist:
        if (option_tuple[0] == "--noraw"):
            options["compare_raw"] = False
        elif (option_tuple[0] == "--nointerim"):
            options["compare_interim"] = False
        elif (option_tuple[0] == "--dir1"):
            options["last_processed_id"] = option_tuple[1]
        elif (option_tuple[0] == "--dir2"):
            options["last_processed_id"] = option_tuple[1]    
    
    compare_dirs = []
    if (options["compare_raw"]):
        compare_dirs.append("raw")
    if(options["compare_interim"]):
        compare_dirs.append("interim")
    
    for comp_dir in compare_dirs:
        if (comp_dir == "raw"):
            dir1_files = glob.glob(os.path.join(options["dir1"],comp_dir,"**","*.csv"))
            dir1_files.sort()
            dir2_files = glob.glob(os.path.join(options["dir2"],comp_dir,"**","*.csv"))
            dir2_files.sort()            
        elif (comp_dir == "interim"):
            dir1_files = glob.glob(os.path.join(options["dir1"],comp_dir,"**","**","*.csv"))
            dir1_files.sort()
            dir2_files = glob.glob(os.path.join(options["dir2"],comp_dir,"**","**","*.csv"))
            dir2_files.sort()

        print(len(dir1_files))
        print(len(dir2_files))
        #print(dir1_files[:10])
        #print(dir2_files[:10])
        if (len(dir1_files) == 0):
            print("Directory: "+os.path.join(options["dir1"],comp_dir)+" has no files to compare.")
        if (len(dir2_files) == 0):
            print("Directory: "+os.path.join(options["dir2"],comp_dir)+" has no files to compare.")
        if ((len(dir1_files) > 0) and (len(dir2_files) > 0)):
            pop_dir_1 = True
            pop_dir_2 = True
            this_pass = 0
            while ((len(dir1_files) > 0) or (len(dir2_files) > 0)):
                this_pass = this_pass + 1
                if (this_pass%5 == 0):
                    print("this_pass = "+str(this_pass))
                if (pop_dir_1):
                    dir1_file = dir1_files.pop(0)
                if (pop_dir_2):
                    dir2_file = dir2_files.pop(0)
                pop_dir_1 = True
                pop_dir_2 = True
                
                no_file_1 = False
                no_file_2 = False
                dir1_file_parts = dir1_file.split("/")
                dir2_file_parts = dir2_file.split("/")
                if (dir1_file_parts[-3] < dir2_file_parts[-3]):
                    no_file_2 = True
                elif (dir1_file_parts[-3] > dir2_file_parts[-3]):
                    no_file_1 = True    
                elif (dir1_file_parts[-2] < dir2_file_parts[-2]):
                    no_file_2 = True
                elif (dir1_file_parts[-2] > dir2_file_parts[-2]):
                    no_file_1 = True
                elif (dir1_file_parts[-1] < dir2_file_parts[-1]):
                    no_file_2 = True
                elif (dir1_file_parts[-1] > dir2_file_parts[-1]):
                    no_file_1 = True 
                    
                if (no_file_2):
                    print("Didn't find file match for:" + dir1_file)
                    pop_dir_2 = False
                    continue
                elif (no_file_1):
                    print("Didn't find file match for:" + dir2_file)
                    pop_dir_1 = False
                    continue
                    
                df1 = pd.read_csv(dir1_file)
                df2 = pd.read_csv(dir2_file)
                
                df1['duplicate_counter'] = df1.groupby(list(df1.columns)).cumcount()
                df2['duplicate_counter'] = df2.groupby(list(df2.columns)).cumcount()
                merged = df1.merge(df2, indicator=True, how='outer')
                merged = merged[merged['_merge'] != 'both']
                if (len(merged) > 0):
                    print("For file: "+os.path.join(comp_dir,dir1_file_parts[-3],dir1_file_parts[-2],dir1_file_parts[-1])+" there are diffs:")
                    print(merged)
                # if "timestamp" in df1.columns:
                #     df1 = df1.sort_values(by=["timestamp"])
                #     df2 = df2.sort_values(by=["timestamp"])
                # elif "local_segment" in df1.columns:
                #     df1 = df1.sort_values(by=["local_segment"])
                #     df2 = df2.sort_values(by=["local_segment"])                       
        


def usage():
    print("python compare_data_directories.py --dir1 <first base directory> --dir2 <second base directory [--noraw] [--noinerim]")
    
if __name__ == "__main__":
    main()