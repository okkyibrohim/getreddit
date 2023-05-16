from utilities import *
from getcontents import get_contents
from getsubreddits import get_subreddit_list
from splitcontents import split_contents
import argparse
import os
import pandas as pd

# The main function for end-to-end used that can be called instantly from the terminal
def main(args):
    # Set argument for mode
    mode = args.mode
    if mode == "":
        if args.url_path != "":
            mode = "download"
            url_path = args.url_path
        elif '.zst' in args.input_path:
            mode = "filter"
        elif args.input_path != "":
            mode = "split"
        else:
            raise ValueError("Please set the 'mode' argument. Choose 'download', 'filter', 'split', or 'subreddit_list'.")
    # Set default filter_type
    filter_type = args.filter_type
    if filter_type == "":
        if mode == "download" or mode == "filter":
            filter_type = "subreddit"
            print(f"You are running {mode} mode but you do not set the 'filter_type' parameter so that it automatically to be set as 'subreddit'.")
        elif mode == "subreddit_list":
            filter_type = "subreddit_list"
            print(f"You are running {mode} mode but you do not set the 'filter_type' parameter so that it automatically to be set as 'subreddit_list'.")
        else:
            if "RC" in f:
                filter_type = "body"
                print(f"You are running {mode} mode but you do not set the 'filter_type' parameter and we detect that you are trying to split a Reddit Comment (RC) file so that it automatically to be set as 'body'.")
            elif "RS" in f:
                filter_type = "title"
                print(f"You are running {mode} mode but you do not set the 'filter_type' parameter and we detect that you are trying to split a Reddit Submission (RS) file so that it automatically to be set as 'title'.")
    # Set argument for filter_list
    filter_list = args.filter_list
    if filter_list == "":
        if mode == "subreddit_list":
            raise ValueError("You try to run 'subreddit_list' mode. You should define the query list on 'filter_list' parameter. Run 'python getreddit.py -h' for the help.")
        else:
            filter_list = [] # This setting will set to collect all Reddit data
    elif '.xlsx' in filter_list:
        df_filter_list = pd.read_excel(filter_list)
        filter_list = list(df_filter_list[filter_type])
    elif '.csv' in filter_list:
        df_filter_list = pd.read_csv(filter_list)
        filter_list = list(df_filter_list[filter_type])
    elif '.pickle' in filter_list:
        df_filter_list = pd.read_pickle(filter_list)
        filter_list = list(df_filter_list[filter_type])
    else:
        try:
            filter_list = filter_list.split(',')
            filter_list = [flt.replace('_',' ') for flt in filter_list]
        except:
            raise ValueError("Wrong filter_list declaration. Please correct it. Run 'python getreddit.py -h' for the help.")
    # Set argument for input_path
    input_path = args.input_path
    # Set argument for output_path
    output_path = args.output_path
    if output_path != "":
        if output_path[-1] != "/":
            output_path = output_path+"/"

    # Process for 'download' or 'filter' mode
    if mode == "download" or mode == "filter":
        # Set argument for reddit_type
        if ("RC" in args.url_path) or ("RC" in args.input_path):
            reddit_type = "comment"
        elif ("RS" in args.url_path) or ("RS" in args.input_path):
            reddit_type = "submission"
        else:
            raise ValueError("Wrong url_path or input_path. Make sure that you corectly set the url_path or input_path argument to download/filter a particular month of Reddit data.")
        # Set argument for attribute
        attribute_list = args.attribute_list
        if attribute_list == "":
            if reddit_type == "comment":
                attribute_list = ["id","subreddit","body"]
            else:
                attribute_list = ["id","subreddit","title"]
        else:
            try:
                attribute_list = attribute_list.split(',')
            except:
                raise ValueError("Wrong attribute_list declaration. Please correct it.")
        # Download the Reddit data for 'download' mode
        if mode == "download":
            if input_path != "":
                if input_path[-1] != "/":
                    input_path = input_path+"/"
            download(url_path,input_path)
            file_path = input_path+get_filename(url_path)
        else:
            file_path = input_path
        get_contents(reddit_type,file_path,filter_list,filter_type,attribute_list,args.add_detail,output_path,args.save_type,"save_file")
        if args.delete_file == "yes":
            remove(file_path)
            print("The entire process to collect and/or filter the Reddit data has been done.")
        else:
            print("The entire process to collect and/or filter the Reddit data has been done.")
            print("You choose to not remove the original downloaded Reddit data. Beware that the original file may be very big and make your storage full.")
    
    # Process for 'subreddit_list' mode
    elif mode == "subreddit_list":
        get_top_subreddit = args.get_top_subreddit
        if get_top_subreddit == 0:
            get_top_subreddit = "all"
        get_subreddit_list(filter_list,filter_type,output_path,args.save_type,"save_file", get_top_subreddit, args.time_sleep, args.time_sleep_mode, args.last_day_sample, args.sample, args.threshold_sample, args.threshold_member,args.verbose,args.headless)            

    # Process for 'split' mode
    else:
        if (input_path == "") or (".zst" in input_path):
            raise ValueError("You are in 'split' mode. Please re-check your input_path argument. The input_path argument must be the path to the folder that contains the list of filtered Reddit file.")
        if input_path[-1] == "/":
            input_path = input_path[:-1]
        num_of_files = len(os.listdir(input_path))
        file_idx = 1
        for filename in os.listdir(input_path):
            f = os.path.join(input_path, filename)
            if os.path.isfile(f):
                if args.save_type in f:
                    if args.save_type == "pickle":
                        dataset = pd.read_pickle(f)
                    elif args.save_type == "excel":
                        dataset = pd.read_excel(f)
                    elif args.save_type == "csv":
                        dataset = pd.read_csv(f)
                    else:
                        raise ValueError("Wrong save_type argument. Only 'pickle', 'excel', or 'csv' that is allowed to be used as save_type argument.")
                    print(f'Processing the file {file_idx} of {num_of_files} i.e. file {f} ...')
                    df = split_contents(dataset,filter_type,args.verbose,file_idx,num_of_files)
                    sentences_to_file(df,f,output_path,args.save_type)
                    if args.delete_file == "yes":
                        remove(f)
                        print("The original filtered data {f} is removed.")
                    print(f'Processing the file {file_idx} of {num_of_files} i.e. file {f} is done. The splitted sentence file is saved in {output_path}splitted_{get_filename(f)}')
            file_idx += 1
        print("The entire process to split the filtered Reddit data has been done.")
        if args.delete_file == "no":
            print("You choose to not remove the original filtered Reddit data. Beware that the original filtered file may be very big and make your storage full.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", help="The mode of use this library i.e. whether you want to download and filter ('download'), filter the downloaded dataset ('filter'), split based on sentences the filtered Reddit file ('split'), or collecting subreddit list from a certain query list ('subreddit_list').",
        type=str, default=""
    )
    parser.add_argument(
        "--verbose", help="The option ('yes' or 'no') whether you want to print the progress or not.",
        type=str, default="yes"
    )
    parser.add_argument(
        "--url_path", help="The URL link to download the reddit submissions (e.g. https://files.pushshift.io/reddit/submissions/) or comments (https://files.pushshift.io/reddit/comments/) from a particular month.",
        type=str, default=""
    )
    parser.add_argument(
        "--input_path", help="The folder path to store the original downloaded file (mode 'download'), or the original downloaded file location that you want to filter (mode 'filter'), or the folder path which contain the list of filtered data that you want to split based on the sentence (mode 'split').",
        type=str, default=""
    )
    parser.add_argument(
        "--output_path", help="The folder path to store the output files.",
        type=str, default=""
    )
    parser.add_argument(
        "--save_type", help="The file extension type that you want to store your output files.",
        type=str, default="pickle"
    )
    parser.add_argument(
        "--add_detail", help="The option ('yes' or 'no') whether you want to add 'type' and 'filter' information into your collected Reddit data.",
        type=str, default="no"
    )
    parser.add_argument(
        "--filter_list", help="The words/phrases list that used to filter the Reddit data or to collect the subreddit list. Replace space with underscore, e.g. 'energy,climate_change,waste'. If you have a huge filter (query) list, you can save it in an excel, csv, or pickle with the name column is the same with the 'filter_type' parameter. Do not set this parameter if you want to collect all Reddit data in 'download' or 'filter' mode.",
        type=str, default=""
    )
    parser.add_argument(
        "--filter_type", help="The Reddit attribute that you want to filter using your filter list (for 'download' or 'filter' mode) or that you want to split based on sentences (for 'split' mode). If you run 'subreddit_list' mode to collect a subreddit list from a list of query ('filter_list'), the 'filter_type' should be set as the column name that contain your query list.",
        type=str, default=""
    )
    parser.add_argument(
        "--attribute_list", help="The Reddit attributes that you want to collect, e.g. 'id,subreddit,body'. Type 'all' if you want to collect all Reddit attributes (this will make the data size may very huge).",
        type=str, default=""
    )
    parser.add_argument(
        "--delete_file", help="The option to delete ('yes') the Reddit original file or not ('no').",
        type=str, default="yes"
    )
    parser.add_argument(
        "--get_top_subreddit", help="The top subreddit list that you want to collect for each query. This option only used for 'subreddit_list' mode. Leave this parameter blank or set 0 to collect all subreddit list from each query",
        type=int, default=0
    )
    parser.add_argument(
        "--time_sleep", help="The time (in seconds) to sleep for the scraping process. If your internet is slow, set a higher time to sleep e.g. 5 or 10 (only used for 'subreddit_list' mode). This option only used for 'subreddit_list' mode.",
        type=int, default=2
    )
    parser.add_argument(
        "--time_sleep_mode", help="The option to set whether the time_sleep only applied for the subreddit list scrapping process (set 'only_subreddit_scrapper' for this option) or all processes including for the subreddit statistic collection process (set 'all' for this option). Set 'all' if your internet speed is slow and you highly consider collecting the subreddit statistic. This option only used for 'subreddit_list' mode.",
        choices=["only_subreddit_scrapper","all"], type=str, default="only_subreddit_scrapper"
    )
    parser.add_argument(
        "--last_day_sample", help="The range of the last day that you want to get the subreddit comments/submissions that mention the query. This option only used for 'subreddit_list' mode.",
        type=int, default=90
    )
    parser.add_argument(
        "--sample", help="The number of total samples that you want to calculate the comments/submissions that mention the query. The maximum is 1,000 following the Pushift API limitation. This option only used for 'subreddit_list' mode.",
        type=int, default=1000
    )
    parser.add_argument(
        "--threshold_sample", help="The number of thresholds for minimum comments/submissions in a subreddit that mentions the query. This option only used for 'subreddit_list' mode.",
        type=int, default=100
    )
    parser.add_argument(
        "--threshold_member", help="The number of thresholds for total members in a subreddit. This option only used for 'subreddit_list' mode.",
        type=int, default=10000
    )
    parser.add_argument(
        "--headless", help="The option to show the browser scrapping process or not. It should be set as 'yes' if you run this script in a server that does not has a browser GUI. This option only used for 'subreddit_list' mode.",
        type=str, default="yes"
    )
    args = parser.parse_args()
    main(args)