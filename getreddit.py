from utilities import *
from getcontents import get_contents
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
        elif ".zst" in args.input_path:
            mode = "filter"
        elif args.input_path != "":
            mode = "split"
        else:
            raise ValueError("Please set the 'mode' argument. Choose 'download', 'filter', or 'split'.")
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
        # Set argument for filter_list
        filter_list = args.filter_list
        if filter_list == "":
            filter_list = [] # This setting will set to collect all Reddit data
        else:
            try:
                filter_list = filter_list.split(',')
            except:
                raise ValueError("Wrong filter_list declaration. Please correct it.")
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
        # Set default filter_type
        filter_type = args.filter_type
        if filter_type == "":
            filter_type = "subreddit"
        merged_dataframe = get_contents(reddit_type,file_path,filter_list,filter_type,attribute_list,args.add_detail)
        save_to_file(merged_dataframe,filter_list,filter_type,url_path,output_path,args.save_type)
        if args.delete_file == "yes":
            remove(file_path)
            print("The entire process to collect and/or filter the Reddit data has been done.")
        else:
            print("The entire process to collect and/or filter the Reddit data has been done.")
            print("You choose to not remove the original downloaded Reddit data. Beware that the original file may be very big and make your storage full.")
    
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
                    filter_type = args.filter_type
                    if filter_type == "":
                        if "RC" in f:
                            filter_type = "body"
                        elif "RS" in f:
                            filter_type = "title"
                    print(f'Processing the file {file_idx} of {num_of_files} i.e. file {f} ...')
                    flag = "no"
                    try:
                        df = split_contents(dataset,filter_type,args.verbose,file_idx,num_of_files)
                        sentences_to_file(df,f,output_path,args.save_type)
                        if args.delete_file == "yes":
                            remove(f)
                            print("The original filtered data {f} is removed.")
                        print(f'Processing the file {file_idx} of {num_of_files} i.e. file {f} is done. The splitted sentence file is saved in {output_path}splitted_{get_filename(f)}')
                    except:
                        print(f'Failed to split sentence the file {file_idx} of {num_of_files} i.e. file {f}. This maybe because the processed file is an empty file and/or does not contains the filter_type {filter_type} that you choose. Please check the file.')
            file_idx += 1
        print("The entire process to split the filtered Reddit data has been done.")
        if args.delete_file == "no":
            print("You choose to not remove the original filtered Reddit data. Beware that the original filtered file may be very big and make your storage full.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", help="The mode of use this library i.e. whether you want to download and filter ('download'), filter the downloaded dataset ('filter') or split based on sentences the filtered Reddit file ('split').",
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
        "--output_path", help="The folder path to store your filtered or splitted (based on sentence) file.",
        type=str, default=""
    )
    parser.add_argument(
        "--save_type", help="The file extension type that you want to store your collected and/or filtered Reddit data.",
        type=str, default="pickle"
    )
    parser.add_argument(
        "--add_detail", help="The option ('yes' or 'no') whether you want to add 'type' and 'filter' information into your collected Reddit data.",
        type=str, default="no"
    )
    parser.add_argument(
        "--filter_list", help="The words/phrases list that used to filter the Reddit data. Replace space with underscore, e.g. 'energy,climate_change,waste'. Do not set this parameter if you want to collect all Reddit data.",
        type=str, default=""
    )
    parser.add_argument(
        "--filter_type", help="The Reddit attribute that you want to filter using your filter list (for 'scrap' or 'filter' mode) or that you want to split based on sentences (for 'split' mode).",
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
    args = parser.parse_args()
    main(args)