import urllib.request as ur
import os
import pandas as pd
import pickle

# Function to get file name with extension
def get_filename(file_path):
    try:
        return file_path.split('/')[-1]
    except:
        raise ValueError("Cannot parsing the URL to get the file name with the extention. Please re-check the URL.")

# Function to get file name without extension
def get_stringname(file_name):
    try:
        return file_name.split('.')[0]
    except:
        raise ValueError("Cannot parsing the URL to get the file name without the extention. Please re-check the URL.")

# Function for downloading the file
def download(url_path,input_folder_path):
    try:
        print("Download process is started ...")
        file_path = input_folder_path+get_filename(url_path)
        ur.urlretrieve(url_path, file_path)
        print(f'Reddit data saved into {file_path}.')
    except:
        raise ValueError("Cannot download the Reddit data file. Please re-check the URL and input_folder_path.")

# Function for deleting the downloaded file
def remove(file_path):
    try:
        os.remove(file_path)
        print(f'{file_path} has been removed.')
    except:
        raise ValueError("Cannot remove the file. Make sure the file_path is correct.")

def save_to_file(merged_dataframe,filter_list,filter_type,input_path,output_folder_path,save_type):
    for i in range(0,len(merged_dataframe)):
        df = merged_dataframe[i]
        if filter_list == []:
            filter_name = "alldata_"
        else:
            flt = filter_list[i]
            filter_name = flt+"_"+filter_type+"_"
        name_with_path = output_folder_path+filter_name+get_stringname(get_filename(input_path))
        try:
            if save_type == "xlsx":
                saved_file_name = name_with_path+".xlsx"
                df.to_excel(saved_file_name, index=False)
            elif save_type == "csv":
                saved_file_name = name_with_path+".csv"
                df.to_csv(saved_file_name, index=False)
            elif save_type == "pickle":
                saved_file_name = name_with_path+".pickle"
                with open(saved_file_name, 'wb') as handle:
                    pickle.dump(df, handle, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                raise ValueError("Wrong save_type. Only 'xlsx', 'csv', or 'pickle' are allowed to be used as save_type.")
            if filter_list == []:
                print(f'The Reddit data (all data without filtering) has been saved in {saved_file_name}.')
            else:
                print(f'The Reddit data for {flt} {filter_type} has been saved in {saved_file_name}.')
        except:
            raise ValueError("Cannot save the file into the output_folder_path defined. Make sure the output_folder_path is correct.")

def sentences_to_file(df,file_path,output_folder_path,save_type):
    try:
        saved_file_name = output_folder_path+"splitted_"+get_filename(file_path)
        if save_type == "xlsx":
            df.to_excel(saved_file_name, index=False)
        elif save_type == "csv":
            df.to_csv(saved_file_name, index=False)
        elif save_type == "pickle":
            with open(saved_file_name, 'wb') as handle:
                pickle.dump(df, handle, protocol=pickle.HIGHEST_PROTOCOL)
        else:
            raise ValueError("Wrong save_type. Only 'xlsx', 'csv', or 'pickle' are allowed to be used as save_type.")
    except:
        raise ValueError("Cannot save the file into the output_folder_path defined. Make sure the output_folder_path is correct.")