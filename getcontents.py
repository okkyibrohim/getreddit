import zstandard
import io
import json
import pandas as pd
from utilities import single_df_to_file

# Function to read and filter the file
def get_contents(reddit_type,file_path,filter_list,filter_type,attribute,add_detail,output_path,save_type,return_type="merged_df"):
    global reddit, df, obj, flt, filtered_obj
    # Initialization
    if filter_list == []:
        reddit = []
    else:
        adapted_filter_list = []
        for flt in filter_list:
            flt = str(flt).replace(".","_DOT_")
            flt = str(flt).replace(",","_COMA_")
            flt = str(flt).replace(":","_COLON_")
            flt = str(flt).replace(";","_SEMICOLON_")
            adapted_filter_list.append(filter_type+'_'+flt)
        for flt in adapted_filter_list:
            exec('{} = {}'.format(flt,[]), globals())
    # Load and filter the file
    print("Reddit data collection and/or filtering process is started ...")
    with open(file_path, 'rb') as fh:
        dctx = zstandard.ZstdDecompressor(max_window_size=2147483648)
        with dctx.stream_reader(fh) as reader:
            previous_line = ""
            while True:
                chunk = reader.read(2**24)  # 16mb chunks
                if not chunk:
                    break

                string_data = chunk.decode('utf-8', 'ignore')
                lines = string_data.split("\n")
                for i, line in enumerate(lines[:-1]):
                    if i == 0:
                        line = previous_line + line
                    obj = json.loads(line)
                    try:
                        if attribute == []:
                            filtered_obj = obj
                        else:
                            filtered_obj = { att: obj[att] for att in attribute }
                        del obj
                    except:
                        raise ValueError("Failed to filter the attribute. Make sure you defined the attribute parameters correctly.")
                    if filter_list == []:
                        reddit.append(filtered_obj)
                    else:            
                        try:
                            retrieved_flt_element = next((flt_element for flt_element in list(filter_list) if flt_element in filtered_obj.get(filter_type)), None)
                            if retrieved_flt_element != None:
                                exec('{}.append({})'.format(adapted_filter_list[list(filter_list).index(retrieved_flt_element)],filtered_obj), globals())
                                del filtered_obj
                                continue
                        except:
                            raise ValueError("Wrong filter_type. Make sure that your filter_type is in the attribute you collected.")
                previous_line = lines[-1]
        del reader
    del fh
    # Save file directly or return the 'merged_df' based on user's preferences
    if return_type == "save_file":
        if filter_list == []:
            flt = "alldata"
            reddit = pd.DataFrame(reddit)
            if add_detail == "yes":
                reddit.insert(0,'type',reddit_type)
                reddit.insert(1,'filter','no_filter')
            single_df_to_file(reddit,filter_type,flt,"alldata",file_path,output_path,save_type)
            del reddit
        else:
            for flt in adapted_filter_list:
                filter_name = str(flt)
                exec('df={}'.format(flt), globals())
                exec('del {}'.format(flt), globals())
                df = pd.DataFrame(df)
                if add_detail == "yes":
                    df.insert(0,'type',reddit_type)
                    df.insert(1,'filter',flt)
                single_df_to_file(df,filter_type,flt,filter_name,file_path,output_path,save_type)
                del df
    elif return_type == "merged_df":
        # Print warning
        print(f"You choose {return_type} for the 'return_type'. Your process maybe killed if your memory is not enough to process. If you filter a huge Reddit dataset, we recommend you to chose 'save_file' for the 'return_type' so that the filtered Reddit data will directly saved to your disk during the process.")
        # Merged all filtered Reddit contents
        merged_df = []
        if filter_list == []:
            reddit = pd.DataFrame(reddit)
            if add_detail == "yes":
                reddit.insert(0,'type',reddit_type)
                reddit.insert(1,'filter','no_filter')
            merged_df.append(reddit)
            del reddit
        else:
            for flt in adapted_filter_list:
                exec('df={}'.format(flt), globals())
                exec('del {}'.format(flt), globals())
                df = pd.DataFrame(df)
                if add_detail == "yes":
                    df.insert(0,'type',reddit_type)
                    df.insert(1,'filter',flt)
                merged_df.append(df)
                del df
        print("Reddit data collection and/or filtering process has been done.")
        # Return the final df
        return merged_df 
    else:
        raise ValueError("Wrong 'return_type' parameter. Only 'save_to_file' or 'merged_df' are allowed.")