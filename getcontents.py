import zstandard
import io
import json
import pandas as pd

# Function to read and filter the file
def get_contents(reddit_type,file_path,filter_list,filter_type,attribute,add_detail):
    global reddit, df, obj, flt, filtered_obj
    # Initialization
    if filter_list == []:
        reddit = []
    else:
        for flt in filter_list:
            exec('{} = {}'.format(flt,[]), globals())
    # Load and filter the file
    print("Reddit data collection and/or filtering process is started ...")
    with open(file_path, 'rb') as fh:
        dctx = zstandard.ZstdDecompressor(max_window_size=2147483648)
        stream_reader = dctx.stream_reader(fh)
        text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
        for line in text_stream:
            obj = json.loads(line)
            try:
                if attribute == []:
                    filtered_obj = obj
                else:
                    filtered_obj = { att: obj[att] for att in attribute }
            except:
                raise ValueError("Failed to filter the attribute. Make sure you defined the attribute parameters correctly.")
            if filter_list == []:
                reddit.append(filtered_obj)
            else:            
                for flt in filter_list:
                    try:
                        if flt in filtered_obj.get(filter_type):
                            exec('{}.append({})'.format(flt,filtered_obj), globals())
                    except:
                        raise ValueError("Wrong filter_type. Make sure that your filter_type is in the attribute you collected.")

    # Merged all filtered Reddit contents
    merged_df = []
    if filter_list == []:
        reddit = pd.DataFrame(reddit)
        if add_detail == "yes":
            reddit.insert(0,'type',reddit_type)
            reddit.insert(1,'filter','no_filter')
        merged_df.append(reddit)
    else:
        for flt in filter_list:
            exec('df={}'.format(flt), globals())
            df = pd.DataFrame(df)
            if add_detail == "yes":
                df.insert(0,'type',reddit_type)
                df.insert(1,'filter',flt)
            merged_df.append(df)
    print("Reddit data collection and/or filtering process has been done.")

    # Final return
    return merged_df 