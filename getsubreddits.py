import time
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
import pandas as pd
from getsubredditstats import *
import re
from utilities import single_df_to_file

# Get subreddit list based on a single query
def get_single_subreddit_list(query: str, get_top_subreddit="all", time_sleep=2, time_sleep_mode="only_subreddit_scrapper", last_day_sample=90, sample=1000, threshold_sample=100, threshold_member=10000,verbose="yes",headless="yes"):
    if verbose == "yes" or verbose == "trace_all":
        print("Start collecting the subreddit ....")
    # Set the driver
    if headless == "yes":
        options = uc.ChromeOptions()
        options.add_argument('--headless=new')
        driver = uc.Chrome(use_subprocess=True,options=options)
    else:
        driver = uc.Chrome()
    url = f"https://www.reddit.com/search/?q={query}&type=sr"
    driver.get(url)
    last_height = driver.execute_script("return document.body.scrollHeight")
    # Get the communities list
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(time_sleep)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    if verbose == "yes" or verbose == "trace_all":
        print("Finish in collecting the subreddits. Now we calculating the statistics for each subreddits ...")
    subreddits = []
    soup = BeautifulSoup(driver.page_source, 'lxml')
    driver.close()
    i = 1
    for x in soup.find('div', {'data-testid': 'communities-list'}).find_all('a', {'data-testid': 'subreddit-link'}):
        if verbose == "yes" or verbose == "trace_all":
            len_soup = len(soup.find('div', {'data-testid': 'communities-list'}).find_all('a', {'data-testid': 'subreddit-link'}))
            print(f'Calculating the statistic subreddit {i} of {len_soup}')
            i = i+1
        subreddit = re.sub("r/","",x.find('h6').get_text())
        description = x.find_all('p')[-1].get_text()
        if verbose == "trace_all":
            print(f'Getting total members of subreddit {subreddit}...')
        total_members, is_total_members_pass = get_total_members(subreddit,time_sleep,time_sleep_mode,threshold_member)
        if verbose == "trace_all":
            print(f'Total members of subreddit {subreddit} is {total_members}')
            print(f'Calculating how many comments on subreddit {subreddit} that contain keyword {query} from {sample} sample...')
        last_day_sample_num_comment, all_num_comment, is_num_comment_pass = count_comment(query,subreddit,time_sleep,time_sleep_mode,last_day_sample,sample,threshold_sample)
        if verbose == "trace_all":
            print(f'Total comments on subreddit {subreddit} that contain keyword {query} from {sample} sample in last {last_day_sample} days are {last_day_sample_num_comment} comments.')
            print(f'Total comments on subreddit {subreddit} that contain keyword {query} from {sample} sample in all time are {all_num_comment} comments.')
            print(f'Calculating how many submissions on subreddit {subreddit} that contain keyword {query} from {sample} sample...')
        last_day_sample_num_submission, all_num_submission, is_num_submission_pass = count_submission(query,subreddit,time_sleep,time_sleep_mode,last_day_sample,sample,threshold_sample)
        if verbose == "trace_all":
            print(f'Total submissions on subreddit {subreddit} that contain keyword {query} from {sample} sample in last {last_day_sample} days are {last_day_sample_num_submission} submissions.')
            print(f'Total submissions on subreddit {subreddit} that contain keyword {query} from {sample} sample in all time are {all_num_submission} submissions.')
        subreddits.append({
            'query': query,
            'subreddit': subreddit,
            'description': description,
            'total_members': total_members,
            'threshold_member': threshold_member,
            'is_total_members_pass': is_total_members_pass,
            'last_day_sample': last_day_sample,
            'sample': sample,
            'threshold_sample': threshold_sample,
            'last_day_sample_num_comment': last_day_sample_num_comment,
            'all_day_sample_num_comment': all_num_comment,
            'is_num_comment_pass': is_num_comment_pass,
            'last_day_sample_num_submission': last_day_sample_num_submission,
            'all_day_sample_num_submission': all_num_submission,
            'is_num_submission_pass': is_num_submission_pass
        })
        if get_top_subreddit != "all":
            if i-1 == get_top_subreddit:
                print(f'We only consider to get the top {get_top_subreddit} subreddits and all of them have collected.')
                break
    df_communities = pd.DataFrame(subreddits)
    if verbose == "yes" or verbose == "trace_all":
        print(f"All process in collecting subreddit from query {query} is finished.")
    return df_communities

# Get all subreddit list from all query from query list
def get_subreddit_list(query_list,filter_type,output_path,save_type,return_type="merged_df", get_top_subreddit="all", time_sleep=2, time_sleep_mode="only_subreddit_scrapper", last_day_sample=90, sample=1000, threshold_sample=100, threshold_member=10000,verbose="yes",headless="yes"):
    global subreddit_list, df
    # Check if the 'query_list' is empty or not
    if query_list == []:
        raise ValueError("You are trying to collect subreddit list based on a list of query. You should define the query list. If you use this script directly from the terminal, the query list should be defined as filter_list argument, run 'python getreddit.py -h' for the help.")
    # Prepare df name
    adapted_query_list = []
    for q in query_list:
        q = str(q).replace(" ","_")
        q = re.sub(r'[^\w\s]', '_', q)
        adapted_query_list.append(q)
    # Get all sub reddit list
    for i in range(0,len(adapted_query_list)):
        if verbose == "yes":
            print(f"Collecting subreddit list for query {i+1} of {len(adapted_query_list)} i.e. {query_list[i].lower()}")
        subreddit_list = get_single_subreddit_list(query_list[i].lower(), get_top_subreddit, time_sleep, time_sleep_mode, last_day_sample, sample, threshold_sample, threshold_member,verbose,headless)
        exec('{}=subreddit_list'.format(adapted_query_list[i]),globals())
    # Save file directly or return the 'merged_df' based on user's preferences
    if return_type == "save_file":
        for i in range(0,len(adapted_query_list)):
            if verbose == "yes":
                print(f"Saving subreddit list from query {i+1} of {len(adapted_query_list)} i.e. {query_list[i].lower()} to {output_path}.")
            exec('df={}'.format(adapted_query_list[i]), globals())
            single_df_to_file(df,filter_type,query_list[i].lower(),adapted_query_list[i],output_path,output_path,save_type)
            del df
        if verbose == "yes":
            print("All process to collect subreddit list from a query list has been done.")
    elif return_type == "merged_df":
        merged_df = []
        for i in range(0,len(adapted_query_list)):
            if verbose == "yes":
                print(f"Merging subreddit list from query {i+1} of {len(adapted_query_list)} i.e. {query_list[i].lower()}.")
            exec('df={}'.format(adapted_query_list[i]), globals())
            if len(df) != 0:
                merged_df.append(df)
            del df
        if verbose == "yes":
            print("All process to collect subreddit list from a query list has been done.")
        return merged_df
    else:
        raise ValueError("Wrong 'return_type' parameter. Only 'save_to_file' or 'merged_df' are allowed.")