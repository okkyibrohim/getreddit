import requests as rq
import re
import urllib.request as ur
import time
import json

# Count comment function
def count_comment(query,subreddit,time_sleep,time_sleep_mode,last_day_sample,sample,threshold_sample):
    # URL setting
    query = re.sub(" ","%20%",query.strip())
    subreddit = re.sub("r/","",subreddit)
    last_day_sample_url = f'https://api.pushshift.io/reddit/search/comment?q={query}&subreddit={subreddit}&after={last_day_sample}d&size={sample}'
    all_url = f'https://api.pushshift.io/reddit/search/comment?q={query}&subreddit={subreddit}&size={sample}'
    # Count comments from last_day random sample
    try: 
        last_day_sample_request = rq.get(last_day_sample_url,timeout=60)
        last_day_sample_json_response = last_day_sample_request.json()
        if time_sleep_mode != "only_subreddit_scrapper":
            time.sleep(time_sleep)
        try:
            last_day_sample_num_comment = len(last_day_sample_json_response["data"])
        except:
            last_day_sample_num_comment = 0
    except:
        last_day_sample_num_comment = "bad_gateway_or_blocked"
    # Count comments from all random sample
    try:
        all_request = rq.get(all_url,timeout=60)
        all_json_response = all_request.json()
        if time_sleep_mode != "only_subreddit_scrapper":
            time.sleep(time_sleep)
        try:
            all_num_comment = len(all_json_response["data"])
        except:
            all_num_comment = 0
    except:
        all_num_comment = "bad_gateway_or_blocked"
    # Check if passing the threshold
    try:
        if all_num_comment >= threshold_sample:
            if last_day_sample_num_comment >= threshold_sample:
                is_num_comment_pass = "yes"
            else:
                is_num_comment_pass = "only_all_day_sample"
        else:
            is_num_comment_pass = "no"
    except:
        is_num_comment_pass = "bad_gateway_or_blocked"
    # Return the result
    return last_day_sample_num_comment, all_num_comment, is_num_comment_pass

# Count submission function
def count_submission(query,subreddit,time_sleep,time_sleep_mode,last_day_sample,sample,threshold_sample):
    # URL setting
    query = re.sub(" ","%20%",query.strip())
    subreddit = re.sub("r/","",subreddit)
    last_day_sample_url = f'https://api.pushshift.io/reddit/search/submission?q={query}&subreddit={subreddit}&after={last_day_sample}d&size={sample}'
    all_url = f'https://api.pushshift.io/reddit/search/submission?q={query}&subreddit={subreddit}&size={sample}'
    # Count comments from last_day random sample
    try:
        last_day_sample_request = rq.get(last_day_sample_url,timeout=60)
        last_day_sample_json_response = last_day_sample_request.json()
        if time_sleep_mode != "only_subreddit_scrapper":
            time.sleep(time_sleep)
        try:
            last_day_sample_num_submission = len(last_day_sample_json_response["data"])
        except:
            last_day_sample_num_submission = 0
    except:
        last_day_sample_num_submission = "bad_gateway_or_blocked"
    # Count comments from all random sample
    try:
        all_request = rq.get(all_url,timeout=60)
        all_json_response = all_request.json()
        if time_sleep_mode != "only_subreddit_scrapper":
            time.sleep(time_sleep)
        try:
            all_num_submission = len(all_json_response["data"])
        except:
            all_num_submission = 0
    except:
        all_num_submission = "bad_gateway_or_blocked"
    # Check if passing the threshold
    try:
        if all_num_submission >= threshold_sample:
            if last_day_sample_num_submission >= threshold_sample:
                is_num_comment_pass = "yes"
            else:
                is_num_comment_pass = "only_all_day_sample"
        else:
            is_num_comment_pass = "no"
    except:
        is_num_comment_pass = "bad_gateway_or_blocked"
    # Return the result
    return last_day_sample_num_submission, all_num_submission, is_num_comment_pass

# Get total members function
def get_total_members(subreddit,time_sleep,time_sleep_mode,threshold_member):
    subreddit = re.sub("r/","",subreddit)
    subreddit_url = f'https://api.reddit.com/r/{subreddit}/about'
    # Get total members
    try:
        with ur.urlopen(subreddit_url) as url:
            json_response = json.load(url)
            if time_sleep_mode != "only_subreddit_scrapper":
                time.sleep(time_sleep)
        total_members = json_response["data"]["subscribers"]
    except:
        total_members = "bad_gateway_or_blocked"
    # Check if total members pass
    try:
        if total_members >= threshold_member:
            is_total_members_pass = "yes"
        else:
            is_total_members_pass = "no"
    except:
        is_total_members_pass = "bad_gateway_or_blocked"
    return total_members, is_total_members_pass