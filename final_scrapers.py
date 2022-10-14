# -*- coding: utf-8 -*-

keywords =  ['nutrition','health', 'wellness','longevity']
start_date = '2022-08-01'
end_date = '2022-09-01'

"""## Twitter Scrapper"""

import snscrape.modules.twitter as sntwitter
from datetime import datetime
import json


import praw

import pandas as pd
import numpy as np
import re
import splitter


twitter_dict = []
print("start of twitter scrapper!")

for each_keyword in keywords:
    
    print("keyword start:", each_keyword)
    start = datetime.now()
    
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(each_keyword,'since:%s until:%s lang:en'%(start_date, end_date)).get_items()):
        if i>8000:
            break

        dtime = tweet.date
        new_datetime = datetime.strftime(datetime.strptime(str(dtime), '%Y-%m-%d %H:%M:%S+00:00'), '%Y-%m-%d %H:%M:%S')
        twitter_dict.append([tweet.content, new_datetime])
    
    print("time taken:", datetime.now()-start)

print("length of twitter_dict before slicing:", len(twitter_dict))
twitter_dict.sort(key=lambda row: (row[1]), reverse=True)

"""## Reddit Scrapper"""


reddit_read_only = praw.Reddit( client_id = 'X51vAo_gxeYLE_4l3IGKIg',
                                client_secret = '8fVY5UM-zLjRAam06evgexOzY0QwIg',
                                user_agent = 'FYP WebScraping', check_for_async=False)

redditposts_dict = []
print("start of reddit scrapper!")

for i in keywords: 
    
    print("keyword start:", i)
    start = datetime.now()

    redditposts = reddit_read_only.subreddit(i)
    posts = redditposts.top(time_filter="month")

    for post in posts: 
        redditposts_dict.append([])
        redditposts_dict[-1].append(post.title + " -- " + post.selftext)
        
        post_parsed_date = datetime.utcfromtimestamp(post.created_utc)
        redditposts_dict[-1].append(post_parsed_date)

        if not post.stickied:
            post.comments.replace_more(limit=0)
            for comment in post.comments.list():
                if comment.author == "AutoModerator": 
                    pass
                else: 
                    redditposts_dict.append([])
                    redditposts_dict[-1].append(post.title + "--" + comment.body)
                    
                    comment_parsed_date = datetime.utcfromtimestamp(comment.created_utc)
                    redditposts_dict[-1].append(comment_parsed_date)
    
    print("time taken:", datetime.now()-start)

print("length of reddit_dict:", len(redditposts_dict))

"""Combine both dictionary"""

combined_dict = twitter_dict[:10000] + redditposts_dict
final_df = pd.DataFrame(combined_dict, columns=["Content", "Datetime"])

#To create a cvs file locally
# final_df.to_csv("FYP_Data_Output.csv", index=True)

#To create a cvs file on google drive
# from google.colab import drive
# drive.mount('drive')
# final_df.to_csv('/content/drive/My Drive/FYP_Data_Output.csv', encoding='utf-8', index=False)

print("done!")

"""Pre-Processing"""



df = final_df.copy()
df['original_text'] = df.loc[:, 'Content']



#Functions
def remove_urls (text):
    text = re.sub(r'(https|http)?:\/\/(\w|\.|\/|\?|\=|\&|\%)*\b', '', text, flags=re.MULTILINE)
    return(text)

def clean_text_sentiment(text):

    text = re.sub(r"(&amp;)",' ',text)
    text = re.sub(r"@[\w]+",' ',text)
    text = re.sub(r"\n",' ',text)
    text = re.sub(r"#",' ',text)
    text = re.sub(r"[^a-zA-Z0-9]+",' ',text)

    return text

def small_words_removal(paragraph):
    result = []
    tokens = paragraph.split(" ")
    for word in tokens:
        if len(word) >= 3:
            result.append(word)

    return " ".join(result)

def bigwords_advanced_cleaning(paragraph):

    result = []
    
    tokens = paragraph.split(" ")
    for outer_idx, word in enumerate(tokens):
        if len(word) > 12:
            # ['r', 'take'...]
            split_words = splitter.split(word.lower())

            # The result for a nonsencial string is '' for e.g. 'aaaaaa'
            if split_words == '':
                continue

            # cases like Gastroenterology (Corner cases)
            if type(split_words) != list:
                result.append(split_words)
                continue 

            for idx, split_word in enumerate(split_words):
                
                # remove super small split_word
                if (len(split_word) < 3 or split_word == ''):
                    split_words.pop(idx)  

            for split_word in split_words:
                result.append(split_word)

        else:
            result.append(word)

    return " ".join(result)

#Remove URLs
df['Content'] = df['Content'].apply(lambda x:remove_urls(x))
print("removed URL")

#remove /n, &amp, @usernames, non english characters
df['Content'] = df['Content'].apply(lambda x:clean_text_sentiment(x))
print("removed HTML")

#remove small words
df['Content'] = df['Content'].apply(small_words_removal)
print("removed small words")

#remove big words
df['Content'] = df['Content'].apply(bigwords_advanced_cleaning)
print("removed big words")

#Final JSON Output

result = df.to_json(orient="index")
with open('pre_processed_data.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False, indent=4)