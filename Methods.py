import os
import types
import uuid
import ibm_boto3
from ibm_botocore.client import Config
from ibm_botocore.exceptions import ClientError
import ibm_s3transfer.manager
import pandas as pd
# import sys
import tweepy
import matplotlib.pyplot as plt
import numpy as np
import nltk
# import pycountry
import re
import string
from wordcloud import WordCloud, STOPWORDS
# from PIL import Image
from nltk.sentiment.vader import SentimentIntensityAnalyzer
# from langdetect import detect
# from nltk.stem import SnowballStemmer
from sklearn.feature_extraction.text import CountVectorizer
import Credentials as cre
import logging
from datetime import datetime

data_movies = pd.DataFrame()
search_data_twitter = pd.DataFrame()
search_data_movies = pd.DataFrame()


class IBM:
    def __init__(self):
        logging.basicConfig(filename="data/guiApp.log",
                            format='%(asctime)s -- %(name)s -- %(levelname)s -- %(funcName)s -- %(message)s',
                            filemode='a', datefmt='%Y/%m/%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(f"Initialize IBM class")

        self.COS_ENDPOINT = "https://s3.eu.cloud-object-storage.appdomain.cloud"
        self.COS_API_KEY_ID = cre.ibm_api_key
        self.COS_AUTH_ENDPOINT = "https://iam.cloud.ibm.com/identity/token"
        self.COS_SERVICE_CRN = cre.ibm_service_crn
        self.COS_STORAGE_CLASS = "eu-standard"
        self.bucket_name = cre.ibm_bucket_name

        self.cos = ibm_boto3.client("s3",
                                    ibm_api_key_id=self.COS_API_KEY_ID,
                                    ibm_service_instance_id=self.COS_SERVICE_CRN,
                                    ibm_auth_endpoint=self.COS_AUTH_ENDPOINT,
                                    config=Config(signature_version="oauth"),
                                    endpoint_url=self.COS_ENDPOINT
                                    )

        self.logger.debug(f"Initialize IBM class - [finished]")

    def download_file(self, fileName="FinalOutput01.csv"):
        self.logger.debug(f"Downloading file")

        if not os.path.exists("data/FinalOutput01.csv"):
            def __iter__(self): return 0

            body = self.cos.get_object(
                Bucket='dataguru01-donotdelete-pr-9rmz8gbtdfizh7', Key=fileName)['Body']
            if not hasattr(body, "__iter__"):
                body.__iter__ = types.MethodType(__iter__, body)

            global data_movies
            data_movies = pd.read_csv(body, dtype={"movie_name": "string",
                                                   "genre": "string",
                                                   "review_content": "string",
                                                   "critic_gender": "string",
                                                   "critic_contenental": "string",
                                                   "critic_country": "string",

                                                   }, parse_dates=['release_date', 'review_date'], low_memory=False, index_col=0)

            data_movies["movie_name"] = data_movies["movie_name"].str.lower()
            data_movies.to_csv("data/"+fileName)

        else:
            data_movies = pd.read_csv("data/"+fileName, index_col=0)
            data_movies["movie_name"] = data_movies["movie_name"].str.lower()
            self.logger.warning(f"The file exists when trying to download")

        self.logger.debug(f"Downloading file - [finished]")

    def delete_item(self, object_name):
        self.logger.debug(f"Deleting file")
        try:
            self.cos.delete_object(Bucket=self.bucket_name, Key=object_name)
            self.logger.debug("Item: {0} deleted!\n".format(object_name))
        except ClientError as be:
            self.logger.error("CLIENT ERROR: {0}\n".format(be))
        except Exception as e:
            self.logger.error("Unable to delete object: {0}".format(e))
        self.logger.debug(f"Deleting file - [finished]")

    def upload_file(self, fileName='data/DashboardInput.csv', key='DashboardInput.csv', bucket='dataguru01-donotdelete-pr-9rmz8gbtdfizh7'):
        self.logger.debug(f"Uploading file")
        self.cos.upload_file(Filename=fileName, Bucket=bucket, Key=key)
        # os.remove(fileName)  # comment if needed
        self.logger.debug(f"Uploading file - [finished]")


class Twitter():
    def __init__(self):
        logging.basicConfig(filename="data/guiApp.log",
                            format='%(asctime)s -- %(name)s -- %(levelname)s -- %(funcName)s -- %(message)s',
                            filemode='a', datefmt='%Y/%m/%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(f"Initialize Twitter class")

        nltk.download("vader_lexicon")
        nltk.download('stopwords')

        self.logger.debug(f"Initialize Twitter class - [finished]")

    def analyze_tweet(self, dataFrame: pd.DataFrame(), text_to_analyze: str):
        self.logger.debug(f"Analyzing tweet")
        score = SentimentIntensityAnalyzer().polarity_scores(text_to_analyze)
        neg = score['neg']
        neu = score['neu']
        pos = score['pos']
        comp = score['compound']

        if neg > pos:
            dataFrame["tweet_sent_negative"] = 1
            dataFrame["tweet_sent_postive"] = 0
            dataFrame["tweet_sent_neutral"] = 0

        elif pos > neg:
            dataFrame["tweet_sent_negative"] = 0
            dataFrame["tweet_sent_postive"] = 1
            dataFrame["tweet_sent_neutral"] = 0

        elif pos == neg:
            dataFrame["tweet_sent_negative"] = 0
            dataFrame["tweet_sent_postive"] = 0
            dataFrame["tweet_sent_neutral"] = 1

        self.logger.debug(f"Analyzing tweet - [finished]")
        return dataFrame

    def get_tweets(self, keyword: str, noOfTweet: int):
        self.logger.debug(f"Getting tweet with keyword: {keyword}")
        consumerKey = cre.twitter_consumerKey
        consumerSecret = cre.twitter_consumerSecret
        accessToken = cre.twitter_accessToken
        accessTokenSecret = cre.twitter_accessTokenSecret

        auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
        auth.set_access_token(accessToken, accessTokenSecret)
        api = tweepy.API(auth)

        tweets = tweepy.Cursor(api.search, q=keyword).items(noOfTweet)
        tweet_list = []
        for tweet in tweets:
            tweet_list.append(tweet.text)

        self.logger.debug(
            f"Getting tweet with keyword: {keyword} - [finished]")

        global search_data_twitter
        search_data_twitter = pd.DataFrame(tweet_list, columns=['tweets'])

    def clean_tweet(self, text_to_clean: str):
        self.logger.debug(f"Cleaning tweet")
        stopword = nltk.corpus.stopwords.words('english')
        ps = nltk.PorterStemmer()
        text_lc = "".join([word.lower()
                           for word in text_to_clean if word not in string.punctuation])
        text_rc = re.sub('[0-9]+', '', text_lc)
        tokens = re.split('\W+', text_rc)

        text_to_clean = [ps.stem(word)
                         for word in tokens if word not in stopword]

        self.logger.debug(f"Cleaning tweet - [finished]")
        return " ".join(text_to_clean)

    def adjust_tweets(self):
        self.logger.debug(f"Adjusting tweets")
        global search_data_twitter

        search_data_twitter["cleaned_tweet"] = search_data_twitter.apply(
            lambda x: self.clean_tweet(x["tweets"]), axis=1)

        search_data_twitter = search_data_twitter.apply(
            lambda x: self.analyze_tweet(x, x["cleaned_tweet"]), axis=1)

        self.logger.debug(f"Adjusting tweets - [finished]")


class Movies():
    def __init__(self):
        logging.basicConfig(filename="data/guiApp.log",
                            format='%(asctime)s -- %(name)s -- %(levelname)s -- %(funcName)s -- %(message)s',
                            filemode='a', datefmt='%Y/%m/%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(f"Initialize Movies class")

        self.logger.debug(f"Initialize Movies class - [finished]")

    def analyze_review(self, dataFrame: pd.DataFrame(), text_to_analyze: str):
        self.logger.debug(f"Analyzing review")
        score = SentimentIntensityAnalyzer().polarity_scores(text_to_analyze)
        neg = score['neg']
        neu = score['neu']
        pos = score['pos']
        comp = score['compound']

        if neg > pos:
            dataFrame["review_sent_negative"] = 1
            dataFrame["review_sent_postive"] = 0
            dataFrame["review_sent_neutral"] = 0

        elif pos > neg:
            dataFrame["review_sent_negative"] = 0
            dataFrame["review_sent_postive"] = 1
            dataFrame["review_sent_neutral"] = 0

        elif pos == neg:
            dataFrame["review_sent_negative"] = 0
            dataFrame["review_sent_postive"] = 0
            dataFrame["review_sent_neutral"] = 1

        self.logger.debug(f"Analyzing review - [finished]")
        return dataFrame

    def clean_review(self, text_to_clean: str):
        self.logger.debug(f"Cleaning review")
        stopword = nltk.corpus.stopwords.words('english')
        ps = nltk.PorterStemmer()
        text_lc = "".join([word.lower()
                           for word in text_to_clean if word not in string.punctuation])
        text_rc = re.sub('[0-9]+', '', text_lc)
        tokens = re.split('\W+', text_rc)

        text_to_clean = [ps.stem(word)
                         for word in tokens if word not in stopword]

        self.logger.debug(f"Cleaning review - [finished]")
        return " ".join(text_to_clean)

    def adjust_reviews(self):
        global search_data_movies
        # print(search_data_movies)
        search_data_movies["review_content_cleaned"] = search_data_movies.apply(
            lambda x: self.clean_review(x["review_content"]), axis=1)

        search_data_movies = search_data_movies.apply(
            lambda x: self.analyze_review(x, x["review_content"]), axis=1)


# # twitter = Twitter()
# # twitter_data = twitter.get_tweets("intel", 10)
# # print(twitter_data)

# ibm = IBM()
# twitter = Twitter()
# movies = Movies()
# ibm.download_file()
# # start = datetime.now()
# # print("started timer")
# # print(data_movies.isna().sum())
# # finish = datetime.now()
# # print(f"finished timer with time: {finish - start}")

# text_input = "shawshank"
# noOfTweet = 20

# start = datetime.now()

# search_data_movies = data_movies.loc[data_movies['movie_name'] == text_input]
# if len(search_data_movies) == 0:
#     search_data_movies = data_movies.loc[data_movies['movie_name'].str.contains(
#         text_input)]
# movies.adjust_reviews()

# twitter.get_tweets(text_input, noOfTweet)
# twitter.adjust_tweets()

# FinalOutput = pd.concat([search_data_movies, search_data_twitter], axis=0)

# finish = datetime.now()

# FinalOutput.to_csv("data/out.csv")

# print(f"finished timer with time: {finish - start}")
# # print(search_data_movies)
# # search_data_movies.to_csv("data/out.csv")
