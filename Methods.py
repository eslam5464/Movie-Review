import os
import types
import uuid
import ibm_boto3
from ibm_botocore.client import Config
from ibm_botocore.exceptions import ClientError
import ibm_s3transfer.manager
import pandas as pd
import tweepy
import matplotlib.pyplot as plt
import numpy as np
import nltk
import re
import string
from wordcloud import WordCloud, STOPWORDS
from PIL import Image
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import CountVectorizer
import Credentials as cre
import logging
from datetime import datetime

data_movies = pd.DataFrame()
search_data_twitter = pd.DataFrame()
search_data_movies = pd.DataFrame()


class IBM:
    def __init__(self):
        # starting the logging for debugging purposes
        logging.basicConfig(filename="data/guiApp.log",
                            format='%(asctime)s -- %(name)s -- %(levelname)s -- %(funcName)s -- %(message)s',
                            filemode='a', datefmt='%Y/%m/%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(f"Initialize IBM class")
        
        # setting the credentials if IBM and other important variables to connect with IBM
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

    def download_file(self, fileName="FinalOutput01.csv"): # method to download the file from IBM cloud
        self.logger.debug(f"Downloading file")        
        global data_movies
        
        # check if the file exists in the location of the program
        if not os.path.exists("data/FinalOutput01.csv"):
            def __iter__(self): return 0
            
            # getting the body of the file from IBM cloud
            body = self.cos.get_object(
                Bucket='dataguru01-donotdelete-pr-9rmz8gbtdfizh7', Key=fileName)['Body']
            if not hasattr(body, "__iter__"):
                body.__iter__ = types.MethodType(__iter__, body)
            
            # read it as csv with the body from IBM API
            data_movies = pd.read_csv(body, dtype={"movie_name": "string",
                                                   "genre": "string",
                                                   "review_content": "string",
                                                   "critic_gender": "string",
                                                   "critic_contenental": "string",
                                                   "critic_country": "string",

                                                   }, parse_dates=['release_date', 'review_date'], low_memory=False, index_col=0)
            
            # saving the downloaded data frame onto the disk
            data_movies["movie_name"] = data_movies["movie_name"].str.lower()
            data_movies.to_csv("data/"+fileName)

            self.logger.debug(f"Downloading file - [finished]")
            
        # if the file already exists, just read it and dont download
        else:
            data_movies = pd.read_csv("data/"+fileName, index_col=0)
            data_movies["movie_name"] = data_movies["movie_name"].str.lower()
            self.logger.warning(f"The file exists when trying to download")

    def delete_item(self, object_name): # method to delete a file in IBM cloud
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
        # method to upload a file to IBM cloud
        self.logger.debug(f"Uploading file")
        self.cos.upload_file(Filename=fileName, Bucket=bucket, Key=key)
        # os.remove(fileName)  # comment if you need to see the file after it was uploaded
        self.logger.debug(f"Uploading file - [finished]")


class Twitter():
    def __init__(self):
        # starting the logging for debugging purposes
        logging.basicConfig(filename="data/guiApp.log",
                            format='%(asctime)s -- %(name)s -- %(levelname)s -- %(funcName)s -- %(message)s',
                            filemode='a', datefmt='%Y/%m/%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(f"Initialize Twitter class")

        nltk.download("vader_lexicon")
        nltk.download('stopwords')

        self.logger.debug(f"Initialize Twitter class - [finished]")

    def get_top_words(self, words="", ngram_range=(1, 1), n=5): # methods to get the most common 5 words in the desired dataframe
        global search_data_twitter
        words = search_data_twitter
        vec = CountVectorizer(ngram_range=ngram_range,
                              stop_words='english').fit(words["tweets"])
        bag_of_words = vec.transform(words["tweets"])
        sum_words = bag_of_words.sum(axis=0)
        words_freq = [(word, sum_words[0, idx])
                      for word, idx in vec.vocabulary_.items()]
        words_freq = sorted(words_freq, key=lambda x: x[1], reverse=True)[:n]

        out_word = []
        out_freq = []
        for word in words_freq:
            out_word.append(word[0])
            out_freq.append(word[1])
        x = [out_word]
        x.append(out_freq)
        out = pd.DataFrame(x, index=["word", "count"])
        return out.T

    def set_word_count(self): # makes a cloud with the most used words to the least in a cloud picture
        global search_data_twitter
        self.logger.debug(f"Started Word count")
        mask = np.array(Image.open("cloud.png"))
        stopwords = set(STOPWORDS)
        wc = WordCloud(background_color="white",
                       mask=mask,
                       max_words=3000,
                       stopwords=stopwords,
                       repeat=True)
        wc.generate(str(search_data_twitter["tweets"]))
        wc.to_file("wc.png")
        img = Image.open(r"wc.png")
        img.show()
        self.logger.debug(f"Started Word count - [finished]")

    def analyze_tweet(self, dataFrame: pd.DataFrame(), text_to_analyze: str):
        self.logger.debug(f"Analyzing tweet")
        
        # using NLTK, start analyzing the desired scentence
        score = SentimentIntensityAnalyzer().polarity_scores(text_to_analyze)
        neg = score['neg']
        neu = score['neu']
        pos = score['pos']
        comp = score['compound']
        
        # for each if condition checks if the scentence is leaning to which side
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
        
        # get the keys from the "Credentials.py" file associated beside this file
        consumerKey = cre.twitter_consumerKey
        consumerSecret = cre.twitter_consumerSecret
        accessToken = cre.twitter_accessToken
        accessTokenSecret = cre.twitter_accessTokenSecret

        auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
        auth.set_access_token(accessToken, accessTokenSecret)
        api = tweepy.API(auth)

        # using tweepy, getting the tweets for the desired search word with the variable
        # "noOfTweet" to be the desired number of tweets you want
        tweets = tweepy.Cursor(api.search, q=keyword).items(noOfTweet)
        tweet_list = []
        for tweet in tweets:
            tweet_list.append(tweet.text)

        self.logger.debug(
            f"Getting tweet with keyword: {keyword} - [finished]")
        
        # returning the searched tweets to the global dataframe to acess it easier
        global search_data_twitter
        search_data_twitter = pd.DataFrame(tweet_list, columns=['tweets'])

    def clean_tweet(self, text_to_clean: str):
        self.logger.debug(f"Cleaning tweet")
        
        # collecting stopwords from NLTK library
        stopword = nltk.corpus.stopwords.words('english')
        
        # removing punctuation
        text_lc = "".join([word.lower()
                           for word in text_to_clean if word not in string.punctuation])
        text_rc = re.sub('[0-9]+', '', text_lc)
        
        # applying tokenization
        tokens = re.split('\W+', text_rc)

        # applying stemmer for all the words while not being in the stopwords
        ps = nltk.PorterStemmer()
        text_to_clean = [ps.stem(word)
                         for word in tokens if word not in stopword]

        self.logger.debug(f"Cleaning tweet - [finished]")
        return " ".join(text_to_clean)

    def adjust_tweets(self): # combing the two methids (clean_tweet & analyze_tweet) together
        self.logger.debug(f"Adjusting tweets")
        global search_data_twitter

        search_data_twitter["cleaned_tweet"] = search_data_twitter.apply(
            lambda x: self.clean_tweet(x["tweets"]), axis=1)

        search_data_twitter = search_data_twitter.apply(
            lambda x: self.analyze_tweet(x, x["cleaned_tweet"]), axis=1)

        self.logger.debug(f"Adjusting tweets - [finished]")


class Movies():
    def __init__(self):
        # starting the logging for debugging purposes
        logging.basicConfig(filename="data/guiApp.log",
                            format='%(asctime)s -- %(name)s -- %(levelname)s -- %(funcName)s -- %(message)s',
                            filemode='a', datefmt='%Y/%m/%d %H:%M:%S')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(f"Initialize Movies class")

        self.logger.debug(f"Initialize Movies class - [finished]")

    def analyze_review(self, dataFrame: pd.DataFrame(), text_to_analyze: str):
        self.logger.debug(f"Analyzing review")
        
        # using NLTK, start analyzing the desired scentence
        score = SentimentIntensityAnalyzer().polarity_scores(text_to_analyze)
        neg = score['neg']
        neu = score['neu']
        pos = score['pos']
        comp = score['compound']
        
        # for each if condition checks if the scentence is leaning to which side
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
        
        # collecting stopwords from NLTK library
        stopword = nltk.corpus.stopwords.words('english')
        
        # removing punctuation
        text_lc = "".join([word.lower()
                           for word in text_to_clean if word not in string.punctuation])
        text_rc = re.sub('[0-9]+', '', text_lc)
        
        # applying tokenization
        tokens = re.split('\W+', text_rc)
        
        # applying stemmer for all the words while not being in the stopwords
        ps = nltk.PorterStemmer()
        text_to_clean = [ps.stem(word)
                         for word in tokens if word not in stopword]

        self.logger.debug(f"Cleaning review - [finished]")
        
        # returning the text with spaces because of cleaning turned it into list or words
        return " ".join(text_to_clean)

    def adjust_reviews(self): # combing the two methids (clean_review & analyze_review) together
        global search_data_movies
        search_data_movies["review_content_cleaned"] = search_data_movies.apply(
            lambda x: self.clean_review(x["review_content"]), axis=1)

        search_data_movies = search_data_movies.apply(
            lambda x: self.analyze_review(x, x["review_content"]), axis=1)
