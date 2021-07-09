from kivy.app import App
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.image import Image
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from PIL import Image as pilImg
import webbrowser
import Methods
import pandas as pd
import numpy as np
import os

dashboard_URL = r"PUT_URL_HERE"

def init(self):
    self.methods = Methods

    self.ibm = self.methods.IBM()
    self.twitter = self.methods.Twitter()
    self.movies = self.methods.Movies()
    self.ibm.download_file()
    self.text_input = ""
    self.noOfTweet = 10 # you can change the number of the desired tweets you want to put in the dashboard


def start_processing(self):
    self.methods.search_data_movies = self.methods.data_movies.loc[
        self.methods.data_movies['movie_name'] == self.text_input]# checks for the searched text in the data frame with the literal word

    if len(self.methods.search_data_movies) == 0: # checks for the searched text in the data frame
        self.methods.search_data_movies = self.methods.data_movies.loc[self.methods.data_movies['movie_name'].str.contains(
            self.text_input)]

    if len(self.methods.search_data_movies) == 0: # if the two methods of searching didnt work out the program will stop searching
        self.lb_searched_text.text = f"No results were found for {self.lb_searched_text.text}, please refince your search words"

    else:
        # clean & analyze data frame of the reviews for the searched movie name
        self.movies.adjust_reviews() 
        self.methods.search_data_movies = self.methods.search_data_movies.reset_index()
           
        # clean & analyze the tweets you searched for
        self.twitter.get_tweets(self.text_input, self.noOfTweet)
        self.twitter.adjust_tweets() 
        self.methods.search_data_twitter = self.methods.search_data_twitter.reset_index()

        # gets the most common words in the tweets
        self.words = self.twitter.get_top_words().reset_index() 
        
        # concatenating all of the above dataframes to -> FinalOutput
        self.FinalOutput = pd.concat(
            [self.methods.search_data_movies, self.methods.search_data_twitter, self.words], axis=1)

        features = ["review_sent_negative", "tweet_sent_negative", "tweet_sent_postive",
                    "tweet_sent_neutral", "count", "review_sent_postive", "review_sent_neutral"]
        self.FinalOutput.drop(["index"], axis=1, inplace=True)

        for feat in features:
            self.FinalOutput[feat].fillna("75896123", inplace=True)
            self.FinalOutput[feat] = self.FinalOutput[feat].astype('int64')
            self.FinalOutput[feat] = self.FinalOutput[feat].replace(
                75896123, np.nan)
        print("###  info after:")
        print(self.FinalOutput.info())

        self.FinalOutput.to_csv('data/DashboardInput.csv',
                                index=False, header=True)

        # delete the file if it exists in IBM cloud
        self.ibm.delete_item('DashboardInput.csv')
        
        # upload the new file to IBM cloud
        self.ibm.upload_file()

        self.lb_searched_text.text = f"Searched for '{self.tb_movie.text}'"

        # self.twitter.set_word_count()


class MovieSearch(App):
    def build(self):
        self.window = GridLayout()
        self.window.cols = 1
        self.window.size_hint = (0.7, 0.8)
        self.window.pos_hint = {"center_x": 0.5, "center_y": 0.5}
        self.window.add_widget(Image(source="logo.png"))

        self.lb_search = Label(
            text="Search for a movie:",
            font_size=18,
            color='#00FFCE'
        )

        self.window.add_widget(self.lb_search)

        self.tb_movie = TextInput(
            multiline=False,
            padding_y=(20, 20),
            size_hint=(1, 0.5)
        )

        self.window.add_widget(self.tb_movie)

        self.button = Button(
            text="Search",
            size_hint=(1, 0.5),
            bold=True,
            background_color='#00FFCE'
        )
        self.button.bind(on_press=self.callback)
        self.window.add_widget(self.button)

        self.lb_searched_text = Label(
            text=" ",
            font_size=15,
            color='#00FFCE'
        )

        self.window.add_widget(self.lb_searched_text)

        init(self)

        return self.window

    def callback(self, instance):
        self.lb_searched_text.text = "Processing data!!....."
        self.text_input = self.tb_movie.text.lower()
        print(f"searched for {self.tb_movie.text.lower()}")
        start_processing(self)

        webbrowser.open(dashboard_URL)


if __name__ == "__main__":
    MovieSearch().run()
