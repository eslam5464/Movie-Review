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

from sklearn.preprocessing import LabelEncoder


def init(self):
    self.methods = Methods

    self.ibm = self.methods.IBM()
    self.twitter = self.methods.Twitter()
    self.movies = self.methods.Movies()
    self.ibm.download_file()
    self.text_input = ""
    self.noOfTweet = 10


def start_processing(self):
    self.methods.search_data_movies = self.methods.data_movies.loc[
        self.methods.data_movies['movie_name'] == self.text_input]

    if len(self.methods.search_data_movies) == 0:
        self.methods.search_data_movies = self.methods.data_movies.loc[self.methods.data_movies['movie_name'].str.contains(
            self.text_input)]

    if len(self.methods.search_data_movies) == 0:
        self.lb_searched_text.text = f"No results were found for {self.lb_searched_text.text}, please refince your search words"

    else:
        self.movies.adjust_reviews()
        self.methods.search_data_movies = self.methods.search_data_movies.reset_index()

        self.twitter.get_tweets(self.text_input, self.noOfTweet)
        self.twitter.adjust_tweets()
        self.methods.search_data_twitter = self.methods.search_data_twitter.reset_index()

        self.words = self.twitter.get_top_words().reset_index()

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

        self.ibm.delete_item('DashboardInput.csv')

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

        url = r"https://eu-gb.dataplatform.cloud.ibm.com/dashboards/e8d08e6c-a884-46c4-bf21-f92c778f624d/view/5f09d679279c23d014cecce407992d0278332459e1bb8757d38c7b490f607597a8604099c87d4f5ad2155431f2ee400b9a"
        webbrowser.open(url)


if __name__ == "__main__":
    MovieSearch().run()
