from ssl import CERT_NONE
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import feedparser
from pymongo import MongoClient
import numpy as np
import pandas as pd

#import nltk
#nltk.download('vader_lexicon')
#nltk.download("stopwords")

url2 = 'https://globalnews.ca/sports/feed/'
#"https://www.ctvnews.ca/rss/ctvnews-ca-sci-tech-public-rss-1.822295"
#'https://globalnews.ca/sports/feed/'
# #'https://www.cbc.ca/news/canada/toronto/toronto-murder-charges-1.6231570'
#'https://www.nytimes.com/article/day-of-the-dead-mexico.html'
#'https://www.nytimes.com/2021/10/29/technology/meta-facebook-zuckerberg.html'
#'https://toronto.ctvnews.ca/police-name-woman-67-found-dead-in-her-north-york-home-son-in-custody-1.5645927'
#'https://www.ctvnews.ca/sci-tech/an-eraser-button-focused-ideas-could-help-bridle-big-tech-1.5645841'


url={
     "GBL": "https://globalnews.ca/sports/feed/"
    ,"CBC": "https://www.cbc.ca/cmlink/rss-sports"
    ,"FOX": "https://api.foxsports.com/v1/rss?partnerKey=zBaFxRyGKCfxBagJG9b8pqLyndmvo7UU"
    ,"CTV": "https://www.ctvnews.ca/rss/sports/ctv-news-sports-1.3407726"
}

stop_words = set(stopwords.words("english"))
summary = list()

def SentimentCategory(value):
    if value >= 0.3: 
        return 'Positive' 
    elif value < 0.3 and value >= -0.3: 
        return 'Neutral'
    elif value < -0.3:
        return 'Negative'


def RSSToMongo(url, source):
    f = feedparser.parse(url)
#    print(f.entries)
    for entry in f.entries:
        filterList= []
        recdict = dict()
        wordToken = word_tokenize(entry.title)   

        '''
        dict_keys(['title', 'title_detail', 'links', 'link', 'authors', 'author', 'author_detail', 'published', 'published_parsed', 
            'tags', 'id', 'guidislink', 'summary', 'summary_detail', 'content', 'post-id', 'media_thumbnail', 'href', 'media_content'])
        '''

        for word in wordToken:        
            if word.lower() not in stop_words:
                filterList.append(word)

        recdict["source"] = source
        recdict["summary"] = ' '.join(filterList)

        VD = SentimentIntensityAnalyzer()
        temp = VD.polarity_scores(' '.join(filterList))

        recdict["PositiveScore"] = temp['neg']
        recdict["NeutralScore"] = temp['neu']
        recdict["NegativeScore"] = temp['pos']
        recdict["CompoundScore"] = temp['compound']
        recdict["SentimentCategory"] = SentimentCategory(temp['compound'])

#        print(recdict)
        if isinstance(recdict, dict):
            db.demo.insert_one(recdict)  
#            db.feeds.insert_one(recdict)  

#        db.feeds.insert_one(recdict)


#connection string of mongoDB
client = MongoClient('mongodb+srv://jay:haha1234@sdmcluster.6rfbh.mongodb.net/exchange?ssl=true&ssl_cert_reqs=CERT_NONE')
db = client.get_database('RSS')
#records = db.foxNews



def mongoDocExport():
    fields={}
    series_list=[]
    for doc in db.demo.find({}):
        for key,val in doc.items():
            try: fields[key] = np.append(fields[key], val)
            except KeyError: fields[key] = np.array([val])

    #print(fields)

    for key, val in fields.items():
        if key != "_id":
            fields[key] = pd.Series(fields[key])
            series_list += [fields[key]]

    #print(series_list)

    df_series = {}
    for num, series in enumerate(series_list):
        # same as: df_series["data 1"] = series
        df_series['data ' + str(num)] = series

    mongo_df = pd.DataFrame(df_series)
    column=["Text","PositiveScore","NeutralScore","NegativeScore","CompoundScore","SentimentCategory"]
#    mongo_df.rename(columns=column)
    print(mongo_df)

#    print ("\nmongo_df:", mongo_df)
    mongo_df.to_csv(r'C:\\Users\\JAY\\Desktop\\dema.csv')

for key,value in url.items():
    if key == "GBL": source = "Global News"
    elif key == "CBC": source = "Canadian Broadcasting Corporation"
    elif key == "FOX": source = "Fox News"
    elif key == "CTV": source = "Canadian Television Network"
    
    RSSToMongo(value, source)
    print("uploaded Source : ", source)



mongoDocExport()


#print(RemoveStopWords(url2, "source"))


'''
a = Article(url)


a.download()
a.parse()
a.nlp()

text = a.text
print(text)

blob = TextBlob(text)
senti = blob.sentiment.polarity
print('Sentiment Analysis Polarity: ', senti)


def polarityCategory(senti):
    if senti >= -1 and senti < -0.2:
        category = 'Negative'
    elif senti >= -0.2 and senti < 0.2:
        category = 'Neutral'
    elif senti >= 0.2 and senti <= 1 :
        category = 'Positive'
    
    return category

print(polarityCategory(senti))
'''