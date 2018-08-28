# coding=utf-8
import re
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import string
from nltk.stem.porter import *
from nltk import FreqDist
import warnings
from wordcloud import WordCloud
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score


# usage remove_pattern("@[\w]*", combined)
def remove_pattern(pattern, input_text):
    r = re.findall(pattern, input_text)
    for i in r:
        input_text = re.sub(i, '', input_text)
    return input_text


# extract hashtags
def extract_hashtags(input_text):
    hashtags = []
    for word in input_text:
        ht = re.findall(r"#(\w+)", word)
        hashtags.append(ht)
    return hashtags


warnings.filterwarnings("ignore", category=DeprecationWarning)
# %matplotlib nbagg

train = pd.read_csv("c:\\data\\train_tweets.csv")
test = pd.read_csv("c:\\data\\test_tweets.csv")
# train.head()
combined = train.append(test, ignore_index=True, sort=True)
# remove user tags (@user)
combined['tidy_tweet'] = np.vectorize(remove_pattern)("@[\w]*", combined['tweet'])
# replace all other than alphabets and hashtag(#) with space
combined['tidy_tweet'] = combined['tidy_tweet'].str.replace("[^a-zA-Z#]", " ")
# removing words which is having 3 or less chars
combined['tidy_tweet'] = combined['tidy_tweet'].apply(lambda x: ' '.join(a for a in x.split() if len(a) > 3))  # []
# tokenizing each word
tokenized_tweets = combined['tidy_tweet'].apply(lambda x: x.split())
# Stemming is a rule-based process of stripping the suffixes (“ing”, “ly”, “es”, “s” etc) from a word.
# For example, “play”, “player”, “played”, “plays” and “playing” are the different variations of the word – “play”.
stemmer = PorterStemmer()
tokenized_tweets = tokenized_tweets.apply(lambda x: [stemmer.stem(i) for i in x])
# joining tokenized(list type) datasets back to combined dataset
for i in range(len(tokenized_tweets)):
    tokenized_tweets[i] = ' '.join(tokenized_tweets[i])
combined['tokenized_tweets'] = tokenized_tweets
# visualize all words using wordcloud plot
all_words = ' '.join(i for i in combined['tokenized_tweets'])
# common words used in tweets
wordcloud = WordCloud(width=800, height=500, random_state=21, max_font_size=110).generate(all_words)
plt.figure(figsize=(10, 7))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis('off')
plt.show()
# words in non racist/sexist tweets
normal_words = ' '.join([i for i in combined['tokenized_tweets'][combined['label'] == 0]])
wordcloud = WordCloud(width=800, height=500, random_state=21, max_font_size=110).generate(normal_words)
plt.figure(figsize=(10, 7))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis('off')
plt.show()
# words in racist/sexist tweets(negative words)
negative_words = ' '.join([i for i in combined['tokenized_tweets'][combined['label'] == 1]])
wordcloud = WordCloud(width=800, height=500, random_state=21, max_font_size=110).generate(negative_words)
plt.figure(figsize=(10, 7))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis('off')
plt.show()
# analysing hashtags
# extracting hashtags from non racist/sexist tweets
ht_regular = extract_hashtags(combined['tokenized_tweets'][combined['label'] == 0])
# extracting hashtags from racist/sexist tweets
ht_negative = extract_hashtags(combined['tokenized_tweets'][combined['label'] == 1])
# extracting from nested list
ht_regular = sum(ht_regular, [])  # concatenate list
ht_negative = sum(ht_negative, [])
# Non-Racist/Sexist Tweets
countByWords = nltk.FreqDist(ht_regular)
countByWordsDF = pd.DataFrame({'Hashtag': list(countByWords.keys()), 'Count': list(countByWords.values())})
# selecting top 10 most frequent hashtags
largestTopTen = countByWordsDF.nlargest(columns="Count", n=10)
plt.figure(figsize=(16, 5))
ax = sns.barplot(data=largestTopTen, x="Hashtag", y="Count")
ax.set(ylabel='Count')
plt.show()
# Racist/Sexist Tweets
countByRacistWords = nltk.FreqDist(ht_negative)
countByRacistWordsDF = pd.DataFrame(
    {'Hashtag': list(countByRacistWords.keys()), 'Count': list(countByRacistWords.values())})
# selecting top 10 most frequent hashtags
largestTopTenRacist = countByRacistWordsDF.nlargest(columns="Count", n=10)
plt.figure(figsize=(16, 5))
ax = sns.barplot(data=largestTopTenRacist, x="Hashtag", y="Count")
ax.set(ylabel='Count')
plt.show()
# --------------------------------------------------------------------------------------------------------
# Building logistic regression model (classification model)
# bag-of-words feature
bow_vectorizer = CountVectorizer(max_df=0.90, min_df=2, max_features=1000, stop_words='english')
# bag-of-words matrix
bow = bow_vectorizer.fit_transform(combined['tokenized_tweets'])
# TF-IDF feature
tfidf_vectorizer = TfidfVectorizer(max_df=0.90, min_df=2, max_features=1000, stop_words='english')
# TF-IDF feature matrix
tfidf = tfidf_vectorizer.fit_transform(combined['tokenized_tweets'])

# Building model using Bag-of-Words features
train_bow = bow[:31962, :]
test_bow = bow[31962:, :]
# splitting data into training and validation set
xtrain_bow, xvalid_bow, ytrain, yvalid = train_test_split(train_bow, train['label'], random_state=42, test_size=0.3)
lreg = LogisticRegression()
lreg.fit(xtrain_bow, ytrain) # training the model
prediction = lreg.predict_proba(xvalid_bow) # predicting on the validation set
prediction_int = prediction[:, 1] >= 0.3 # if prediction is greater than or equal to 0.3 than 1 else 0
prediction_int = prediction_int.astype(np.int)
f1_score(yvalid, prediction_int) # calculating f1 score (0.5017421602787456)
# use above model to predict for the test data.
test_pred = lreg.predict_proba(test_bow)
test_pred_int = test_pred[:, 1] >= 0.3
test_pred_int = test_pred_int.astype(np.int)
test['label'] = test_pred_int
submission = test[['id', 'label']]
submission.to_csv('c:\\data\\output\\sub_lreg_bow.csv', index=False) # writing data to a CSV file
# Building model using TF-IDF features
train_tfidf = tfidf[:31962, :]
test_tfidf = tfidf[31962:, :]

xtrain_tfidf = train_tfidf[ytrain.index]
xvalid_tfidf = train_tfidf[yvalid.index]
lreg.fit(xtrain_tfidf, ytrain)

prediction = lreg.predict_proba(xvalid_tfidf)
prediction_int = prediction[:, 1] >= 0.3
prediction_int = prediction_int.astype(np.int)

f1_score(yvalid, prediction_int)    # 0.5091240875912408
