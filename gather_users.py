from __future__ import absolute_import, print_function
import time
import datetime
import urllib2
import urllib
import httplib
json = import_simplejson()
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after

bearer_token = '' # insert your bearer token here
def get_all_tweets(user_screen_name):
    try:
        alltweets=[]
        alltweets_next_200=[]
        timeline_request = urllib2.Request("https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name="+str(user_screen_name)+"&count=200")
        timeline_request.add_header("Authorization", "Bearer %s" % bearer_token)
        timeline_response = urllib2.urlopen(timeline_request)
        timeline_contents = timeline_response.read()
        jdata = json.loads(timeline_contents)
        alltweets = [s['text'].encode('utf-8') for s in jdata]
        alltweets_id = [ids['id'] for ids in jdata]
        oldest_tweet_id = alltweets_id[-1] - 1
        timeline_request = urllib2.Request("https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name="+str(user_screen_name)+"&count=200"+"&max_id="+str(oldest_tweet_id))
        timeline_request.add_header("Authorization", "Bearer %s" % bearer_token)
        timeline_response = urllib2.urlopen(timeline_request)
        timeline_contents = timeline_response.read()
        jdata = json.loads(timeline_contents)
        alltweets_next_200 = [s['text'].encode('utf-8') for s in jdata]

        alltweets.extend(alltweets_next_200)
        time.sleep(50.0 / 1000.0)
        return alltweets
    except urllib2.HTTPError as err:
        if err.code == 429:
            print('Limit exceeded')
        elif err.code == 404:
            return ("not-found")
            pass
        else:
            print(str(err))

if __name__ == "__main__":
    tweet_list = get_all_tweets
    sc = SparkContext(appName="Spark-twitter-user")
    input_rdd = sc.textFile('/home/ec2-user/opt/tmp_python/twitterDB.csv').map(lambda entry: tuple(entry.split(",")))
    print('input_rdd loaded')
    print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    london_filter_rdd = input_rdd.filter(lambda (loc,user): loc=='London')
    london_rdd = london_filter_rdd.mapValues(lambda user : tweet_list(user))
    print('London RDD created')
    time.sleep(61*15)
    chicago_filter_rdd = s1.filter(lambda (loc,user): loc=='Chicago')
    chicago_rdd = chicago_filter_rdd.mapValues(lambda user : tweet_list(user))
    print('Chicago RDD created')
    time.sleep(61*15)
    newyork_filter_rdd = s1.filter(lambda (loc,user): loc=='NewYork')
    newyork_rdd = newyork_filter_rdd.mapValues(lambda user : tweet_list(user))
    print('NewYork RDD created')
    time.sleep(61*15)
    california_filter_rdd = s1.filter(lambda (loc,user): loc=='California')
    california_rdd = california_filter_rdd.mapValues(lambda user : tweet_list(user))
    print('California RDD created')
    time.sleep(61*15)
    joinedrdd_1 = london_rdd.join(chicago_rdd)
    joinedrdd_2 = joinedrdd_1.join(newyork_rdd)
    joinedrdd_3 = joinedrdd_2.join(california_rdd)

    joinedrdd_3.saveAsTextFile('/home/ec2-user/opt/tmp_python/twitter-users.txt')
    print('All done,file created')
    print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    sys.exit()
