from __future__ import absolute_import, print_function 
import time 
import datetime 
import urllib2 
import httplib 
import requests 
import json 
from pyspark import SparkConf,SparkContext
import sys

# Go to http://apps.twitter.com and create an app. The consumer key and secret will be generated for you
bearer_token = ''
#get_all_tweets would gather the first 400 tweets from a twitter profile

def get_all_tweets(user_screen_name):
    try:
        alltweets=[]
        alltweets_next_200=[]
        timeline_request = urllib2.Request("https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name="+str(user_screen_name)+"&count=200")
        timeline_request.add_header("Authorization", "Bearer %s" % bearer_token)
        timeline_response = urllib2.urlopen(timeline_request)
        timeline_contents = timeline_response.read()
        jdata = json.loads(timeline_contents)
        alltweets = [status for status in jdata]
        alltweets_id = [ids['id'] for ids in jdata]
        oldest_tweet_id = alltweets_id[-1] - 1
        timeline_request = urllib2.Request("https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name="+str(user_screen_name)+"&count=200"+"&max_id="+str(oldest_tweet_id))
        timeline_request.add_header("Authorization", "Bearer %s" % bearer_token)
        timeline_response = urllib2.urlopen(timeline_request)
        timeline_contents = timeline_response.read()
        jdata = json.loads(timeline_contents)
        alltweets_next_200 = [status for status in jdata]
        alltweets.extend(alltweets_next_200)
        time.sleep(50.0 / 1000.0)
        statuses_list = map(convert_to_dict,alltweets)
        time.sleep(50.0 / 1000.0)
        return get_personality ({'contentItems': statuses_list})
    except urllib2.HTTPError as err:
        if err.code == 429:
            print('Limit exceeded')
        elif err.code == 404:
            return ("not-found")
            pass
        else:
            print(str(err))


# get_personality would send requests to IBM-watson personality-insights service to get the personality assessment for that user
def get_personality(user_content):
    perloc_data = requests.post("https://gateway.watsonplatform.net/personality-insights/api" + "/v2/profile",
                      auth=("", ""),
                      headers={
                          'content-type': 'application/json',
                          'accept': 'application/json'
                      },
                      data=json.dumps(user_content)
                      )
    time.sleep(100/1000)
    return (extract_big5_elements (json.loads(perloc_data.text)))


# extract_big5_elements would filter out the json that is returned by the personality-insights service
def extract_big5_elements(parent_json_data):
    user_dict = {}
    for idx in range(0,5):
        if parent_json_data["tree"]["children"][0]["children"][0]["children"][idx]["id"] == "Openness" :
            user_dict.update(update_entry(idx,3,parent_json_data))
        if parent_json_data['tree']['children'][0]['children'][0]['children'][idx]['id'] == 'Conscientiousness' :
            user_dict.update(update_entry(idx,2,parent_json_data))
        if parent_json_data['tree']['children'][0]['children'][0]['children'][idx]['id'] == 'Extraversion' :
            user_dict.update(update_entry(idx,5,parent_json_data))
        if parent_json_data['tree']['children'][0]['children'][0]['children'][idx]['id'] == 'Agreeableness' :
            user_dict.update(update_entry(idx,1,parent_json_data))
        if parent_json_data['tree']['children'][0]['children'][0]['children'][idx]['id'] == 'Neuroticism' :
            user_dict.update(update_entry(idx,4,parent_json_data))
    return user_dict def update_entry(idx,index,parent_json_data):
    trait_name = parent_json_data["tree"]["children"][0]["children"][0]["children"][idx]["children"][index]["id"]
    trait_percentage = parent_json_data["tree"]["children"][0]["children"][0]["children"][idx]["children"][index]["percentage"]
    return {trait_name:trait_percentage}


# convert_to_dict would properly format the statuses of the user (i.e. it would make more descriptive)
def convert_to_dict(obj):
    return {
        'userid': str(obj['user']['id']),
        'id': str(obj['id']),
        'sourceid': 'python-twitter',
        'contenttype': 'text/plain',
        'language': obj['lang'],
        'content': obj['text'],
        'reply': ((obj['in_reply_to_status_id']) == None),
        'forward': False
    }


# calculate_average would be called by reduceByKey() function to get the average personality trait for each location
def calculate_average(a,b):
    sum_dict = {}
    for key in a:
        sum_dict[key] = ((a[key] + b[key])/1000) # divide by the total number of users in a location
    return sum_dict 


if __name__ == "__main__":
    tweet_list = get_all_tweets
    get_avg = calculate_average
    my_conf = (SparkConf()
         .setAppName("Spark-twitter-user")
         .set("spark.network.timeout", "1200s")) # set timeout to 20 minutes as spark job
         # would sit idle for 15 minutes in order to refresh the request window of twitter API
    sc = SparkContext(conf=my_conf)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId","") #specify your AWSAccessKey here
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey","") #specify your AWSSecretKey here
    input_rdd = sc.textFile('s3n://perloc/twitterDB.txt/').map(lambda entry: tuple(entry.split(",")))
    print('input_rdd loaded')
    #print the start-time of the spark-job
    print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    ''' We'll create subsets from the main rdd based on the key, i.e. location
     and work upon each rdd for every window of 15 minutes
     (twitter restrictions apply here, see twitter-API-rate-limits)
     https://dev.twitter.com/rest/public/rate-limits
    '''
    rio_filter_rdd = input_rdd.filter(lambda (loc,user): loc=='Rio')
    rio_rdd = rio_filter_rdd.mapValues(lambda user : tweet_list(user))
    print('Rio RDD created')
    time.sleep(61*15) # required because of the API-rate limit
    chicago_filter_rdd = input_rdd.filter(lambda (loc,user): loc=='Chicago')
    chicago_rdd = chicago_filter_rdd.mapValues(lambda user : tweet_list(user))
    print('Chicago RDD created')
    time.sleep(61*15)
    newyork_filter_rdd = input_rdd.filter(lambda (loc,user): loc=='NewYork')
    newyork_rdd = newyork_filter_rdd.mapValues(lambda user : tweet_list(user))
    print('NewYork RDD created')
    time.sleep(61*15)
    california_filter_rdd = input_rdd.filter(lambda (loc,user): loc=='California')
    california_rdd = california_filter_rdd.mapValues(lambda user : tweet_list(user))
    print('California RDD created')
    # Once done with the personality analysis for each location, simply gather them all in one rdd
    finalRDD = sc.union([rio_rdd,chicago_rdd,newyork_rdd,california_rdd])
                 .reduceByKey(lambda user1,user2 : get_avg(user1,user2)) # get the average of each personality trait for each location
    finalRDD.repartition(1).saveAsTextFile("s3n://perloc/twitter-ibm/personality-profile.txt")
    print('All done,file created')
    print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
    sys.exit()
