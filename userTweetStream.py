from __future__ import absolute_import, print_function
import tweepy
import time
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.utils import import_simplejson
json = import_simplejson()


# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key= #your consumer_key
consumer_secret= #your consumer_secret

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token=#your access_token
access_token_secret=#your access_token_secret

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, api=None):
        super(StdOutListener, self).__init__()
        # add the cities of interest here
        self.location_list = [
        ("Chicago",[-88,41.6,-87.5,42]),
        #("London",[-0.4,51.4,0.2,51.6]),
        ("Rio",[-43.8,-23.0,-43.1,-22.7]),
        ("California",[-124.4,32.5,-114.1,42.0]),
        #("Tokyo",[2.2,48.8,2.5,48.9]),
        ("NewYork",[-74.2,40.5,-73.7,40.9])
        #("Miami",[-80.3,25.7,-80.1,25.9])
        ]
        self.number = 0
        self.user_dict = {}
        self.city=""
        
    def on_data(self, data):
        #print(data)
        try:
            jdata = json.loads(data)
            user_info = jdata['user']['screen_name']
            is_protected = jdata['user']['protected']
            self.number+=1
            if (is_protected==False) and (self.user_dict.has_key(user_info)==False):
                self.user_dict[user_info]=self.city
                saveFile=open('twitterDB.txt','a')
                writeThis = str( self.number + "," + self.city + "," +  str(user_info) )
                #saveFile.write("\n")
                saveFile.write(writeThis)
                #saveFile.write(str(self.user_dict))
                saveFile.write("\n")
                saveFile.close()
            # keep on gathering users from the stream until the count doesn't reach 1000
            if (self.number != 0) and (len(self.user_dict) % 1000 == 0):
                print ("Reached limit of 1000..stopping")
                return False # then move on to the next city by closing the current city stream
            else:
                return True


            #if (len(self.user_dict) > 80):
            #    return False
                #if (self.num_tweets > 20):
                #print ("reached limit of 20..stopping")
                #return False
        except BaseException, e:
            print('BaseException'+str(e))
            time.sleep(5)
    def on_error(self, status):
        print(status)


def start_streaming(l,auth):
    locationList = l.location_list
    for location in locationList:
        city,boundary = location
        l.city = city
        stream = Stream(auth,l)
        stream.filter(locations=boundary)
        time.sleep(5)



if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    start_streaming(l,auth)
