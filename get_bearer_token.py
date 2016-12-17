import base64
import requests
from tweepy.utils import import_simplejson
json = import_simplejson()

CONSUMER_KEY = ''
CONSUMER_SECRET = ''
encoded_CONSUMER_KEY = requests.utils.quote(CONSUMER_KEY,safe='')
encoded_CONSUMER_SECRET = requests.utils.quote(CONSUMER_SECRET,safe='')

concat_consumer_url = encoded_CONSUMER_KEY + ":" + encoded_CONSUMER_SECRET

host_url = 'https://api.twitter.com/oauth2/token/'
base64_encode_bytes = base64.b64encode(bytes(concat_consumer_url,'utf-8'))
param_dict = {'grant_type' : 'client_credentials'}
my_header = { "Host" : 'api.twitter.com',
              "User-Agent" : "My Twitter 1.1",
              "Authorization" : "Basic " + base64_encode_bytes.decode('utf-8'),
              "Content-Type" : "application/x-www-form-urlencoded;charset=UTF-8",
              "Content-Length" : "29",
              "Accept-Encoding" : "utf-8" }
resp = requests.post(host_url,data=param_dict,headers=my_header)
print (resp.status_code)
print (resp.text)



#resp = req.getresponse()

#
