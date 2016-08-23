import urllib
import base64
import httplib
from tweepy.utils import import_simplejson
json = import_simplejson()

CONSUMER_KEY = ''
CONSUMER_SECRET = ''
encoded_CONSUMER_KEY = urllib.quote(CONSUMER_KEY)
encoded_CONSUMER_SECRET = urllib.quote(CONSUMER_SECRET)

concat_consumer_url = encoded_CONSUMER_KEY + ":" + encoded_CONSUMER_SECRET

host = 'api.twitter.com'
url = '/oauth2/token/'
params = urllib.urlencode({'grant_type' : 'client_credentials'})
req = httplib.HTTPSConnection(host)
req.putrequest("POST", url)
req.putheader("Host", host)
req.putheader("User-Agent", "My Twitter 1.1")
req.putheader("Authorization", "Basic %s" % base64.b64encode(concat_consumer_url))
req.putheader("Content-Type" ,"application/x-www-form-urlencoded;charset=UTF-8")
req.putheader("Content-Length", "29")
req.putheader("Accept-Encoding", "utf-8")

req.endheaders()
req.send(params)
resp = req.getresponse()

print resp.status, resp.reason
print resp.read()
