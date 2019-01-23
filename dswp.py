import sys
import getopt
import json
import requests
import re

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


class listener(StreamListener):
    
    def __init__(self, solrHost, collectionName):
        self.solrHost = solrHost
        self.collectionName = collectionName

    def on_data(self, data):
        tweet = json.loads(data)
        result = re.sub(r'<\s*a[^>]*>(.*?)<\s*/\s*a>', '\g<1>', tweet["source"])
        tweet["source_app"] = re.sub(r"(twitter\s*|for\s*)", "", result.lower())
        d = '{"add":{ "doc":' + json.dumps(tweet) + ',"boost":1.0,"overwrite":true, "commitWithin": 1000}}'
        url = "http://{}/api/collections/{}/update/json?f=$FQN:/**".format(self.solrHost, self.collectionName)
        r = requests.post(url, headers={"Content-Type":"application/json"}, data=d)
        return(True)

    def on_status(self, status):
        print(status.text)

    def on_error(self, status):
        print("Error: {}".format(status))

    def postToSolr(self, tweets):
        self.solr.add(tweets)



def usage():
    print("""
        dswp.py -s [solrhost:port] -c [collection name] -f [words separated by spaces to filter on the stream]
    """)


def main(argv):
    solrHost = ""
    collectionName = ""
    f = ""
    try:
        opts, args = getopt.getopt(argv, "c:s:f:")
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    if len(opts) < 3:
        print("Faltan argumentos")
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-s':
            solrHost = arg
        elif opt in "-c":
            collectionName = arg
        elif opt in "-f":
            f = arg   
 
    # Twitter api keys
    ckey="[]"
    csecret="[]"
    atoken="[]"
    asecret="[]"

    ckey="ZYNmbbb7SmJssbFEOSyv3m8tb"
    csecret="CL30LHpoPwsIyJ8pmhHKauzn7qEtM22G0LslOd5Apg1H0z4aTN"
    atoken="285959375-GkbbbsZgGE5s4coVtlMk1R0ucMcHqTeyEfysOB3e"
    asecret="LTWO6uB4ruIfv9FWqCi6P4tIQRl1C3J2UHSR31Mh292us"

    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)

    twitterStream = Stream(auth, listener(solrHost, collectionName))
    twitterStream.filter(track=[f])


if __name__ == "__main__":
    main(sys.argv[1:])
