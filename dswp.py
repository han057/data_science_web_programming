import sys
import getopt
import json
import requests
import pysolr

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


class listener(StreamListener):

    def __init__(self, zooKeeperHost, collectionName, n):
        self.n = n
        self.tweets = []
        zookeeper = pysolr.ZooKeeper(zooKeeperHost)
        self.solr = pysolr.SolrCloud(zookeeper, collectionName)

    def on_data(self, data):
        self.tweets.append(json.loads(data))
        if(len(self.tweets) > self.n):
            print("Savin {} news tweets".format(self.n))
            self.postToSolr(self.tweets)
            self.tweets = []        
        return(True)

    def on_status(self, status):
        print(status.text)

    def on_error(self, status):
        print("Error: {}".format(status))

    def postToSolr(self, tweets):
        self.solr.add(tweets)



def usage():
    print("""
        dswp.py -z [zookeeperhost:port] -c [collection name] -n [Number of tweets before send to solr] -f [words separated by spaces to filter on the stream]
    """)


def main(argv):
    zooKeeperHost = ""
    collectionName = ""
    n = 100
    f = ""
    try:
        opts, args = getopt.getopt(argv, "c:z:n:f")
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    if len(opts) < 4:
        print("Faltan argumentos")
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-z':
            zooKeeperHost = arg
        elif opt in "-c":
            collectionName = arg
        elif opt in "-n":
            n = arg
        elif opt in "-f":
            f = arg
    
    pysolr.ZooKeeper.CLUSTER_STATE = '/collections/'+ collectionName +'/state.json'
 
    # Twitter api keys
    ckey="[]"
    csecret="[]"
    atoken="[]"
    asecret="[]"

    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)

    twitterStream = Stream(auth, listener(zooKeeperHost, collectionName, n))
    twitterStream.filter(track=[f])


if __name__ == "__main__":
    main(sys.argv[1:])
