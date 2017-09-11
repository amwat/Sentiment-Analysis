from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")

    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    
    posplots = [x[0][1] for x in counts if x]
    negplots = [x[1][1] for x in counts if x]
    
    plt.plot(posplots,'-o',label='positive')
    plt.plot(negplots,'-o',label='negative')
    plt.axis([-1,12,0,300])
    plt.legend(loc='upper left')
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.show()

def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    return open(filename).read().strip().split('\n')

def seperator(word,pWords,nWords):
    if word in pWords:
        return ("positive",1)
    elif word in nWords:
        return ("negative",1)
    return

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    
    #tweets.flatMap(lambda x:x.split(" ")).pprint()
    table = tweets.flatMap(lambda x:x.lower().split(" ")).map(lambda x:seperator(x,pwords,nwords)).filter(lambda x:x is not None).reduceByKey(lambda x,y:x+y)
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    totalcount = table.updateStateByKey(lambda x,y: x[0] + (y or 0))
    
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    table.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    print totalcount.pprint()

    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    
    return counts


if __name__=="__main__":
    main()
