import sys
import string
import re
import MapReduce

mr = MapReduce.MapReduce()
scores = {}
text=[]
id=0

def mapper(each_tweet):
    # Mapper code goes in here
    tweet_score=0
    tweets=[]
    global id
    for key in each_tweet:
        if ("text" in key):
            id+=1
            val=each_tweet["text"]
            newstring1 = re.sub(r"http\S+", "", val)
            newstring2=re.sub(r"(RT|retweet)", "", newstring1)
            #(?:\b\W*@(\w+))+
            newstring3=re.sub(r'@\S+', "", newstring2)
            newstring4=re.sub(r"#\S+", "", newstring3)
            #newstring5=re.sub(r"[^a-zA-Z\d\s]", "", newstring4)
            newstring = newstring4.encode('utf-8').translate(None, string.punctuation)
            wr=newstring.split()
            for i in range(0,len(wr)):
                if(wr[i].lower() in scores):
                    #print "Words and its count %s %f \n" %(wr[i].lower(),float(scores[wr[i].lower()]))
                    mr.emit_intermediate(id,float(scores[wr[i].lower()]))
                else:
                    mr.emit_intermediate(id,0)





def reducer(key, list_of_values):
    # Reducer code goes in here
    sent_list=[]
    val=0
    for i in list_of_values:
        #print i
        val+=i
    mr.emit([key,val])


if __name__ == '__main__':
    afinnfile = open(sys.argv[1])       # Make dictionary out of AFINN_111.txt file.
    for line in afinnfile:
        term, score  = line.split("\t")  # The file is tab-delimited. #\t means the tab character.
        scores[term] = int(score)  # Convert the score to an integer.
    tweet_data = open(sys.argv[2])
    mr.execute(tweet_data, mapper, reducer)



