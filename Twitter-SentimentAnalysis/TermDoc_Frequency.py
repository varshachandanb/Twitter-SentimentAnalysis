import MapReduce
import sys
import re
import string

id=0
mr = MapReduce.MapReduce()

def mapper(each_tweet):
    # Mapper code goes in here
    #new_tweet=each_tweet.encode('utf-8').translate(None, string.punctuation)
    global id
    for key in each_tweet:
        if ("text" in key):
            id+=1
            val=each_tweet["text"]
            newstring1 = re.sub(r"http\S+", "", val)
            newstring2=re.sub(r"(RT|retweet)", "", newstring1)
            newstring3=re.sub(r'@\S+', "", newstring2)
            newstring4=re.sub(r"#\S+", "", newstring3)
            #newstring5=re.sub(r"[^a-zA-Z\d\s]", "", newstring4)
            newstring = newstring4.encode('utf-8').translate(None, string.punctuation)
            wr=newstring.split()
            for word in wr:
                #print word.lower(),id
                mr.emit_intermediate(word.lower(),id)



def reducer(key, list_of_values):
    #reducer code goes in here
    global id
    tf_count=0
    df_count=0
    id_count=1
    output=[]
    final_list=[]
    final_term_list=[]
    term_list=[]

    for val in range(1,id+1):
        #count of words with same tweet id= tf
        lst=list_of_values.count(val)
        if(lst>0):
            df_count+=1
            term_list.append([val,lst])
    mr.emit([key,df_count,term_list])



if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

