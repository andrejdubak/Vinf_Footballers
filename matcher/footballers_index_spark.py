import re
import csv
import time
import sys
from pyspark.sql import SparkSession
from operator import add
from collections import defaultdict
# footballers_mini footballers
path = r'/home/data/footballers_matches.xml'

def extract_words(page):
    fromTags=re.findall(r"(?<=>)\n?[^<>\n]*(?=<)",page)
    text=" ".join(fromTags).replace("\n", "") 
    words=text.split()
    unique_words = set(words)
    return unique_words

def insert_into_index(file,posting_list):
    with open(file, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        for key, value in posting_list.items():
            csv_writer.writerow([key, value])
    csv_file.close()

def mapper(page):
    unique_words=extract_words(page[1])
    title=re.findall(r"(?<=<title>).+?(?=<)",page[1])
    # return 
    if ("Squads" in title[0]) & (len(re.findall( r"<h2",page[1]))!=0):
        return [{key:{page[0]} for key in unique_words},{}]
    elif "Results of the matches in the season" in title[0] :
        return [{},{key:{page[0]} for key in unique_words}]
    else:
        return [{},{}]

def reducer(x, y):
    for i in range(2):
        for key,value in y[i].items():
            if key in x[i].keys():
                x[i][key].update(value)
            else:
                x[i][key]=value
    return x

def print_time(start):
    print(" -> "+ str(round(time.time() - start, 2))+" s")

def information_print(information):
    print("         - "+information, end=" ")
    print_time(start)

if __name__ == '__main__':
    start = time.time()
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    partitions = 16
    print("Progress:")
    information_print("1|5 Opening footballers_matches.xml for reading")
    with open(path, 'r', encoding='utf8') as file:
        data = file.read()
    file.close()
    counter=0
    information_print("2|5 Dividing footballers_matches.xml into pages")
    pages = re.findall(r"<html[\S \s]+?</html>", data)
    del data
    information_print("3|5 Indexing words from footballers_matches.xml")
    posting_lists = spark.sparkContext.parallelize(enumerate(pages), partitions).map(mapper).reduce(reducer)

    information_print("4|5 Inserting footballers indexis into index_footballers.csv")
    insert_into_index('/home/data/index_footballers.csv',posting_lists[0])
    
    information_print("5|5 Inserting matches indexis into index_matches.csv")
    insert_into_index('/home/data/index_matches.csv',posting_lists[1])
    end = time.time()
    print("\nTotal count of words in index_footballers.csv: "+ str(len(posting_lists[0])))
    print("Total count of words im index_matches.csv: "+ str(len(posting_lists[1])))
    print("Total time: "+ str(round(time.time() - start, 2))+ " s")