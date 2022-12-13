import re
import csv
import time
from pyspark.sql import SparkSession

# path to file, from witch we creating index
path = r'/home/data/footballers_matches.xml'

#*** extracting words from page
def extract_words(page):
    fromTags=re.findall(r"(?<=>)\n?[^<>\n]*(?=<)",page) # we take words only which are inside tags
    text=" ".join(fromTags).replace("\n", "") 
    words=text.split()
    unique_words = set(words)   # making unique list of words
    return unique_words

#*** inserting dictionary of indexes into file
def insert_into_index(file,posting_list):
    with open(file, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        for key, value in posting_list.items():
            csv_writer.writerow([key, value])
    csv_file.close()

#*** extracting and adding unique words from page into dictionary
def mapper(page):
    unique_words=extract_words(page[1]) # extracting unique words from page
    title=re.findall(r"(?<=<title>).+?(?=<)",page[1])
    if ("Squads" in title[0]) & (len(re.findall( r"<h2",page[1]))!=0):      # checking into which index the page would be insert
        return [{key:{page[0]} for key in unique_words},{}]     #into footballers
    elif "Results of the matches in the season" in title[0] :
        return [{},{key:{page[0]} for key in unique_words}]     #into matches
    else:
        return [{},{}]      # or wouldn't be insert anywhere


#*** combining dictionaries of index into one dictionary
def reducer(x, y):
    for i in range(2):
        for key,value in y[i].items():  #comparing each index from each dictionary
            if key in x[i].keys():
                x[i][key].update(value)
            else:
                x[i][key]=value
    return x

#*** printing time since the search was started
def print_time(start):
    print(" -> "+ str(round(time.time() - start, 2))+" s")

#*** printing the information about status of searching
def information_print(information):
    print("         - "+information, end=" ")
    print_time(start)

#*** main function
if __name__ == '__main__':
    start = time.time()
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()      #inicialization and setup of spark
    spark.sparkContext.setLogLevel('ERROR')
    partitions = 16
    print("Progress:")
    information_print("1|5 Opening footballers_matches.xml for reading")
    with open(path, 'r', encoding='utf8') as file:      #opening footballers_matches.xml for reading
        data = file.read()
    file.close()
    counter=0
    information_print("2|5 Dividing footballers_matches.xml into pages")
    pages = re.findall(r"<html[\S \s]+?</html>", data) # Dividing file footballers_matches.xml into html pages
    del data
    information_print("3|5 Indexing words from footballers_matches.xml")
    posting_lists = spark.sparkContext.parallelize(enumerate(pages), partitions).map(mapper).reduce(reducer)    #creating dictionaries for footballers and matches indexes 

    information_print("4|5 Inserting footballers indexis into index_footballers.csv")
    insert_into_index('/home/data/index_footballers.csv',posting_lists[0])
    
    information_print("5|5 Inserting matches indexis into index_matches.csv")
    insert_into_index('/home/data/index_matches.csv',posting_lists[1])
    end = time.time()
    print("\nTotal count of words in index_footballers.csv: "+ str(len(posting_lists[0])))  #printing summary
    print("Total count of words im index_matches.csv: "+ str(len(posting_lists[1])))
    print("Total time: "+ str(round(time.time() - start, 2))+ " s")