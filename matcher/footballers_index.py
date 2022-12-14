import re
import csv
import time

# path to file, from witch we creating index
path = r'C:\Users\Dubak\Desktop\7.semester\VINF\projekt\data\footballers_matches.xml'
# path = r'/home/data/footballers_matches.xml'

def extract_words(page):
    """ 
        Extracting words from page
        :param page: webpage html
        :returns: set of unique words from tags from page
    """

    fromTags=re.findall(r"(?<=>)\n?[^<>\n]*(?=<)",page) # we take words only which are inside tags
    text=" ".join(fromTags).replace("\n", "") 
    words=text.split()
    unique_words = set(words)   # making unique list of words
    return unique_words

def insert_into_posting(posting_list,page,counter):
    """ 
        Inserting dictionary of indexes into posting dicitonary
        :param posting_list: dictionary into which the words going to be inserted
        :param page: webpage html
        :param counter: number of page
        :returns: dictionary of occurancces of words in file
    """

    unique_words=extract_words(page)    # dividing page into unigue words
    for unique_word in unique_words:    # each word is insert into posting list
        if unique_word in posting_list.keys():
            posting_list[unique_word].add(counter)
        else:
            posting_list[unique_word]={counter}
    return posting_list


def insert_into_index(file,posting_list):
    """ 
        Inserting dictionary of indexes into file
        :param file: path to file, into which the posting list going to be inserted
        :param posting_list: dictionary of words and number of pages, where they are located
    """

    with open(file, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        for key, value in posting_list.items():
            csv_writer.writerow([key, value])
    csv_file.close()
    
def print_time(start):
    """ 
        Printing time since the search was started
        :param start: time of the start of program
    """

    print(" -> "+ str(round(time.time() - start, 2))+" s")

def information_print(information):
    """ 
        Printing the information about status of searching
        :param information: information, which going to be printed
    """

    print("         - "+information, end=" ")
    print_time(start)


if __name__ == '__main__':
    """ 
        Main function
    """

    print("Progress:")
    start = time.time()
    information_print("1|5 Opening footballers_matches.xml for reading")
    with open(path, 'r') as file:
        data = file.read()
    file.close()

    counter=0
    information_print("2|5 Dividing footballers_matches.xml into pages")
    pages = re.findall(r"<html[\S \s]+?</html>", data) # Dividing file footballers_matches.xml into html pages
    posting_list_f={}
    posting_list_m={}
    information_print("3|5 Indexing words from footballers_matches.xml")
    for page in pages:      # Browsing single pages independently
        title=re.findall(r"(?<=<title>).+?(?=<)",page)
        if ("Squads" in title[0]) & (len(re.findall( r"<h2",page))!=0):     # checking into which index the page would be insert
            posting_list_f=insert_into_posting(posting_list_f,page,counter)  # inserting webpage into posting_list_f
        elif "Results of the matches in the season" in title[0] :
            posting_list_m=insert_into_posting(posting_list_m,page,counter)     # inserting webpage into posting_list_m
        counter+=1
    information_print("4|5 Inserting footballers indexis into index_footballers.csv")
    insert_into_index('index_footballers.csv',posting_list_f)   # inserting footballers dictionary into index_footballers.csv file
    information_print("5|5 Inserting matches indexis into index_matches.csv")
    insert_into_index('index_matches.csv',posting_list_m)   # inserting matches dictionary into index_matches.csv file
    end = time.time()
    print("\nTotal count of words in index_footballers.csv: "+ str(len(posting_list_f)))  #printing summary
    print("Total count of words im index_matches.csv: "+ str(len(posting_list_m)))
    print("Total time: "+ str(round(time.time() - start, 2))+ " s")
