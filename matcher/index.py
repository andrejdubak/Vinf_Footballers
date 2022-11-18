import re
import csv
# footballers_mini footballers
path = r'C:\Users\Dubak\Desktop\7.semester\VINF\projekt\data\footballers_matches.xml'

def extract_words(page):
    fromTags=re.findall(r"(?<=>)\n?[^<>\n]*(?=<)",page)
    text=" ".join(fromTags).replace("\n", "") 
    words=text.split()
    unique_words = set(words)
    return unique_words

def insert_into_posting(posting_list,page,counter):
    unique_words=extract_words(page)
    for unique_word in unique_words:
        if unique_word in posting_list.keys():
            posting_list[unique_word].add(counter)
        else:
            posting_list[unique_word]={counter}
    return posting_list

def insert_into_index(file,posting_list):
    with open(file, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        for key, value in posting_list.items():
            csv_writer.writerow([key, value])
    csv_file.close()
    
if __name__ == '__main__':
    with open(path, 'r') as file:
        data = file.read()
    file.close()

    counter=0
    pages = re.findall(r"<html[\S \s]+?</html>", data)
    # print(len(pages))
    posting_list_f={}
    posting_list_m={}
    for page in pages:
        title=re.findall(r"(?<=<title>).+?(?=<)",page)
        if ("Squads" in title[0]) & (len(re.findall( r"<h2",page))!=0):
            posting_list_f=insert_into_posting(posting_list_f,page,counter)
        elif "Results of the matches in the season" in title[0] :
            posting_list_m=insert_into_posting(posting_list_m,page,counter)
        counter+=1

    insert_into_index('index_footballers.csv',posting_list_f)
    insert_into_index('index_matches.csv',posting_list_m)