import re
import csv
# footballers_mini
path = r'C:\Users\Dubak\Desktop\7.semester\VINF\projekt\data\footballers.xml'

with open(path, 'r') as file:
    data = file.read()
file.close()

counter=0
pages = re.findall(r"<html[\S \s]+?</html>", data)
posting_list={}
for page in pages:
    fromTags=re.findall(r"(?<=>)\n?[^<>\n]*(?=<)",page)
    text=" ".join(fromTags)
    words=text.split() 
    unique_words = set(words)
    for unique_word in unique_words:
        if unique_word in posting_list.keys():
            posting_list[unique_word].add(counter)
        else:
            posting_list[unique_word]={counter}
    counter+=1

with open('index.csv', 'w', newline='', encoding='utf-8') as csv_file:
    csv_writer = csv.writer(csv_file)
    for key, value in posting_list.items():
        csv_writer.writerow([key, value])
csv_file.close()



# (?<=>)\n? {0}[^ \n][^<>\n]*(?=<)