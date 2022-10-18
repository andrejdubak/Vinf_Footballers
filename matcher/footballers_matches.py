import re
import csv
from time import sleep

path = r'C:\Users\Dubak\Desktop\7.semester\VINF\projekt\data\footballers_mini.xml'

with open(path, 'r') as file:
    data = file.read()

file.close()
#input = input()
input = "osa"

posting_list={}
with open('index.csv', 'r') as csv_file:
    reader = csv.reader(csv_file)
    for row in reader:
        posting_list[row[0]]=row[1]
csv_file.close()

occurrences = dict(filter(lambda item: input in item[0], posting_list.items()))

pages = re.findall(r"<html[\S \s]+?</html>", data)
for key,value in occurrences.items():
    occurre_pages = value[1:][:-1].split(", ")
    print("************  "+key+":")
    for occurre_page in occurre_pages:
        page=pages[int(occurre_page)]
        control = re.findall(input+"?", page)
        if len(control) !=0:
            title=re.findall(r"(?<=<title>FootballSquads - ).+?(?=<)", page)[0]
            manager=re.findall(r"(?<=Manager:)[\S \s]+?(?=</h3>)",page)
            if len(manager) !=0:
                manager= re.sub(r"\n|&#.*?;|</b>", "",manager[0]).strip()
            else:
                manager="not found"
            print("- "+title+" - "+manager)