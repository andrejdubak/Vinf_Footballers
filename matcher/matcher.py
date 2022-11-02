import re
import csv
import numpy as np
from time import sleep

def extract_years(value):
    return re.findall("\d+", value.split(" -&- ")[1])[0]
def extract_team_years(value):
    array = value.split(" -&- ")
    return array[0].replace("         - ", ""),re.findall("\d+", array[1])[0]


path = r'C:\Users\Dubak\Desktop\7.semester\VINF\projekt\data\footballers.xml'

with open(path, 'r') as file:
    data = file.read()

file.close()
#input = input()
input = "Cristiano Ronaldo"

import sys
maxInt = sys.maxsize

while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)


posting_list={}
with open('index.csv', 'r') as csv_file:
    reader = csv.reader(csv_file)
    for row in reader:
        posting_list[row[0]]=row[1]
csv_file.close()

# np.isin(inputs, X)

inputs= input.split()
# any(part in item[0] for part in inputs)
# input in item[0]
occurrences = dict(filter(lambda item: any(part in item[0] for part in inputs), posting_list.items()))
# print(occurrences)
results={}

pages = re.findall(r"<html[\S \s]+?</html>", data)
for key,value in occurrences.items():
    occurre_pages = value[1:][:-1].split(", ")
    #print("************  "+key+":")
    for occurre_page in occurre_pages:
        page=pages[int(occurre_page)]
        control = re.findall( r"<h2",page)
        players= re.findall( r"(?<=<tr>)[\S \s]*?(?=</tr)",page)
        player=""
        for p in players:
            if key in p: 
                player=p
                break

        check_name = re.findall(r"(?<=>)[\S ]*?"+input+r"[\S ]*?(?=</td>)",page)
        if (len(control) !=0) & (player!="") & (len(check_name)!=0): 
            specs= re.findall(r"(?<=>)[^<>]*?(?=</)",player)
            name= re.sub(r"\n|&#.*?;|</b>", "",check_name[0]).strip()
            i=3
            if specs[i] not in {"G","F","D","M"}:
                i=i-1
            pos=specs[i]
            height=specs[i+1]
            weight=specs[i+2]
            title=re.findall(r"(?<=<title>FootballSquads - ).+?(?=<)", page)[0]
            array_title = re.sub(r"\n|&#.*?;|</b>", "",title).strip().split(" - ",1)
            team= array_title[0]
            season = array_title[1]
            manager=re.findall(r"(?<=Manager:)[\S \s]+?(?=</h3>)",page)
            if len(manager) !=0:
                manager= re.sub(r"\n|&#.*?;|</b>", "",manager[0]).strip()
            else:
                manager="not found"
            #result = "         - "+team+" - "+season+" - "+manager+" - "+pos+" - "+height+" - "+weight
            result = "         - "+team+" -&- "+season+" -&- "+manager+" -&- "+pos+" -&- "+height+" -&- "+weight
            result= result.replace("\n", "")
            if name in results.keys():
                results[name].append(result)
            else:
                results[name]=[result]
            #print("- "+name+" - "+team+" - "+season+" - "+manager)

sorted_results = sorted(list(results.items()), key = lambda key : len(key[0]))
# reordering to dictionary
reorder_results = {ele[0] : ele[1]  for ele in sorted_results}

for key,values in reorder_results.items():
    print("\n*******************************************************************************************************")
    print(key+":")
    values.sort(key=extract_years)
    for value in values:
        print(value)
        team,year = extract_team_years(value)
        # print(team+" "+year)