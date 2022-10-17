import re
from time import sleep

path = r'C:\Users\Dubak\Desktop\7.semester\VINF\projekt\data\footballers_matches.xml'

with open(path, 'r') as file:
    data = file.read()

file.close()
#input = input()
input = "Neymar"

pages = re.findall(r"<html[\S \n\t]+?</html>", data)
for page in pages:
    control = re.findall(input+"?", page)
    if len(control) !=0:
        title=re.findall(r"(?<=<title>FootballSquads - ).+?(?=<)", page)[0]
        manager=re.findall(r"(?<=Manager:)[\S \t\n]+?(?=</h3>)",page)
        if len(manager) !=0:
            manager= re.sub(r"\n|&#.*?;|</b>", "",manager[0]).strip()
        else:
            manager="not found"
        print(title+" - "+manager)