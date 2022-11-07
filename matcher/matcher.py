import re
import csv
import numpy as np

def extract_years(value):
    return re.findall("\d+", value.split(" -&- ")[1])[0]
def extract_team_years(value):
    array = value.split(" -&- ")
    return array[0].replace("         - ", ""),array[1]

def read_csv(index):
    posting_list={}
    with open(index, 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            posting_list[row[0]]=row[1]
    csv_file.close()
    return posting_list
    
def get_occurrences(index,inputs,exactly):
    posting_list=read_csv(index)
    if exactly:
        occurrences = dict(filter(lambda item: any(part == item[0] for part in inputs), posting_list.items()))
    else:   
        occurrences = dict(filter(lambda item: any(part in item[0] for part in inputs), posting_list.items()))
    AND_occurrences={}
    for key,value in occurrences.items():
        occurre_pages = value[1:][:-1].split(", ")
        if exactly:
            for i in inputs:
                if(i == key):
                    if i in AND_occurrences.keys():
                        AND_occurrences[i]=AND_occurrences[i]+occurre_pages
                    else:
                        AND_occurrences[i]=occurre_pages
                    break
        else:
            for i in inputs:
                if(i in key):
                    if i in AND_occurrences.keys():
                        AND_occurrences[i]=AND_occurrences[i]+occurre_pages
                    else:
                        AND_occurrences[i]=occurre_pages
    del posting_list, occurrences
    return AND_occurrences


path = r'C:\Users\Dubak\Desktop\7.semester\VINF\projekt\data\footballers_matches.xml'

with open(path, 'r') as file:
    data = file.read()
file.close()
#input = input()
input = "Lionel Messi"
inputs= input.split()

import sys
maxInt = sys.maxsize

while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)



AND_occurrences = get_occurrences("index_footballers.csv",inputs,False)

if len(AND_occurrences)!=0:
    final_occurrences=AND_occurrences[inputs[0]]
    for i in range(len(inputs)-1):
        current_occurrences=[]
        for occur in final_occurrences:
            if occur in AND_occurrences[inputs[i+1]]:
                current_occurrences.append(occur)
        final_occurrences=current_occurrences

    del AND_occurrences

    final_occurrences = sorted(list(dict.fromkeys(final_occurrences)))
    results={}
    for_matches_season=[]
    for_matches_team=[]
    pages = re.findall(r"<html[\S \s]+?</html>", data)

    for occurre_page in final_occurrences:
        page=pages[int(occurre_page)]
        control = re.findall( r"<h2",page)
        players= re.findall( r"(?<=<tr>)[\S \s]*?(?=</tr)",page)
        player=""
        for p in players:
            if input in p: 
                player=p
                check_name = re.findall(r"(?<=>)[\S ]*?"+input+r"[\S ]*?(?=</td>)",page)
                if (len(control) !=0) & (len(check_name)!=0): 
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
                    team= ' '.join(word for word in array_title[0].split() if len(word)>2)
                    season = array_title[1]
                    manager=re.findall(r"(?<=Manager:)[\S \s]+?(?=</h3>)",page)
                    if len(manager) !=0:
                        manager= re.sub(r"\n|&#.*?;|</b>", "",manager[0]).strip()
                    else:
                        manager="not found"
                    #result = "         - "+team+" - "+season+" - "+manager+" - "+pos+" - "+height+" - "+weight
                    season = re.findall(r"\d+", season)
                    if len(season) == 2:
                        s1=season[0]
                        s2=season[1]
                        if len(s2)!=4:
                            s2=s1[0]+s1[1]+s2
                        season=s1+"-"+s2
                    else:
                        season=season[0]
                    
                    result = "         - "+team+" -&- "+season+" -&- "+manager+" -&- "+pos+" -&- "+height+" -&- "+weight
                    result= result.replace("\n", "")
                    if name in results.keys():
                        results[name].append(result)
                    else:
                        results[name]=[result]
                    #print("- "+name+" - "+team+" - "+season+" - "+manager)
                    # for_matches_team.append(' '.join(word for word in team.split() if len(word)>2))
                    for_matches_team.append(team)
                    for_matches_season.append(season)
    del final_occurrences

    sorted_results = sorted(list(results.items()), key = lambda key : len(key[0]))
    # reordering to dictionary
    reorder_results = {ele[0] : ele[1]  for ele in sorted_results}

    unique_f_m_season=list(set(for_matches_season))

    uniques=list(set(for_matches_team))
    unique_f_m_team=[]
    for unique in uniques:
        unique_f_m_team=unique_f_m_team+unique.split()

    occurrences_season = get_occurrences("index_matches.csv",unique_f_m_season,True)
    AND_occurrences_team = get_occurrences("index_matches.csv",unique_f_m_team,False)


    # print(AND_occurrences_team)
    occurrences_team={}
    for unique in uniques:
        # print(unique)
        match=[]
        for key,value in AND_occurrences_team.items():
            if key in unique:
                # print(key)
                # print(value)
                match.append(value)
        if len(match)!=0:
            occurrences_team[unique]=match[0]
            for i in range(len(match)-1):
                current_occurrences=[]
                for occur in occurrences_team[unique]:
                    if occur in match[i+1]:
                        current_occurrences.append(occur)
                occurrences_team[unique]=current_occurrences
        del match
        # print( occurrences_team[unique])
    # print(occurrences_season)
    # print(occurrences_team)
    occurrences_matches={}
    for i in range(len(for_matches_season)):
        season=for_matches_season[i]
        team=for_matches_team[i]
        
        if (season in occurrences_season.keys()) & (team in occurrences_team.keys()):
            key = season +"-&-" +team
            occurrences_matches[key]=[]
            for occur in occurrences_season[season]:
                if occur in occurrences_team[team]:
                    occurrences_matches[key].append(occur)

    del for_matches_season,for_matches_team

    for key,values in reorder_results.items():
        print("\n*******************************************************************************************************")
        print(key+":")
        values.sort(key=extract_years)
        for value in values:
            team,season = extract_team_years(value)
            key = season +"-&-" +team
            matches=[]
            max_match=""
            max_goals=0
            
            if (key in occurrences_matches.keys()):
                for occurre_page in occurrences_matches[key]:
                    page=pages[int(occurre_page)]
                    title=re.findall(r"(?<=<title>).+?(?=<)", page)[0]
                    if season in title:
                        all_matches=re.findall(r"<tr[\S \s]+?/tr>", page)
                        for raw_match in all_matches:
                            if team in raw_match:
                                fromTags=re.findall(r"(?<=>)\n?[^<>\n]*(?=<)",raw_match)
                                match_sep=' '.join(fromTags).replace("&#160;", "").split()
                                result= match_sep[len(match_sep)-1]
                                match=' '.join(match_sep)
                                if ":" in result:  
                                    final_scores = result.split('(')[0].split(":")
                                    goals=int(final_scores[0])+int(final_scores[1])
                                    if goals>=max_goals:
                                        max_match=match
                                        max_goals=goals
                                # print(match+"\n")
                                matches.append(match)
                        del all_matches
            if len(matches)!=0:
                value+=" -&- "+str(len(matches)) 
            else: 
                value+=" -&- no match"
            print(value)
            print("             + most goals match: "+max_match)
else:
    print("             - NO MATCH FOR THIS FOOTBALLER")
        # break
    # print(for_matches_season[i])
    # print(for_matches_team[i])