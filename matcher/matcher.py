import re
import csv
import jellyfish

def setup_csv_reader():
    import sys
    maxInt = sys.maxsize

    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)

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

def compare_strings(string1,string2):
    accuracy=0.85
    strings1=string1.lower().split()
    strings2=string2.lower().split()
    for s1 in strings1:
        match=False
        for s2 in strings2:
            if (jellyfish.jaro_distance(s1,s2)>accuracy):
                match=True
                break
        if not match:    
            return False
    return True
    
def get_occurrences(index,inputs,exactly):
    posting_list=read_csv(index)
    if exactly:
        occurrences = dict(filter(lambda item: any(part == item[0] for part in inputs), posting_list.items()))
    else:   
        occurrences = dict(filter(lambda item: any((compare_strings(item[0],part)) for part in inputs), posting_list.items()))
    
    INPUT_occurrences={}
    for key,value in occurrences.items():
        occurre_pages = value[1:][:-1].split(", ")
        if exactly:
            for i in inputs:
                if(i == key):
                    if i in INPUT_occurrences.keys():
                        INPUT_occurrences[i]=INPUT_occurrences[i]+occurre_pages
                    else:
                        INPUT_occurrences[i]=occurre_pages
                    break
        else:
            for i in inputs:
                if(compare_strings(i,key)):
                    if i in INPUT_occurrences.keys():
                        INPUT_occurrences[i]=INPUT_occurrences[i]+occurre_pages
                    else:
                        INPUT_occurrences[i]=occurre_pages
    del posting_list, occurrences
    return INPUT_occurrences

def get_AND_occurrences(INPUT_occurrences,inputs):
    final_occurrences=INPUT_occurrences[inputs[0]]
    for i in range(len(inputs)-1):
        current_occurrences=[]
        for occur in final_occurrences:
            if (inputs[i+1]) in INPUT_occurrences:
                if occur in INPUT_occurrences[inputs[i+1]]:
                    current_occurrences.append(occur)
        final_occurrences=current_occurrences
    final_occurrences = sorted(list(dict.fromkeys(final_occurrences)))
    return final_occurrences

def extract_manager_footballer(page):
    manager=re.findall(r"(?<=Manager:)[\S \s]+?(?=</h3>)",page)
    if len(manager) !=0:
        manager= re.sub(r"\n|&#.*?;|</b>", "",manager[0]).strip()
    else:
        manager="not found" 
    return manager

def transfer_to_cm(height):
    e_height = height.replace("\"", "").split("\'")
    foot = e_height[0].replace("\'", "")
    inch= e_height[1]
    try:
        return str(round((((int(foot)*12+int(inch))*2.54)/100),2))
    except:
        return "couldn't convert"
    


def extract_specs_footballer(player):
    specs= list(filter(None, re.findall(r"(?<=>)[^<>]*?(?=</)",player)))
    if len(specs)>5:
        i=3
        if specs[i] not in {"G","F","D","M"}:
            i=i-1
        pos=specs[i]
        height=specs[i+1]
        try:
            weight=int(specs[i+2].replace(" ", "")) 
            if  weight > 150:
                weight=str(round(weight*0.0625))
            else:
                weight=str(weight)
        except:
            weight=specs[i+2]

        if "\'" in height:
            height=transfer_to_cm(height)
        if "\'" in weight:
            w=weight
            weight=height
            height=transfer_to_cm(w)
       

    else:
        return "","",""
      
    return pos, height, weight

def extract_team_season_footballer(page):
    title=re.findall(r"(?<=<title>FootballSquads - ).+?(?=<)", page)[0]
    array_title = re.sub(r"\n|&#.*?;|</b>", "",title).strip().split(" - ",1)
    team= ' '.join(word for word in array_title[0].split() if len(word)>2)
    
    season = array_title[1]
    season = re.findall(r"\d+", season)
    if len(season) == 2:
        s1=season[0]
        s2=season[1]
        if len(s2)!=4:
            s2=s1[0]+s1[1]+s2
        season=s1+"-"+s2
    else:
        season=season[0]
    return team,season

def extract_data_footballer(results, for_matches_season, for_matches_team,player,check_name,page):
    
    manager=extract_manager_footballer(page)
    
    pos, height, weight=extract_specs_footballer(player)
    
    team, season=extract_team_season_footballer(page)
    
    result =team+" -&- "+season+" -&- "+manager+" -&- "+pos+" -&- "+height+" -&- "+weight
    result= "         - "+" ".join(result.replace("\n", "").split())
    result = re.sub(r'<.*?>', '', result)

    name= re.sub(r"\n|&#.*?;|</b>", "",check_name).strip()
    name = re.sub(r'<.*?>', '', name)
    if name in results.keys():
        results[name].append(result)
    else:
        results[name]=[result]

    for_matches_team.append(team)
    for_matches_season.append(season)


def get_footballers(final_occurrences,pages,input):
    results={}
    for_matches_season=[]
    for_matches_team=[]
    
    for occurre_page in final_occurrences:
        page=pages[int(occurre_page)]
        control = re.findall( r"<h2",page)
        players= re.findall( r"(?<=<tr>)[\S \s]*?(?=</tr)",page)
        player=""
        for player in players: 
            check_name = re.findall(r"(?<=>)[^<>]*?(?=</)",player)
            if len(check_name)>1:
                check_name=check_name[1]
                if (len(control) !=0) & (compare_strings(input,check_name.lower())): 
                    extract_data_footballer(results, for_matches_season, for_matches_team,player,check_name,page)   
    return results, for_matches_season, for_matches_team

def extract_team_occurrences(uniques,INPUT_occurrences_team):
    occurrences_team={}
    for unique in uniques:
        # print(unique)
        match=[]
        for key,value in INPUT_occurrences_team.items():
            if key in unique:
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
    return occurrences_team


def get_team_season_occurrences(for_matches_team,for_matches_season):
    unique_f_m_season=list(set(for_matches_season))

    uniques=list(set(for_matches_team))
    unique_f_m_team=[]
    for unique in uniques:
        unique_f_m_team=unique_f_m_team+unique.split()

    occurrences_season = get_occurrences("index_matches.csv",unique_f_m_season,True)
    INPUT_occurrences_team = get_occurrences("index_matches.csv",unique_f_m_team,False)

    del unique_f_m_season, unique_f_m_team

    occurrences_team=extract_team_occurrences(uniques,INPUT_occurrences_team)
    return occurrences_team,occurrences_season

def get_matches(for_matches_season,for_matches_team):
    
    occurrences_team,occurrences_season=get_team_season_occurrences(for_matches_team,for_matches_season)

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
    return occurrences_matches

def extract_max_match(key,pages,matches,occurrences_matches,season,team):
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
    return max_match,max_goals
        

def print_seasion(value,occurrences_matches,pages):
    team,season = extract_team_years(value)
    key = season +"-&-" +team
    matches=[]
    
    max_match,max_goals=extract_max_match(key,pages,matches,occurrences_matches,season,team)

    if len(matches)!=0:
        value+=" -&- "+str(len(matches)) 
    else: 
        value+=" -&- no match"
    print(value)
    print("             + most goals match: "+max_match+"   +Σ: "+ str(max_goals))
    print("         -----------------------------------------------------------------------------------------------")

def print_results(reorder_results,occurrences_matches,pages):
    for key,values in reorder_results.items():
        print("\n*******************************************************************************************************")
        print(key+":")
        values.sort(key=extract_years)
        for value in values:
            print_seasion(value,occurrences_matches,pages)

if __name__ == '__main__': 
    # print(jellyfish.jaro_distance(u'united', u'utd'))
    input = input()
    setup_csv_reader()
    path = r'C:\Users\Dubak\Desktop\7.semester\VINF\projekt\data\footballers_matches.xml'

    with open(path, 'r') as file:
        data = file.read()
    file.close()

    
    # input = "Messi lionel"
    inputs= input.split()

    INPUT_occurrences = get_occurrences("index_footballers.csv",inputs,False)
    
    # print(INPUT_occurrences)
    if len(INPUT_occurrences)!=0:
        
        final_occurrences_footballers= get_AND_occurrences(INPUT_occurrences,inputs)
        del INPUT_occurrences
        pages = re.findall(r"<html[\S \s]+?</html>", data)

        results_footballers, for_matches_season, for_matches_team=get_footballers(final_occurrences_footballers,pages,input)
        del final_occurrences_footballers
        
        if len(results_footballers)!=0:
            sorted_results = sorted(list(results_footballers.items()), key = lambda key : len(key[0]))
            # reordering to dictionary
            reorder_results_footballers = {ele[0] : ele[1]  for ele in sorted_results}
            del results_footballers,sorted_results

            occurrences_matches=get_matches(for_matches_season,for_matches_team)
            del for_matches_season,for_matches_team
            
            print_results(reorder_results_footballers,occurrences_matches,pages)
        
        else:
            print("             - NO MATCH FOR THIS FOOTBALLER")
    else:
        print("             - NO MATCH FOR THIS FOOTBALLER")