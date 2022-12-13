import re
import csv
import sys
import time
from pyspark.sql import SparkSession
import jellyfish

spark = SparkSession.builder.appName("PythonPi").getOrCreate() #inicialization and setup of spark
spark.sparkContext.setLogLevel('ERROR')
partitions = 16
accuracy=0.85 #default accuracy of serch results

#*** prepare csv reader to by able to process big files
def setup_csv_reader():
    maxInt = sys.maxsize
    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)

#*** extracting only year value from string
def extract_years(value):
    return re.findall("\d+", value.split(" -&- ")[1])[0]

#*** extracting team and year from string
def extract_team_years(value):
    array = value.split(" -&- ")
    return array[0].replace("         - ", ""),array[1]

#*** reading footballers_matches.xml into posting list dictionary
def read_csv(index):
    posting_list={}
    with open(index, 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            posting_list[row[0]]=row[1]
    csv_file.close()
    return posting_list

#*** comparing two strings
def compare_strings(string1,string2):
    strings1=string1.lower().split()    #lower case divided into words
    strings2=string2.lower().split()
    for s1 in strings1:
        match=False
        for s2 in strings2:
            if (jellyfish.jaro_distance(s1,s2)>accuracy):      #using jaro distance algoritm to determine the degree of similarity
                match=True
                break
        if not match:    
            return False
    return True
    
#*** from index file extracting pages on which searching inputs are located 
def get_occurrences(index,inputs,exactly):
    posting_list=read_csv(index)
    if exactly: # matching only the same words
        occurrences = dict(filter(lambda item: any(part == item[0] for part in inputs), posting_list.items()))
    else:   # matching words which are similar enough according to jaro distance algorithm
        occurrences = dict(filter(lambda item: any((compare_strings(item[0],part)) for part in inputs), posting_list.items()))
    
    INPUT_occurrences={}
    for key,value in occurrences.items():   # for each word found in the index, adding it into dictionary
        occurre_pages = value[1:][:-1].split(", ")  # extracting pages on which the word is found
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
    return INPUT_occurrences    #the result is a dictionary containing inputs and pages on which they or similar ones to them are located

#*** reducing dictionary by AND rule
def get_AND_occurrences(INPUT_occurrences,inputs):
    final_occurrences=INPUT_occurrences[inputs[0]] 
    for i in range(len(inputs)-1):  # for each word in the INPUT_occurrences it is checked
        current_occurrences=[]
        for occur in final_occurrences:
            if (inputs[i+1]) in INPUT_occurrences:
                if occur in INPUT_occurrences[inputs[i+1]]:     # if the occurrence pages of the one word are intersecting with occurrence pages of another word
                    current_occurrences.append(occur)
        final_occurrences=current_occurrences
    final_occurrences = sorted(list(dict.fromkeys(final_occurrences)))
    return final_occurrences    # the result is a list containing the numbers of pages on which all the words from the inputs are found

#*** extracting manager from page which was training the player
def extract_manager_footballer(page):
    manager=re.findall(r"(?<=Manager:)[\S \s]+?(?=</h3>)",page)
    if len(manager) !=0:
        manager= re.sub(r"\n|&#.*?;|</b>", "",manager[0]).strip()
    else:
        manager="not found" 
    return manager

#*** transfering heigh values from foots into cm
def transfer_to_cm(height):
    e_height = height.replace("\"", "").split("\'")
    foot = e_height[0].replace("\'", "")
    inch= e_height[1]
    try:
        return str(round((((int(foot)*12+int(inch))*2.54)/100),2))
    except:
        return "couldn't convert"


#*** extracting informations about player from the season
def extract_specs_footballer(player):
    specs= list(filter(None, re.findall(r"(?<=>)[^<>]*?(?=</)",player)))
    if len(specs)>5:   # if the record is intact 
        i=3
        if specs[i] not in {"G","F","D","M"}:
            i=i-1
        pos=specs[i]    # position of player
        height=specs[i+1]   # heright of player
        try:
            weight=int(specs[i+2].replace(" ", "")) 
            if  weight > 150:
                weight=str(round(weight*0.0625))
            else:
                weight=str(weight)
        except:
            weight=specs[i+2]       # weight of player

        if "\'" in height:
            height=transfer_to_cm(height)
        if "\'" in weight:
            w=weight
            weight=height
            height=transfer_to_cm(w)
    else:
        return "","",""
      
    return pos, height, weight

#*** extracting team and season from page
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

#*** extracting all information about team , season, manager from club at that season from page
def extract_club(page):
    manager=extract_manager_footballer(page)    
    team, season=extract_team_season_footballer(page)
    for_matches_team=[team]
    for_matches_season=[season]
    return team+" -&- "+season+" -&- "+manager+" -&- ",for_matches_season,for_matches_team

#*** extracting all information player stats from club at that season from page
def extract_data_footballer(results, for_matches_season, for_matches_team,player,check_name,club):
    pos, height, weight=extract_specs_footballer(player)
    
    result =club+pos+" -&- "+height+" -&- "+weight
    result= "         - "+" ".join(result.replace("\n", "").split())
    result = re.sub(r'<.*?>', '', result)

    name= re.sub(r"\n|&#.*?;|</b>", "",check_name).strip()
    name = re.sub(r'<.*?>', '', name)
    if name in results.keys():
        results[name].append(result)
    else:
        results[name]=[result]

#*** extracting all data about footballers from the page
def footballers_mapper(tupple_page):
    for_matches_team=[]     # extracting teams
    for_matches_season=[]   # extracting seasons
    results={}
    page=tupple_page[1]
    input=tupple_page[0]

    control = re.findall( r"<h2",page)
    players= re.findall( r"(?<=<tr>)[\S \s]*?(?=</tr)",page) # dividing page into players
    club = None
    for player in players:  # for each player
        check_name = re.findall(r"(?<=>)[^<>]*?(?=</)",player)
        if len(check_name)>1:
            check_name=check_name[1]
            if (len(control) !=0) & (compare_strings(input,check_name)):    # check if is it similar enough to serached player
                if club == None:
                    club,for_matches_season,for_matches_team = extract_club(page)       # extract information about team and season
                extract_data_footballer(results, for_matches_season, for_matches_team,player,check_name,club)   #extract data about footballer
    return (results,for_matches_season,for_matches_team)    # the result is dictionary of players and their matches, list of matches and teams
    
#*** combining dictionaries of players, list of matches and list of teams of the same player into once 
def footballers_reducer(x,y):
    for_matches_season=x[1]+y[1]
    for_matches_team=x[2]+y[2]
    results=x[0]
    for key,value in y[0].items():
        if key in results.keys():
            results[key]=results[key]+value
        else: 
            results[key]=value
    return (results,for_matches_season,for_matches_team)

#*** getting informations about players get from index
def get_footballers(final_occurrences,pages,input):
    occurre_pages=[]
    for occurre_page in final_occurrences:
        occurre_pages.append((input,pages[int(occurre_page)]))

    results_footballers = spark.sparkContext.parallelize(occurre_pages, partitions).map(footballers_mapper).reduce(footballers_reducer) # parallel extracting data about footballers
    del occurre_pages

    results=results_footballers[0]
    for_matches_season=results_footballers[1]
    for_matches_team=results_footballers[2]    

    del results_footballers
    return results, for_matches_season, for_matches_team    # the result is dictionary of footballers, list of matches and teams 

#*** reducing dictionary by AND rule
def extract_team_occurrences(uniques,INPUT_occurrences_team):
    occurrences_team={}
    for unique in uniques:  # for each word in the INPUT_occurrences it is checked
        match=[]
        for key,value in INPUT_occurrences_team.items():
            if key in unique:   # if the occurrence pages of the one word are intersecting with occurrence pages of another word
                match.append(value)
        if len(match)!=0:   # and then the result is added into existing distionary
            occurrences_team[unique]=match[0]
            for i in range(len(match)-1):
                current_occurrences=[]
                for occur in occurrences_team[unique]:
                    if occur in match[i+1]:
                        current_occurrences.append(occur)
                occurrences_team[unique]=current_occurrences
        del match
    return occurrences_team  # the result is a list containing the numbers of pages on which all the words from the uniques are found

#*** getting occurencess of teams and seasons separately
def get_team_season_occurrences(for_matches_team,for_matches_season):
    unique_f_m_season=list(set(for_matches_season)) # unique list of season

    uniques=list(set(for_matches_team))
    unique_f_m_team=[]
    for unique in uniques:      # unique list of teams
        unique_f_m_team=unique_f_m_team+unique.split()  

    occurrences_season = get_occurrences("/home/data/index_matches.csv",unique_f_m_season,True)     # getting seasons occurence pages
    INPUT_occurrences_team = get_occurrences("/home/data/index_matches.csv",unique_f_m_team,False)  # getting teams occurence pages

    del unique_f_m_season, unique_f_m_team

    occurrences_team=extract_team_occurrences(uniques,INPUT_occurrences_team)       # reducing list dictionary of tems by AND rule
    return occurrences_team,occurrences_season

#*** getting occurencess pages of matches of searched player
def get_matches_occurrences(for_matches_season,for_matches_team):
    occurrences_team,occurrences_season=get_team_season_occurrences(for_matches_team,for_matches_season)    # extracting occurencess of teams and seasons separately

    occurrences_matches={}
    for i in range(len(for_matches_season)):    # mapping the season to the team and vice versa
        season=for_matches_season[i]
        team=for_matches_team[i]
        
        if (season in occurrences_season.keys()) & (team in occurrences_team.keys()):
            key = season +"-&-" +team
            occurrences_matches[key]=[]
            for occur in occurrences_season[season]:
                if occur in occurrences_team[team]:
                    occurrences_matches[key].append(occur)
    return occurrences_matches  # the result is a list of occurencces of season, the player played

#***combining two dictionaries
def matches_reducer(x,y):
    for key,value in y.items():
        x[key]=value
    return x

#*** extracting information about season and club from web page
def matches_mapper(occurrences_match):
    key = occurrences_match[0]
    pages= occurrences_match[1]
    
    array = key.split("-&-")
    season=array[0]
    team=array[1]
    max_match=""
    max_goals=0
    count_matches=0
    for page in pages:      # for if page in occurence page
        title=re.findall(r"(?<=<title>).+?(?=<)", page)[0]
        if season in title:         # check if season is valid
            all_matches=re.findall(r"<tr[\S \s]+?/tr>", page)
            for raw_match in all_matches:    # then compare all matches
                if team in raw_match:       # in which was selected team playing
                    fromTags=re.findall(r"(?<=>)\n?[^<>\n]*(?=<)",raw_match)
                    match_sep=' '.join(fromTags).replace("&#160;", "").split()
                    result= match_sep[len(match_sep)-1]
                    match=' '.join(match_sep)
                    if ":" in result:  
                        final_scores = result.split('(')[0].split(":")
                        goals=int(final_scores[0])+int(final_scores[1])
                        if goals>=max_goals:       # and pick up the most goals match
                            max_match=match
                            max_goals=goals
                    count_matches+=1
            del all_matches
    return {key:[count_matches,max_match,max_goals]}    # the result is season and team with the most goals match


#*** extract all infformation about matches from occurence list
def get_matches(occurrences_matches,pages):
    for key,values in occurrences_matches.items():
        ps=[]
        for value in values:
            ps.append(pages[int(value)])
        occurrences_matches[key]=ps
    matches = spark.sparkContext.parallelize(occurrences_matches.items(), partitions).map(matches_mapper).reduce(matches_reducer)
    return matches

#*** printing gained information about matches off player
def print_seasion(value,matches):
    team,season = extract_team_years(value)
    key = season +"-&-" +team
    if key in matches.keys():   # if the information was gained
        count_matches=matches[key][0]
        max_match=matches[key][1]
        max_goals=matches[key][2]
    else:
        max_match=""
        max_goals=0
        count_matches=0

    if count_matches!=0:
        value+=" -&- "+str(count_matches) 
    else: 
        value+=" -&- no match"
    print(value)
    print("             + most goals match: "+max_match+"   +Sum: "+ str(max_goals))
    print("         -----------------------------------------------------------------------------------------------")


#*** printig all players with match searched player
def print_results(footballers,matches):
    for key,values in footballers.items():
        print("\n*******************************************************************************************************")
        print(key+":")
        values.sort(key=extract_years)
        for value in values:
            print_seasion(value,matches) # also printing the season of player, which he was playing  

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
    input=""
    # gaining information from arguments
    sys.argv.pop(0)     
    if len(sys.argv)>0:
        # getting accuracy
        try:
            a=float(sys.argv[0])
            if (a<1.0) & (a>0.5): 
                accuracy = a
            sys.argv.pop(0)
        except ValueError:
            accuracy = 0.85
        if len(sys.argv)!=0: # getting name of searched player 
            for arrg in sys.argv:
                input+=arrg+" "
                
            print("\nName of searched player: "+input)
            print("With accuracy: "+ str(accuracy))
            print("Progress:")
            setup_csv_reader()
            path = r'/home/data/footballers_matches.xml'
                
            inputs= input.split()
            information_print("1|8 Getting occurancce of searched player from index_footballers.csv")
            INPUT_occurrences = get_occurrences("/home/data/index_footballers.csv",inputs,False)
            
            if len(INPUT_occurrences)!=0:
                occurrences_footballers= get_AND_occurrences(INPUT_occurrences,inputs)
                del INPUT_occurrences

                information_print("2|8 Opening footballers_matches.xml for reading")
                with open(path, 'r') as file:
                    data = file.read()
                file.close()

                information_print("3|8 Dividing footballers_matches.xml into pages")
                pages = re.findall(r"<html[\S \s]+?</html>", data)
                del data

                information_print("4|8 Extracting informations from a file about a searched player")
                results_footballers, for_matches_season, for_matches_team=get_footballers(occurrences_footballers,pages,input)
                del occurrences_footballers
                
                if len(results_footballers)!=0:
                    information_print("5|8 Sorting result football players by the length of their name")
                    sorted_results = sorted(list(results_footballers.items()), key = lambda key : len(key[0]))
                    footballers = {ele[0] : ele[1]  for ele in sorted_results}
                
                    del results_footballers,sorted_results
                    information_print("6|8 Getting occurancce of searched player matches from index_matches.csv")
                    numbers_seasons=str(len(for_matches_season))
                    occurrences_matches=get_matches_occurrences(for_matches_season,for_matches_team)
                    del for_matches_season,for_matches_team

                    information_print("7|8 Extracting informations from a file about matches")
                    matches =  get_matches(occurrences_matches,pages)
                    del occurrences_matches,pages

                    information_print("8|8 Printing results:")
                    print_results(footballers,matches)
                    print("\n\nTotal numbers of footballers: "+ str(len(footballers)))
                    print("Total numbers of seasons: " +numbers_seasons)
                else:
                    print("             - NO MATCH FOR THIS FOOTBALLER")
            else:
                print("             - NO MATCH FOR THIS FOOTBALLER")
        else:
            print("\nNo player name was entered")

        print("Total time: "+ str(round(time.time() - start, 2))+ " s")
    else:
        print("\nNo player name was entered")