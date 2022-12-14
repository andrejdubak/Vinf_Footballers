import re
import csv
import jellyfish
import time
path = r'C:\Users\Dubak\Desktop\7.semester\VINF\projekt\data\footballers_matches.xml'
    # path = r'/home/data/footballers_matches.xml'
    
def setup_csv_reader():
    """ 
        Prepare csv reader to by able to process big files
    """

    import sys
    maxInt = sys.maxsize

    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt/10)

def extract_years(value):
    """ 
        Extracting only year value from string
        :param value: string with year value in it
        :returns: year value
    """

    return re.findall("\d+", value.split(" -&- ")[1])[0]

def extract_team_years(value):
    """ 
        Extracting team and year from string
        :param value: string with year and team values in it
        :returns: tupple of year and season
    """

    array = value.split(" -&- ")
    return array[0].replace("         - ", ""),array[1]

def read_csv(index):
    """ 
        Reading index file into posting list dictionary
        :param index: path to index
        :returns: dictionary of indexis
    """

    posting_list={}
    with open(index, 'r') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            posting_list[row[0]]=row[1]
    csv_file.close()
    return posting_list

def compare_strings(string1,string2): 
    """ 
        Comparing two strings
        :param string1: first string, which will be compare
        :param string1: second string, which will be compare
        :returns: if the strings match
    """

    strings1=string1.lower().split() #lower case divided into words
    strings2=string2.lower().split()
    for s1 in strings1:
        match=False
        for s2 in strings2:
            if (jellyfish.jaro_distance(s1,s2)>accuracy):#using jaro distance algoritm to determine the degree of similarity
                match=True
                break
        if not match:    
            return False
    return True
   
def get_occurrences(index,inputs,exactly):
    """ 
        Extracting pages from index file on which searching inputs are located 
        :param index: path to index
        :param inputs: inputs that have been entered by the user
        :returns: dictionary containing inputs and pages on which they or similar ones to them are located
    """

    posting_list=read_csv(index)
    if exactly:  # matching only the same words
        occurrences = dict(filter(lambda item: any(part == item[0] for part in inputs), posting_list.items()))
    else:      # matching words which are similar enough according to jaro distance algorithm
        occurrences = dict(filter(lambda item: any((compare_strings(item[0],part)) for part in inputs), posting_list.items()))
    
    INPUT_occurrences={}
    for key,value in occurrences.items():   # for each word found in the index, adding it into dictionary
        occurre_pages = value[1:][:-1].split(", ")   # extracting pages on which the word is found
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
    """ 
        Reducing dictionary by AND rule
        :param INPUT_occurrences: dictionary containing inputs and pages on which they or similar ones to them are located
        :param inputs: inputs that have been entered by the user
        :returns: list containing the numbers of pages on which all the words from the inputs are found
    """

    final_occurrences=INPUT_occurrences[inputs[0]]
    for i in range(len(inputs)-1):  # for each word in the INPUT_occurrences it is checked
        current_occurrences=[]
        for occur in final_occurrences:
            if (inputs[i+1]) in INPUT_occurrences:
                if occur in INPUT_occurrences[inputs[i+1]]:  # if the occurrence pages of the one word are intersecting with occurrence pages of another word
                    current_occurrences.append(occur)
        final_occurrences=current_occurrences
    final_occurrences = sorted(list(dict.fromkeys(final_occurrences)))
    return final_occurrences     

def extract_manager_footballer(page):
    """ 
        Extracting manager from page which was training the player
        :param page: webpage html
        :returns: name of manager
    """

    manager=re.findall(r"(?<=Manager:)[\S \s]+?(?=</h3>)",page)
    if len(manager) !=0:
        manager= re.sub(r"\n|&#.*?;|</b>", "",manager[0]).strip()
    else:
        manager="not found" 
    return manager

def transfer_to_cm(height):
    """ 
        Transfering heigh values from foots into cm
        :param height: height of player in foot
        :returns: height of player in cm
    """

    e_height = height.replace("\"", "").split("\'")
    foot = e_height[0].replace("\'", "")
    inch= e_height[1]
    try:
        return str(round((((int(foot)*12+int(inch))*2.54)/100),2))
    except:
        return "couldn't convert"


def extract_specs_footballer(player):
    """ 
        Extracting informations about player from the season
        :param player: The tags in which the information about player are located
        :returns: postion, height and weight of player in that season
    """

    specs= list(filter(None, re.findall(r"(?<=>)[^<>]*?(?=</)",player)))
    if len(specs)>5:     # if the record is intact 
        i=3
        if specs[i] not in {"G","F","D","M"}:
            i=i-1
        pos=specs[i]     # position of player
        height=specs[i+1]   # heright of player
        try:
            weight=int(specs[i+2].replace(" ", "")) 
            if  weight > 150:
                weight=str(round(weight*0.0625))
            else:
                weight=str(weight)
        except:
            weight=specs[i+2]   # weight of player

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
    """ 
        Extracting team and season from page
        :param page: webpage html
        :returns: team and season, which was player playing
    """

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
    """ 
        Extracting all information player stats from club at that season from page
        :param results: dictionary consist of players which match with searched one
        :param player: The tags in which the information about player are located
        :param check_name: The name of player
        :param club: information about club from that season
    """

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
    """ 
        Getting informations about players get from index
        :param final_occurrences: numbers of pages, where players are located
        :param pages: list of webpages html
        :param imput: input from user
        :returns: dictionary of footballers, list of matches and teams 
    """

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
    """ 
        Reducing dictionary by AND rule
        :param uniques: unique list of teams
        :param INPUT_occurrences_team: teams occurence pages
        :returns: list containing the numbers of pages on which all the words from the uniques are found
    """

    occurrences_team={}
    for unique in uniques:  # for each word in the INPUT_occurrences it is checked
        match=[]
        for key,value in INPUT_occurrences_team.items():
            if key in unique:   # if the occurrence pages of the one word are intersecting with occurrence pages of another word
                match.append(value)
        if len(match)!=0:    # and then the result is added into existing distionary
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
    """ 
        Getting occurencess of teams and seasons separately
        :param for_matches_team: list of teams of player
        :param for_matches_season: list of seasons of player
        :returns: numbers of page where teams and seasons are located
    """

    unique_f_m_season=list(set(for_matches_season)) # unique list of season

    uniques=list(set(for_matches_team))
    unique_f_m_team=[]
    for unique in uniques:  # unique list of teams
        unique_f_m_team=unique_f_m_team+unique.split()

    occurrences_season = get_occurrences("index_matches.csv",unique_f_m_season,True)    # getting seasons occurence pages
    INPUT_occurrences_team = get_occurrences("index_matches.csv",unique_f_m_team,False) # getting teams occurence pages

    del unique_f_m_season, unique_f_m_team

    occurrences_team=extract_team_occurrences(uniques,INPUT_occurrences_team)    # reducing list dictionary of tems by AND rule
    return occurrences_team,occurrences_season

def get_matches(for_matches_season,for_matches_team):
    """ 
        Getting occurencess pages of matches of searched player
        :param for_matches_team: list of teams of player
        :param for_matches_season: list of seasons of player
        :returns: list of occurencces of season, the player played
    """
    occurrences_team,occurrences_season=get_team_season_occurrences(for_matches_team,for_matches_season)  # extracting occurencess of teams and seasons separately

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
    return occurrences_matches  


def extract_max_match(key,pages,matches,occurrences_matches,season,team):
    """ 
        Extracting information about season and club from web page
        :param key: key for dictionary
        :param pages: list of webpages html
        :param matches: dictionary of matches
        :param occurrences_matches: dictionary of pages, where matches are located
        :param season: searched season
        :param team: searched team
        :returns: stats and count of gola from the most score goal match
    """

    max_match=""
    max_goals=0
    if (key in occurrences_matches.keys()):
        for occurre_page in occurrences_matches[key]:      # for if page in occurence page
            page=pages[int(occurre_page)]
            title=re.findall(r"(?<=<title>).+?(?=<)", page)[0]
            if season in title:  # check if season is valid
                all_matches=re.findall(r"<tr[\S \s]+?/tr>", page)
                for raw_match in all_matches:   # then compare all matches
                    if team in raw_match:    # in which was selected team playing
                        fromTags=re.findall(r"(?<=>)\n?[^<>\n]*(?=<)",raw_match)
                        match_sep=' '.join(fromTags).replace("&#160;", "").split()
                        result= match_sep[len(match_sep)-1]
                        match=' '.join(match_sep)
                        if ":" in result:  
                            final_scores = result.split('(')[0].split(":")
                            goals=int(final_scores[0])+int(final_scores[1])
                            if goals>=max_goals:    # and pick up the most goals match
                                max_match=match
                                max_goals=goals
                        matches.append(match)
                del all_matches
    return max_match,max_goals
        

def print_seasion(value,occurrences_matches,pages):
    """ 
        Printing gained information about matches off player
        :param value: info about player from season
        :param occurrences_matches: dictionary of matches
        :param pages: webpages hmtl
    """
    team,season = extract_team_years(value)
    key = season +"-&-" +team
    matches=[]
    
    max_match,max_goals=extract_max_match(key,pages,matches,occurrences_matches,season,team)

    if len(matches)!=0:
        value+=" -&- "+str(len(matches)) 
    else: 
        value+=" -&- no match"
    print(value)
    print("             + most goals match: "+max_match+"   +Sum: "+ str(max_goals))
    print("         -----------------------------------------------------------------------------------------------")

def print_results(reorder_results,occurrences_matches,pages):
    """ 
        Printig all players with match searched player
        :param footballers: dictionary of players
        :param matches: dictionary of matches
    """

    for key,values in reorder_results.items():
        print("\n*******************************************************************************************************")
        print(key+":")
        values.sort(key=extract_years)
        for value in values:
            print_seasion(value,occurrences_matches,pages)  # also printing the season of player, which he was playing

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

    accuracy = 0.85
    setup_csv_reader()
    
    print("\nName of searched player: ", end =" ")
    input = input() 
    start=time.time()
    print("With accuracy: "+ str(accuracy))
    print("Progress:")
    
    inputs= input.split()

    information_print("1|8 Getting occurancce of searched player from index_footballers.csv")
    INPUT_occurrences = get_occurrences("index_footballers.csv",inputs,False)
    
    if len(INPUT_occurrences)!=0:
        final_occurrences_footballers= get_AND_occurrences(INPUT_occurrences,inputs)
        del INPUT_occurrences
        
        information_print("2|8 Opening footballers_matches.xml for reading")
        with open(path, 'r') as file:
            data = file.read()
        file.close()
        
        information_print("3|8 Dividing footballers_matches.xml into pages")
        pages = re.findall(r"<html[\S \s]+?</html>", data)
        del data

        information_print("4|8 Extracting informations from a file about a searched player")
        results_footballers, for_matches_season, for_matches_team=get_footballers(final_occurrences_footballers,pages,input)
        del final_occurrences_footballers
        
        if len(results_footballers)!=0:
            information_print("5|8 Sorting result football players by the length of their name")
            sorted_results = sorted(list(results_footballers.items()), key = lambda key : len(key[0]))
            reorder_results_footballers = {ele[0] : ele[1]  for ele in sorted_results}
            del results_footballers,sorted_results
            
            information_print("6|8 Getting occurancce of searched player matches from index_matches.csv")
            occurrences_matches=get_matches(for_matches_season,for_matches_team)
            numbers_seasons=str(len(for_matches_season))
            del for_matches_season,for_matches_team
            information_print("7|8 Extracting informations from a file about matches")
            information_print("8|8 Printing results:")
            print_results(reorder_results_footballers,occurrences_matches,pages)
            print("\n\nTotal numbers of footballers: "+ str(len(reorder_results_footballers)))
            print("Total numbers of seasons: " +numbers_seasons)
        
        else:
            print("             - NO MATCH FOR THIS FOOTBALLER")
    else:
        print("             - NO MATCH FOR THIS FOOTBALLER")

    print("Total time: "+ str(round(time.time() - start, 2))+ " s")