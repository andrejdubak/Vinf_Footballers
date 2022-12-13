import scrapy
import time
import re
from lxml import html, etree

#*** We use the scrapy framework for crawling http://www.footballsquads.co.uk/index.html webpage to find all footballers
class FootballersSpider(scrapy.Spider):
    name = 'footballers'
    allowed_domains = ['footballsquads.co.uk']
    start_urls = ['http://www.footballsquads.co.uk/index.html']
    
    def write_to_file(self,response_body):
        htmldoc = html.fromstring(response_body)
        with open("footballers.xml", 'ab') as out:
            out.write(etree.tostring(htmldoc))
    
    #*** This is function, when our code is starting
    def parse(self, response):
        # Firstly we clear footballers.xml file to be sure, nothing is in here
        text_file = open("footballers.xml", "w")
        text_file.write('')
        text_file.close()
        self.write_to_file(response.body.decode("ISO-8859-1"))
        
        # Then we follow each link witch leade us to different type of league
        yield response.follow('squads.htm',callback=self.parse_league)
        yield response.follow('national.htm',callback=self.parse_league)
        yield response.follow('archive.htm',callback=self.parse_league)

    #*** All lines leading to individual league will be selected here
    def parse_league(self, response):
        table=re.findall(r"<table[\s \S]+?table>", response.body.decode("ISO-8859-1"))[0]
        links=re.findall(r"(?<=<a href=\")[\S \t\n]+?(?=\">)",table)
        
        for link in links:
            self.write_to_file(response.body.decode("ISO-8859-1"))
            yield response.follow(link, callback=self.parse_team)
    
    #*** All lines leading to individual teams will be selected here
    def parse_team(self, response):
        main=re.findall(r"main\">[\S \s]+?<h3", response.body.decode("ISO-8859-1"))[0]
        links=re.findall(r"(?<=<a href=\")[\S \t\n]+?(?=\">)",main)
        for link in links:
            time.sleep(1)
            self.write_to_file(response.body.decode("ISO-8859-1"))
            yield response.follow(link, callback=self.parse_players)

    #*** All webpage html, which contains informations about players is insert into footballers.xml 
    def parse_players(self, response):
        self.write_to_file(response.body.decode("ISO-8859-1"))
    