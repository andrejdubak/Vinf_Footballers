import scrapy
import time
import re
from lxml import html, etree


class FootballersSpider(scrapy.Spider):
    name = 'footballers'
    allowed_domains = ['footballsquads.co.uk']
    start_urls = ['http://www.footballsquads.co.uk/index.html']
    
    def write_to_file(self,response_body):
        htmldoc = html.fromstring(response_body)
        with open("footballers.xml", 'ab') as out:
            out.write(etree.tostring(htmldoc))
    
    def parse(self, response):
        text_file = open("footballers.xml", "w")
        text_file.write('')
        text_file.close()
        self.write_to_file(response.body.decode("ISO-8859-1"))
        
        yield response.follow('squads.htm',callback=self.parse_league)
        yield response.follow('national.htm',callback=self.parse_league)
        yield response.follow('archive.htm',callback=self.parse_league)

    def parse_league(self, response):
        
        table=re.findall(r"<table[\s \S]+?table>", response.body.decode("ISO-8859-1"))[0]
        links=re.findall(r"(?<=<a href=\")[\S \t\n]+?(?=\">)",table)
        
        for link in links:
            self.write_to_file(response.body.decode("ISO-8859-1"))
            yield response.follow(link, callback=self.parse_team)
    
    def parse_team(self, response):
        # print(response.css('title'))
        links=response.css('h5 a::attr(href)')
        main=re.findall(r"main\">[\S \s]+?<h3", response.body.decode("ISO-8859-1"))[0]
        links=re.findall(r"(?<=<a href=\")[\S \t\n]+?(?=\">)",main)
        for link in links:
            #time.sleep(1)
            self.write_to_file(response.body.decode("ISO-8859-1"))
            yield response.follow(link, callback=self.parse_players)

    def parse_players(self, response):
        self.write_to_file(response.body.decode("ISO-8859-1"))
        
        # htmldoc = html.fromstring(response.body.decode("utf-8"))
        # with open("footballers.xml", 'ab') as out:
        #     out.write(etree.tostring(htmldoc))
    