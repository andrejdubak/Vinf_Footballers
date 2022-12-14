import scrapy
import re
import time
from lxml import html, etree

#*** We use the scrapy framework for crawling https://www.natipuj.eu/en/results webpage to find all matches

class MatchesSpider(scrapy.Spider):
    """ 
        We use the scrapy framework for crawling https://www.natipuj.eu/en/results webpage to find all matches
        :param scrapy.Spider: scrapy element
    """
    name = 'matches'
    allowed_domains = ['natipuj.eu']
    start_urls = ['https://www.natipuj.eu/en/results']

    #*** Writing all webpage html code into file matches.xml
    def write_to_file(self,response_body):
        """ 
            Writing webpage into file
            :param self: class FootballersSpider
            :param response_body: webpage html;
        """
        htmldoc = html.fromstring(response_body.decode("ISO-8859-1"))
        with open("matches.xml", 'ab') as out:
            out.write(etree.tostring(htmldoc))

    #*** This is a when code is starting. First we delete everything from matches.xml to have empty file to write
    def parse(self, response):
        """ 
            The start of the program
            :param self: class FootballersSpider
            :param response: current webpage html;
        """

        text_file = open("matches.xml", "w")
        text_file.write('')
        text_file.close()
        
        self.write_to_file(response.body)

        # We visit all the pages on which this page points. Each represent different league.
        links=re.findall(r"(?<=<li><a href=\")[\S \s]+?(?=\")",response.body.decode("ISO-8859-1"))
        for link in links:
            if link!="/en":
                yield response.follow(link, callback=self.parseSession)

    #*** Here we look through each season of the league 
    def parseSession(self, response):
        """ 
            Extracting all seasons from league page
            :param self: class FootballersSpider
            :param response: current webpage html;
        """

        # We looking for each links to each season of league
        css3menu=re.findall(r"(?<=css3menu_top)[\S \s]+?(?=<script)",response.body.decode("ISO-8859-1"))[0]
        topfirst=re.findall(r"(?<=<li class=\"topfirst)[\S \s]+?(?=<li class=\"topmenu)",css3menu)[0]
        links=re.findall(r"(?<=<li><a href=\")[\S \s]+?(?=\")",topfirst)
        for link in links:
            time.sleep(1)
            yield response.follow(link, callback=self.parseMatches)

    # Write down each season of each league into file matches.xml
    def parseMatches(self, response):
        """ 
            Inserting all information about matches from season pages into file 
            :param self: class FootballersSpider
            :param response: current webpage html;
        """

        self.write_to_file(response.body)