import scrapy
import re
from lxml import html, etree

class MatchesSpider(scrapy.Spider):
    name = 'matches'
    allowed_domains = ['natipuj.eu']
    start_urls = ['https://www.natipuj.eu/en/results']

    def write_to_file(self,response_body):
        htmldoc = html.fromstring(response_body.decode("ISO-8859-1"))
        with open("matches.xml", 'ab') as out:
            out.write(etree.tostring(htmldoc))

    def parse(self, response):
        text_file = open("matches.xml", "w")
        text_file.write('')
        text_file.close()
        
        self.write_to_file(response.body)

        
        links=re.findall(r"(?<=<li><a href=\")[\S \s]+?(?=\")",response.body.decode("ISO-8859-1"))
        #links=response.css('li a::attr(href)')
        for link in links:
            if link!="/en":
                yield response.follow(link, callback=self.parseSession)

    def parseSession(self, response):
        # print(response.css('title').get())
        # links=response.css('ul#css3menu_top').css('li.topfirst a::attr(href)')
        css3menu=re.findall(r"(?<=css3menu_top)[\S \s]+?(?=<script)",response.body.decode("ISO-8859-1"))[0]
        topfirst=re.findall(r"(?<=<li class=\"topfirst)[\S \s]+?(?=<li class=\"topmenu)",css3menu)[0]
        links=re.findall(r"(?<=<li><a href=\")[\S \s]+?(?=\")",topfirst)
        for link in links:
            # time.sleep(1)
            yield response.follow(link, callback=self.parseMatches)

    def parseMatches(self, response):
        #print(response.css('title').get())
        self.write_to_file(response.body)