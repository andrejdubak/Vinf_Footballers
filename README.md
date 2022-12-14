# Vyhľadávač futbalistov
Tento program ponúka riešenie, kde používateľ zadá meno hráča. Systém mu následne vráti zoznam všetkých hráčov, ktorý sú podobný zadanému menu hráča. Mieru podobnosti si vie používateľ určiť sám. Predvolená hodnota je 0,85. Ku každému hráčovi systém priradí jeho informácie z každej sezóny, ktorú odohral. To znamená tím za ktorý hral, manažéra, ktorý ho trénoval, a jeho miery (post, váha, výška) z danej sezóny. Rovnako sú priradené k sezónam aj počty zápasov a zápasy s maximálnym počtom strelených gólov. Program počas celého behu informuje používateľa o aktuálnom stave, v ktorom sa nachádza aj s priradenou časovou jednotkou. Po skončení vyhľadávania sa vypíšu štatistiky.

## Návod na spustenie:

### Príprava pred používaním: 
  1. Stiahnutie git repozitára do zvoleného priečinka cez cmd príkaz ```git clone https://github.com/andrejdubak/Vinf_Footballers.git``` 
  1. Nainštalovanie scrapy knižnice na prehľadávanie web domén do zvoleného priečinka cez cmd príkaz ```pip install scrapy```
  1. Nainštalovanie jellyfish knižnice na porovnávanie stringov do zvoleného pričinka cez cmd príkaz ```pip install jellyfish```
  1. Stiahnutie docker image, ktorý slúži na obsluhu spraku cez cmd príkaz ```docker pull iisas/hadoop-spark-pig-hive:2.9.2```  
  1. Inštalácia virtualneho prostredia cez cmd príkaz ```pip install virtualenv```
  1. Vytvorenie vyrtuálneho prostredia cez cmd príkaz ```virtualenv --python C:\Path\To\Python\python.exe venv```
   
    
 
### Použitie programu:
  1. Spustenie virtualneho prostredia cez cmd príkaz ```.\venv\Scripts\activate``` 
  1. Presunutie sa do scrapy priečinka cmd príkazom ```cd ./spider/spider/spiders/```
  1. Spustenie programu [footballers_spider.py](https://github.com/andrejdubak/Vinf_Footballers/blob/main/spider/spider/spiders/footballers_spider.py) na virtualnom prostredi v priečinku cmd príkazom ```scrapy crawl footballers```, ktoré prehladá celú doménu [https://www.footballsquads.co.uk/index.html](https://www.footballsquads.co.uk/index.html) a vytvorí nám súbor *footballers.xml*, kde sú všetky html page zo stránky. 
  1. Spustenie programu [matches_spider.py](https://github.com/andrejdubak/Vinf_Footballers/blob/main/spider/spider/spiders/matches_spider.pyspider/spider/spiders/matches_spider.py) na virtualnom prostredi  v priečinku cmd príkazom ```scrapy crawl matches```, ktoré prehladá celú doménu [https://www.natipuj.eu/en/results](https://www.natipuj.eu/en/results) a vytvorí nám súbor *matches.xml*, kde sú všetky html page zo stránky. 
  1. Manuálne spojenie súborov *footballers.xml* a  *matches.xml* do jedného veľkého súboru *footballers_matches.xml*, ktorý predstavuje naše dáta, s ktoými budeme ďalej pracovať
  1. Nahranie súborov *footballers_matches.xml*, *[footballers_index_spark.py](https://github.com/andrejdubak/Vinf_Footballers/blob/main/matcher/footballers_index_spark.py)* a *[footballers_parser_spark.py](https://github.com/andrejdubak/Vinf_Footballers/blob/main/matcher/footballers_parser_spark.py)*  do stiahnutého docker imagu do home adresára
  1. Nastavenie správnej cesty pre oba súbory .py (cesty k súborom sú v premennej *path*, ktorá je na začiatku obidvoch súborov)
  1. Spustenie programu *footballers_index_spark.py* docker príkazom ```spark-submit --driver-memory 25g  /path_to_file/footballers_index_spark.py```, ktorý nám vytvorí dva nové index súbory
  1. Spustenie programu *footballers_parser_spark.py* docker príkazom ```spark-submit --driver-memory 25g  /path_to_file/footballers_parser_spark.py```, ktorý nám spustí samotné vyhľadávanie futbalistov
  
  ## Možné dopyty používateľa pre program *footballers_parser_spark.py*:
  Je docker prikaz ```spark-submit --driver-memory 25g  /path_to_file/footballers_parser_spark.py 0.9 Cristiano Ronaldo``` <br />
  - hodnota *0.9* znamená minimálnu mieru zhody medzi výslednými a hľadaným futbalistom <br />
  - hodnota *Cristiano Ronaldo* predstavuje meno hľadaného futbalistu
  
