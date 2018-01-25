import scrapy
import re
from scrapy.selector import Selector
from scrapy.http import HtmlResponse
from time import gmtime, strftime
import pandas as pd

class OGolSpider(scrapy.Spider):
    name = 'ogolspider'

    handle_httpstatus_list = [500]

    #anos = [ str(i) for i in range(2017, 2005, -1)]
    anos = ['2006', '2013', '2014', '2016']
    meses = [ '0'+str(i) if i < 10 else str(i) for i in range(4, 13)]
    dias = [ '0'+str(i) if i < 10 else str(i) for i in range(1, 32)]

    lista_links = {}
    
    #start_urls = [
    #    'http://www.espn.com/soccer/fixtures/_/date/20130613/league/bra.1',
    #    'http://www.espn.com/soccer/fixtures/_/date/20130529/league/bra.1',
    #    'http://www.espn.com/soccer/fixtures/_/date/20171127/league/bra.1'
    #]

    logfile = open('log_file.txt', 'w')
    
    start_urls = [
        '/match?gameId=446153', # 2016
        '/match?gameId=445960', # 2016
        '/match?gameId=390297',
        #'/match?gameId=446293', # 2014
        '/match?gameId=390303', # 2014        
    ]

    #Faz a leitura das start_urls do arquivo txt
    #with open('links_2018-01-25_20-07-12.txt') as f:
    #    start_urls = f.read().splitlines()

    print(len(start_urls))

    infos_jogos = pd.DataFrame(columns=['matchId', 'home', 'away', 'competition', 'home_goals', 'away_goals', 'date', 'stadium', 'home_lineup', 'away_lineup'])
    i = 0
    porJogo = True
    
    def start_requests(self):
        if self.porJogo:
            self.logfile.write("Programa iniciado em " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "\n")
            for url in self.start_urls:
                yield scrapy.Request('http://www.espn.com/soccer/' + url, callback=self.principal, dont_filter=True)
                self.logfile.flush()
        else:
            url_base = 'http://www.espn.com/soccer/fixtures/_/date/'
            self.logfile.write("Programa iniciado em " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "\n")
            for ano in self.anos:
                for mes in self.meses:
                    for dia in self.dias:
                        url = url_base + ano + mes + dia + '/league/bra.1'
                        yield scrapy.Request(url, callback=self.parse, dont_filter=True)
                        self.logfile.flush()
            self.logfile.write("Fim das buscas em " + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "\n")
            
            #for url in self.start_urls:
            #yield scrapy.Request(url, callback=self.parse, dont_filter=True)

    def closed(self, reason):
        if self.porJogo:
            if reason == "finished":
                self.infos_jogos.to_csv('infos_jogos_' + strftime("%Y-%m-%d_%H-%M-%S", gmtime()) + '.csv', encoding='utf-8-sig')
                print("Dados salvos em csv")
        else:
            if reason == "finished":
                print("\tGuardando links no arquivo links.txt...:\n");
                thefile = open('links_' + strftime("%Y-%m-%d_%H-%M-%S", gmtime()) + '.txt', 'w')
                chaves = self.lista_links.keys()
                
                for chave_link in sorted(chaves):
                      thefile.write("%s\n" % chave_link)

    def parse(self, response):
        if response.status != 200:
            self.logfile.write("[" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "] ERROR CODE "+ str(response.status) +" NO LINK " + str(response.url) + "\n")
        sem_jogos_agendados = response.css('p[id=noScheduleContent]')
        if not sem_jogos_agendados:
            links_jogos = response.css("div[id=sched-container] a::attr(href)").extract()
            self.logfile.write("[" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "] Foram encontrados " + str(len(links_jogos)/2) + " jogos nesta data " + str(response.url) + "\n")
            for link in links_jogos:
                if "gameId" in link:
                    self.lista_links[link] = response.url
        else:
            self.logfile.write("[" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "] Sem jogos nesta data " + str(response.url) + "\n")      

    def principal(self, response):
        #response.css("div[data-home-away=away]").css("ul[data-event-type=goal]").css("li")
        #response.css("div[data-home-away=home]").css("ul[data-event-type=goal]").css("li")
        times = response.css('a[class="team-name"] span[class="long-name"]::text').extract()
        competicao = response.css('div[class="game-details header"]::text').extract()[0].strip()
        data_jogo = response.css('ul.gi-group')[1].css('span::attr(data-date)').extract_first()
        estadio = response.css('li[class="venue"] div::text').extract()[0].replace('VENUE: ', '')

        base = response.css('div.content-tab')
        if (len(base) > 0):
            time_casa = base[0].css('div[class="accordion-header lineup-player"] span[class="name"]::text').extract()
            time_fora = base[1].css('div[class="accordion-header lineup-player"] span[class="name"]::text').extract()
        else:
            time_casa = []
            time_fora = []

        self.infos_jogos.loc[self.i] = [response.url, times[0], times[1], competicao, self.getGoals(response, "home"), self.getGoals(response, "away"), data_jogo, estadio, self.getLineUps(response, time_casa), self.getLineUps(response, time_fora)]
        self.i = self.i + 1

        self.logfile.write("[" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "] " + competicao + " " + times[0] + " vs. " + times[1] + " com sucesso!" + "\n")
        
        #print(self.infos_jogos)

    def getLineUps(self, response, team):
        jogadores = []
        reserva = 0
        
        for element in team:
            if (len(element.strip()) > 0):
                if reserva != 0:
                    jogadores.append('(' + element.strip() + ' ' + str(reserva) + '\')')
                    reserva = 0
                else:  
                    if element.strip().isdigit():
                        reserva = int(element.strip())
                    else:
                        jogadores.append(element.strip())
                        reserva = 0
                        
        return(jogadores)
        
    
    def getGoals(self, response, side):
        team = response.css("div[data-home-away=" + side + "]").css("ul[data-event-type=goal]")
        
        lista_gols = team.css("li::text").extract()
        tempo_gols = team.css("li").css("span::text").extract()
    
        nomes = []
        tempos = []
        
        for element in lista_gols:
            if(len(element.strip()) > 0):
                nomes.append(element.strip())

        for element in tempo_gols:
            if(len(element.strip()) > 0):
                tempos.append(element.strip())

        return([nomes, tempos])
