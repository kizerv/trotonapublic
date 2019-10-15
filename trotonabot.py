# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import requests
import re
from elasticsearch import Elasticsearch 
import datetime


IPElastic = "localhost"


urlbase = 'http://www.futmondo.com'
startingTeam = '/team?team=504e581e4d8bec9a670000cb'

req = requests.get(urlbase + startingTeam)
soup = BeautifulSoup(req.text, "lxml")


def getTeams(soup):
	links = []
	for link in soup.findAll('a', attrs={'href': re.compile("^/team*")}):
		href = link.get('href')
		if href not in links:
			links.append(href)
	return links


def getPlayersFromTeam(teams):
	links = []
	for team in teams:
		req = requests.get (urlbase + team)
		soup = BeautifulSoup (req.text, "lxml")
		for link in soup.findAll('a', attrs={'href': re.compile("^/player*")}):
			href = link.get('href')
			if href not in links:
				links.append(href)
	return links

def getInfoFromPlayers(players):
	playerData = {}
	matchesData = {}
	marketData = {}
	allPlayers = []
	allMatches = []
	allMarket = []
	allinfo = {}

	for playerlink in players:
		playerData = {}
		marketData = {}
		req = requests.get (urlbase + playerlink)
		soup = BeautifulSoup (req.text, "lxml")
		for player in soup.find('h1'):
			playerData.update (jugador = player)
			playerData.update (injured = 'no')
			playerData.update (doubt = 'no')
		for status in soup.find('div', attrs={'class':'status'}):
			if "injured" in str(status):
				playerData.update(injured = 'yes')
			if "doubt" in str(status):
				playerData.update(doubt = 'yes')
			if "redcard" in str(status):
				playerData.update(redcard = 'yes')
		for player in soup.find_all('ul', attrs={'class':'value'}):
			for data in player.find_all("li"):
				if "role" in data.get("class")[0]: 
					playerData.update (position = data.get('class')[1])
				if "center" in data.get("class")[0]:
					marketData.update (jugador = playerData ['jugador'] )
					marketData.update (valor =  int(data.get_text()[16:-2].strip().replace(".","")))
					marketData.update (fecha = datetime.date.today())
		equipo = soup.find('article', attrs={'class':'data'}).figure.a.get('title').strip()
		playerData.update (equipo = equipo)

		partidos = []

		for stats in soup.find_all("ul", attrs={'class':'playerStats'}):
			jornada = {}
			jornada.update (jugador = playerData['jugador'])

			for li in stats.find_all("li"):
				if "gweek" in li.get("class")[0]:
					if len(li.get_text()[8:]) is not 0:
						jornada.update(jornada = int(li.get_text()[8:].strip()))
					else:
						jornada.update(jornada = 0)
				if "points" in li.get("class")[0]:
					if "press" in li.get("class")[1]:
						jornada.update(press = int (li.text.strip().strip()))
					else:
						jornada.update(stats = int (li.text.strip().strip()))
				if "Crest" in li.get("class")[0]:
					rival = li.img.get('title').strip()
					if equipo not in rival:
						if "homeCrest" in li.get("class")[0]:
							jornada.update(home = False)
						else:
							jornada.update(home = True)
						jornada.update(rival = rival)
						jornada.update(position = playerData ['position'])
						jornada.update(equipo = playerData ['equipo'])
				if "played" in li.get("class")[0]:
					jornada.update (played = li.get("class")[1])
				if "titular" in li.get("class")[0]:
					jornada.update (startingTeam = li.get("class")[1])
			if "Futmondo" not in jornada ['rival']:
				partidos.append (jornada)

		allPlayers.append(playerData)

		for partido in partidos:
			allMatches.append(partido)

		allMarket.append(marketData)

	allinfo.update (jugadores = allPlayers)
	allinfo.update (partidos = allMatches)
	allinfo.update (mercado = allMarket)

	return allinfo

def saveToDisk(info):
	f = open('dict.txt','w')
	f.write(str(info))
	f.close()

def getFromDisk():
	f = open('dict.txt','r')
	data=f.read()
	f.close()
	info = eval(data)
	return info

# def sendToElastic (info):
# 	es = Elasticsearch(
# 	    hosts = [{'host': 'search-futlastic-upt3ob7wyaoigkw64psnxdvqsq.eu-west-3.es.amazonaws.com', 'port': 443}],
# 	    use_ssl = True,
# 	    verify_certs = False
# 	)

# 	# for jugadores in info['jugadores']:
# 	#	es.index(index='futmondo_jugadores', doc_type='_doc', body=jugadores)

# 	for partidos in info['partidos']:
# 		es.index(index='futmondo_partidos', doc_type='_doc', body=partidos)

# 	# for mercado in info['mercado']:
# 		# es.index(index='futmondo_mercado', doc_type='_doc', body=mercado)

def sendToElastic (info):
	es = Elasticsearch(
	    hosts = [{'host': 'IPElastic', 'port': 9200}],
	    use_ssl = False,
	    verify_certs = False
	)

	# for jugadores in info['jugadores']:
	#	es.index(index='futmondo_jugadores', doc_type='_doc', body=jugadores)

	for partidos in info['partidos']:
		es.index(index='futmondo_partidos', doc_type='_doc', body=partidos)

	# for mercado in info['mercado']:
		# es.index(index='futmondo_mercado', doc_type='_doc', body=mercado)


print ("Rapiñando links equipos...")
teams = getTeams(soup)
print ("Rapiñando links de jugadores...")
players = getPlayersFromTeam(teams)
print ("Sacando los higadillos de cada uno...")
info = getInfoFromPlayers (players)
print ("Guardando...")
saveToDisk (info)
sendToElastic (info) 
#sendToElastic(getFromDisk())


