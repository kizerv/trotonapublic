# -*- coding: utf-8 -*-


from elasticsearch import Elasticsearch
import datetime 

IPElastic = "localhost"

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

def getFromDisk():
	f = open('dict.txt','r')
	data=f.read()
	f.close()
	info = eval(data)
	return info

print ("Enviando dict a Elastic")
sendToElastic(getFromDisk())
print ("Listo")
