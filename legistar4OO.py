''' legistar4OO

Created on May 7, 2019

v0.1 250417 update

@author: rik
'''

from collections import defaultdict
import json
import os
import pathlib
import pickle
import re 
import sqlite3 as sqlite
import sys

# import requests
# import pyodata

import urllib.request
from _ast import Or

## utilities

def getDBSize(currDB):
	'''also return number of replicates 
	'''
	curs = currDB.cursor()
	stats = {}
	for tblName in DBTableSpecTbl.keys():
		sql = 'SELECT Count(*) FROM %s' % (tblName)
		curs.execute(sql)
		res = curs.fetchall()
		stats[tblName] = res[0][0]
	return stats

DBTableSpecTbl = {'municipality':
				  """CREATE TABLE IF NOT EXISTS municipality (
				  muniID	INTEGER PRIMARY KEY,
				  name      TEXT,
				  publicURL TEXT,
				  client	TEXT
				  )""",
				  'body':
				  """CREATE TABLE IF NOT EXISTS body (
				  bodyID	INTEGER PRIMARY KEY,
				  muniID    INTEGER,
				  name	TEXT,
				  contactName TEXT,
				  phone TEXT,
				  email TEXT
				  )""",
				  'event':
				  """CREATE TABLE IF NOT EXISTS event (
				  eventID INTEGER PRIMARY KEY,  
				  bodyID	INTEGER,
				  date	TEXT,
				  siteURL TEXT,
				  agendaURL	TEXT,
				  minutesURL TEXT
				  )""",
				  'eventItem':
				  """CREATE TABLE IF NOT EXISTS eventItem (
				  eitemID	INTEGER PRIMARY KEY,
				  eiEventId INTEGER,
				  agendaSequence INTEGER,
				  agendaNumber INTEGER,
				  minutesSequence INTEGER
				  )""",
				  'eiAttachment':
				  """CREATE TABLE IF NOT EXISTS eiAttachment (
				  eiaIdx INTEGER PRIMARY KEY,
				  eiaEventId INTEGER,
				  eiaItemId INTEGER,
				  matterId INTEGER,
				  eiaModDate TEXT,
				  link TEXT
				  )""",
				  'body2muni':
				   """CREATE TABLE IF NOT EXISTS body2muni (
				  b2mIdx INTEGER PRIMARY KEY,
				  bodyIdx INTEGER,
				  muniIdx INTEGER
				  )""",
				  'event2body':
				   """CREATE TABLE IF NOT EXISTS event2body (
				  e2bIdx INTEGER PRIMARY KEY,
				  eventIdx INTEGER,
				  bodyIdx INTEGER
				  )""",
				  'ei2e':
				   """CREATE TABLE IF NOT EXISTS ei2e (
				  ei2eIdx INTEGER PRIMARY KEY,
				  eiIdx INTEGER,
				  eIdx INTEGER
				  )""",
				  'eia2ei':
				   """CREATE TABLE IF NOT EXISTS eia2ei (
				  eia2eiIdx INTEGER PRIMARY KEY,
				  eiaIdx INTEGER,
				  eiIdx INTEGER
				  )""",}


def initDB(currDB):
	curs = currDB.cursor()

	for tbl in DBTableSpecTbl.keys():
		curs.execute('DROP TABLE IF EXISTS %s' % (tbl) )
		curs.execute(DBTableSpecTbl[tbl])

	# check to make sure it loaded as expected
	curs = currDB.cursor()
	curs.execute("select tbl_name from sqlite_master")
	allTblList = curs.fetchall()
	for tbl in DBTableSpecTbl.keys():
		assert (tbl,) in allTblList, "initdb: no %s table?!" % tbl

	return currDB

def postEvents(currDB,eventList):
	curs = currDB.cursor()
	ninsert = 0
	for e in eventList:
		
		try:		
			sql = 'insert into event (eventID,bodyID,date,siteURL,agendaURL,minutesURL) values(?,?,?,?,?,?)'
			valList = [e['EventId'], e['EventBodyId'], e['EventDate'],e['EventInSiteURL'],e['EventAgendaFile'],e['EventMinutesFile']]
			curs.execute(sql,tuple(valList))
			ninsert += 1
			# eventIdx = cursor.lastrowid
		except Exception as e:
			print('event', e)
			#eventIdx = -1

	print('events2db: %d/%d' % (len(eventList),ninsert))
	currDB.commit()
	
def postAllEventItems(currDB,cacheDir=None,bodyID=None,verbose=False):
	'''retrieve all eventItems associated with ALL events
	and all attachments associated with the eventItems
	'''
	
	curs = currDB.cursor()
	sql = '''select event.eventID from event'''
	if bodyID != None:
		sql += f''' where event.bodyID = {bodyID}'''
	curs.execute(sql)
	res = curs.fetchall()
	nevent = len(res)

	print(f'postAllEventItems: {nevent=}')
	
	nEIinsert = 0
	nEIAinsert = 0
	
	for irow,row in enumerate(res):
		eid = row[0]
		eitemList = getOneEventsItems(eid,cacheDir)
		
		for eitem in eitemList:
			try:		
				sql2 = 'insert into eventItem (eitemID,eiEventID,agendaSequence,agendaNumber,minutesSequence) values(?,?,?,?,?)'
				valList = [eitem['EventItemId'], eid, eitem['EventItemAgendaSequence'],eitem['EventItemAgendaNumber'],eitem['EventItemMinutesSequence']]
				curs.execute(sql2,tuple(valList))
				nEIinsert += 1
				eiIdx = curs.lastrowid
			except Exception as e:
				print('eventItem', e)
				eiIdx = -1
				
			try:		
				sql3 = 'insert into ei2e (eiIdx,eIdx) values(?,?)'
				valList = [eiIdx,eid]
				curs.execute(sql3,tuple(valList))
			except Exception as e:
				print('ei2e', e)
				
			for attach in eitem['EventItemMatterAttachments']:
				modDate = attach['MatterAttachmentLastModifiedUtc']
				matterId = attach['MatterAttachmentId']
				link = attach['MatterAttachmentHyperlink']

				try:		
					sql4 = 'insert into eiAttachment (eiaEventId,eiaItemId,eiaModDate,matterId,link) values(?,?,?,?,?)'
					valList = [eid,eitem['EventItemId'],modDate,matterId,link]
					curs.execute(sql4,tuple(valList))
					nEIAinsert += 1
					eiaIdx = curs.lastrowid
				except Exception as e:
					print('eventItemAttach', e)
					eiaIdx = -1

				try:		
					sql5 = 'insert into eia2ei (eiaIdx,eiIdx) values(?,?)'
					valList = [eiaIdx,eiIdx]
					curs.execute(sql5,tuple(valList))
				except Exception as e:
					print('eia2ei', e)
				
					
		currDB.commit() # NB: commit after every event
		if verbose:
			print(f'postAllEventItems: eventID={eid} {nEIinsert=} {nEIAinsert=}')
			
	print(f'postAllEventItems: done {nevent=} {nEIinsert=} {nEIAinsert=}')
				
def anlyzEventItems(currDB,rptDir,cacheDir=None,bodyID=None):
	'''explore eventItems and attachments associated with the eventItems
	'''
	
	curs = currDB.cursor()
	sql = '''select event.eventID from event'''
	if bodyID != None:
		sql += f''' where event.bodyID = {bodyID}'''
	curs.execute(sql)
	res = curs.fetchall()
	nevent = len(res)
	
	print(f'anlyzEventItems: {nevent=}')
	stats = defaultdict(lambda: defaultdict(int)) # eitemKey -> val -> freq
	neitems = 0
	neia = 0
	
	# "header" fields to be ignored
	
	hdrFields = ['EventItemId', 'EventItemGuid', 'EventItemLastModifiedUtc,EventItemRowVersion', 
				'EventItemEventId', 'EventItemLastModifiedUtc','EventItemRowVersion',
				'EventItemMatterId', 'EventItemMatterGuid','EventItemAgendaSequence', 
				'EventItemMinutesSequence', 'EventItemMoverId', 'EventItemSeconderId', 'EventItemVideoIndex']
	
	for irow,row in enumerate(res):
		eid = row[0]
		
		# NB: whether to use cache resolved in getOneEventsItems
		eitemList = getOneEventsItems(eid,cacheDir)
		
		neitems += len(eitemList)
		
		for eitem in eitemList:
			# eitem fields
			# EventItemId, EventItemGuid, EventItemLastModifiedUtc,
			# EventItemRowVersion, EventItemEventId, EventItemAgendaSequence,
			# EventItemMinutesSequence, EventItemAgendaNumber, EventItemVideo,
			# EventItemVideoIndex, EventItemVersion, EventItemAgendaNote,
			# EventItemMinutesNote, EventItemActionId, EventItemActionName,
			# EventItemActionText, EventItemPassedFlag, EventItemPassedFlagName,
			# EventItemRollCallFlag, EventItemFlagExtra, EventItemTitle,
			# EventItemTally, EventItemAccelaRecordId, EventItemConsent,
			# EventItemMoverId, EventItemMover, EventItemSeconderId,
			# EventItemSeconder, EventItemMatterId, EventItemMatterGuid,
			# EventItemMatterFile, EventItemMatterName, EventItemMatterType,
			# EventItemMatterStatus, EventItemMatterAttachments

			# print('eitem0')
			for eik,eiv in eitem.items():
				if eik in hdrFields:
					continue
				
				# count text fields
				
				if eik=='EventItemTitle' and eiv != None and len(eiv) > 0:
					stats[eik][0] += 1
					continue
				
				if eik=='EventItemMinutesNote' and eiv != None and len(eiv) > 0:
					stats[eik][0] += 1
					continue
				
				if eik=='EventItemActionText' and eiv != None and len(eiv) > 0:
					stats[eik][0] += 1
					continue
				
				if eik=='EventMatterName' and eiv != None and len(eiv) > 0:
					stats[eik][0] += 1
					continue

				if eik=='EventItemMatterName' and eiv != None and len(eiv) > 0:
					stats[eik][0] += 1
					continue  

				# count variable fields
				
				if eik=='EventItemMatterFile':
					stats[eik][0] += 1
					continue

				if eik=='EventItemAgendaNote':
					stats[eik][0] += 1
					continue

				if eik=='EventItemVideo':
					stats[eik][0] += 1
					continue

				if eik=='EventItemAgendaNumber':
					stats[eik][0] += 1
					continue
					
				if eik=='EventItemMatterAttachments':
					# EventItemMatterAttachments keys
					# ['MatterAttachmentId', 'MatterAttachmentGuid', 'MatterAttachmentLastModifiedUtc', 'MatterAttachmentRowVersion', 
					# 'MatterAttachmentName', 'MatterAttachmentHyperlink', 'MatterAttachmentFileName', 'MatterAttachmentMatterVersion', 
					# 'MatterAttachmentIsHyperlink', 'MatterAttachmentBinary', 'MatterAttachmentIsSupportingDocument', 
					# 'MatterAttachmentShowOnInternetPage', 'MatterAttachmentIsMinuteOrder', 'MatterAttachmentIsBoardLetter', 
					# 'MatterAttachmentAgiloftId', 'MatterAttachmentDescription', 'MatterAttachmentPrintWithReports', 'MatterAttachmentSort']

					if len(eiv) > 0:
						neia += len(eiv)
						for eia in eiv:
							if eia['MatterAttachmentShowOnInternetPage']:
								stats[eik]['showTrue'] += 1
							else:
								stats[eik]['showFalse'] += 1
					continue
				
				try:
					if eiv != None and eiv != 0:
						stats[eik][eiv] += 1
				except Exception as e:
					print('huh')
					
			# print('eitem1')
					
			# sql2 = 'insert into eventItem (eitemID,eiEventID,agendaSequence,agendaNumber,minutesSequence) values(?,?,?,?,?)'
			# valList = [eitem['EventItemId'], eid, eitem['EventItemAgendaSequence'],eitem['EventItemAgendaNumber'],eitem['EventItemMinutesSequence']]
			#
			# for attach in eitem['EventItemMatterAttachments']:
			# 	print('attach')
			# 	modDate = attach['MatterAttachmentLastModifiedUtc']
			# 	matterId = attach['MatterAttachmentId']
			# 	link = attach['MatterAttachmentHyperlink']
			#
			# 	sql4 = 'insert into eiAttachment (eiaEventId,eiaItemId,eiaModDate,matterId,link) values(?,?,?,?,?)'
			# 	valList = [eid,eitem['EventItemId'],modDate,matterId,link]
									
		# currDB.commit() # NB: commit after every event
		print(f'{irow=}')
		
	outs = open(rptDir+'summaryStats.txt','w')
	outs.write(f'anlyzEventItems: {nevent=} {neitems=} {neia=}\n')
	outs.write('Key,Val,Freq\n')
	for k in stats.keys():
		for nv,v in enumerate(stats[k]):
			if nv==0 and v==0:
				outs.write(f'{k},0,{stats[k][v]}\n')
				break
			outs.write(f'{k},{v},{stats[k][v]}\n')
	outs.close()				

def getEvents(startDate,cacheFile=None,bodyID=None,endDate=None):
	'''ASSUME startDate is string; bodyID optional
	'''
	
	qstr = f"events?$filter=EventDate+ge+datetime%27{startDate}%27" 
	if endDate != None:
		qstr += f"+and+EventDate+le+datetime%27{endDate}%27"
	if bodyID != None:
		qstr += f"+and+EventBodyId+eq+{bodyID}"
		
	fullURL = "http://"+SERVICE_ROOT_URL+qstr
	
	contents = urllib.request.urlopen(fullURL)
	payload = contents.read()
	
	eventList = json.loads(payload)
	
	# use pickel vs json?
	if cacheFile != None:
		with open(cacheFile,'wb') as f:
			pickle.dump(eventList,f)
		
	return eventList

def getOneEventsItems(eventID,cacheDir=None):
	
	# GET v1/{Client}/Events/{EventId}/EventItems?AgendaNote={AgendaNote}&MinutesNote={MinutesNote}&Attachments={Attachments}

	qstr = "Events/%s/EventItems?AgendaNote=1&MinutesNote=1&Attachments=1" % (eventID)
	fullURL = "http://"+SERVICE_ROOT_URL+qstr
		
	# use pickel vs json?
	if cacheDir == None:
		contents = urllib.request.urlopen(fullURL)
		payload = contents.read()
		eitemList = json.loads(payload)

	else:
		eitemFile = cacheDir + f'eitemList_{eventID}.pkl'
		if pathlib.Path(eitemFile).is_file():
			with open(eitemFile,'rb') as f:
				eitemList = pickle.load(f)
		else:
			contents = urllib.request.urlopen(fullURL)
			payload = contents.read()
			eitemList = json.loads(payload)
			
			with open(eitemFile,'wb') as f:
				pickle.dump(eitemList,f)
		
	return eitemList

def harvestEventAgenda(currDB,bodyID,pdfDir,verbose=True):
	'''download events' agendas
	'''
	curs = currDB.cursor()
	sql1 = '''select eventID,agendaURL from event '''
	curs.execute(sql1)
	res = curs.fetchall()
	nevent = len(res)
	nmiss = 0
	npdfErr = 0
	nskip=0
	
	print(f'harvestEventAgenda: {nevent=}')
	for row in res:
		eventID,url = row
		if url ==  None:
			nmiss += 1
			continue
		
		fname = pdfDir + f'agenda_%s_%05d.pdf' % (bodyID,eventID)
		if pathlib.Path(fname).is_file():
			nskip += 1
			continue

		try:
			pdf = urllib.request.urlopen(url)
			pdfData = pdf.read()
		except Exception as e:
			print(f'harvestEventAgenda: {url=} {e=}')
			npdfErr += 1
			continue
		
		with open(pdfDir+fname,'wb') as outf:
			outf.write(pdfData)
			
		if verbose:
			print('harvestEventAgenda: ', eventID)
		
	print(f'harvestEventAgenda: done. NRow={len(res)} {nskip=} {nevent=} {nmiss=} {npdfErr=}')

def harvestAttach(currDB,pdfDir,verbose=True):
	'''download ALL eiAttachments
	'''
	curs = currDB.cursor()
	sql1 = '''select eiaIdx,link from eiAttachment '''
	curs.execute(sql1)
	res = curs.fetchall()
	neia = len(res)
	
	print(f'harvestAttach: {neia=}')

	nerr = 0
	nskip = 0
	
	for row in res:
		eiaIdx,link = row
		
		fname = pdfDir + 'eia_%05d.pdf' % (eiaIdx)
		if pathlib.Path(fname).is_file():
			nskip += 1
			continue

		try:
			pdf = urllib.request.urlopen(link)
		except Exception as e:
			print('harvestAttach: ',eiaIdx,link,e)
			nerr += 1
			continue
		
		pdfData = pdf.read()
		with open(fname,'wb') as outf:
			outf.write(pdfData)
	
		if verbose:
			print('harvestAttach: ', eiaIdx)
			
	print(f'harvestAttach: {nskip=} {nerr=}')
	
def parseAgenda(atxt):
	''' return [(item#,topic,body)]
	'''
	
	# ASSUME items begin with item# on its own line
	items = re.split(r'^([0-9]+)$',atxt,flags=re.M)
	
	aInfoList = []
	currItemNum = None
	for item in items:
		# NB: drop preamble
		if currItemNum == None:
			currItemNum = 0
		elif re.match(r'[0-9]+',item):
			currItemNum = int(item)
		else:
			lines = item.split('\n')
			lines2 = [l.strip() for l in lines]
			lines3 = [l for l in lines if l != '']
			topic = lines3[0]
			adjournFnd = None
			for ib, l in enumerate(lines3):
				if l == 'ADJOURNMENT':
					adjournFnd = ib
					break
			if adjournFnd==None:
				body = lines3[1:]
			else:
				body = lines3[1:adjournFnd]

				
			
			aInfo = {'itemNum': currItemNum, 'topic': topic, 'body': body}
			
			aInfoList.append(aInfo)
			
	return aInfoList
	
SERVICE_ROOT_URL = 'webapi.legistar.com/v1/oakland/'	

# HACK: bootstrap of initially identified Bay Area Legistar clients
KnownLegistarClients = {'oakland': ['Oakland', 'https://oakland.legistar.com/Calendar.aspx'],
      # 'sanmateocounty': ['San Mateo (county)', 'https://sanmateocounty.legistar.com/Calendar.aspx'], 
      # 'mountainview': ['Mountain View', 'https://mountainview.legistar.com/Calendar.aspx'], 
      # 'cupertino': ['Cupertino', 'https://cupertino.legistar.com/Calendar.aspx'], 
      # 'sunnyvaleca': ['Sunnyvale. Milpitas. Palo Alto', 'https://sunnyvaleca.legistar.com/Calendar.aspx']
						}

def addAllMuni(currDB):
	curs = currDB.cursor()
	ninsert = 0
	for legClient,clientInfo in KnownLegistarClients.items():
		name,url = KnownLegistarClients[legClient]
		valList = [name,url,legClient]
		try:		
			sql = 'insert into municipality (name,publicURL,client) values(?,?,?)'
			curs.execute(sql,tuple(valList))
			ninsert += 1
		except Exception as e:
			print('muni', e)
		
	currDB.commit()	
	print('addAllMuni: %d/%d' % (len(KnownLegistarClients),ninsert))
		
def addBodies(currDB,legClient):

	curs = currDB.cursor()
	
	sql = '''select muniID from municipality where client="%s" ''' % (legClient)
	curs.execute(sql)
	res = curs.fetchall()
	muniIdx = res[0][0]
	
	legService = 'webapi.legistar.com/v1/' + legClient + '/'
	qstr = "Bodies" 
	fullURL = "http://" + legService + qstr
	
	contents = urllib.request.urlopen(fullURL)
	allBodies = json.loads(contents.read())

	curs = currDB.cursor()
	ninsert = 0
	for bodyInfo in allBodies:
		name = bodyInfo['BodyName']
		contactName = bodyInfo['BodyContactFullName']
		phone = bodyInfo['BodyContactPhone']
		email = bodyInfo['BodyContactEmail']
		valList = [muniIdx,name,contactName,phone,email]
		try:		
			sql = 'insert into body (muniID,name,contactName,phone,email) values(?,?,?,?,?)'
			curs.execute(sql,tuple(valList))
			bodyIdx = curs.lastrowid
			ninsert += 1
		except Exception as e:
			print('body', bodyInfo, e)

		try:		
			sql2 = 'insert into body2muni (bodyIdx,muniIdx) values(?,?)'
			valList = [bodyIdx,muniIdx]
			curs.execute(sql2,tuple(valList))
		except Exception as e:
			print('body2muni', e)
	
	currDB.commit()
	print('addBodies: %s %d/%d' % (legClient,len(allBodies),ninsert))
	
def main():

	runDate = '250423'
	beginDate = '2024-01-01'
	endDate = None # '2023-12-31'
	useCache = True
	initializeDB = False
	
	PubSafetyBodyID = '12'
	SpecialPubSafetyBodyID = '160'
	CCBodyID = '230'
	
	dataDir = f'/Users/rik/data/oc2data/legistar/{runDate}/'
	
	if not pathlib.Path(dataDir).is_dir():
		os.makedirs(dataDir)
	
	dbPath = dataDir + f'legistar4OO.db'
	
	currDB = sqlite.connect(dbPath)

	cacheFile = dataDir + f'eventList.pkl'

	if initializeDB:
		print('initializing DB...')
		initDB(currDB)
		
		addAllMuni(currDB)
		
		for legClient in KnownLegistarClients.keys():
			addBodies(currDB,legClient)

		if useCache:
			if pathlib.Path(cacheFile).is_file():
				with open(cacheFile,'rb') as f:
					eventList = pickle.load(f)
			else:
				getEvents(beginDate,cacheFile,endDate=endDate)
		else:
			eventList = getEvents(beginDate,endDate=endDate)
			
		postEvents(currDB,eventList)
		
		eitemCacheDir = dataDir + 'eitems/'
		# if useCache:
		# 	if not pathlib.Path(eitemCacheDir).is_dir():
		# 		os.makedirs(eitemCacheDir)
		# 	anlyzEventItems(currDB,dataDir,eitemCacheDir)
		# else:					
		# 	anlyzEventItems(currDB,dataDir)

		if useCache:
			if not pathlib.Path(eitemCacheDir).is_dir():
				os.makedirs(eitemCacheDir)
			postAllEventItems(currDB,eitemCacheDir)
		else:					
			postAllEventItems(currDB)
 	
		print('Database loaded:',getDBSize(currDB))
	
	print('Database loaded:',getDBSize(currDB))
	
	pdfDir = dataDir + 'pdfAttach/'
	if not pathlib.Path(pdfDir).is_dir():
		print('creating pdfAttach',pdfDir)
		os.mkdir(pdfDir)
		
	harvestEventAgenda(currDB, CCBodyID, pdfDir)
	harvestAttach(currDB,pdfDir)
	
	sys.exit()
	
	tstAgendaFile = dataDir + 'agenda.txt'
	tstAgendaTxt = open(tstAgendaFile).read()
	agendaInfoList = parseAgenda(tstAgendaTxt)
	
	agendaSummFile = dataDir + 'agendaSumm.csv'
	
	with open(agendaSummFile,'w') as outs:
		outs.write('Item#,Topic,Body\n')
		for ia, ainfo in enumerate(agendaInfoList):
			# {'itemNum': currItemNum, 'topic': topic, 'body': body}
			topic = ainfo['topic']
			if len(ainfo['body']) == 0:
				bodySnip = ''
			else:
				allBody = ' '.join(ainfo['body'])
				bodySnip = allBody[:100] + ' ... ' + allBody[-100:]
			outs.write('%d,"%s","%s"\n' % (ainfo['itemNum'],topic,bodySnip))
	
	
if __name__ == '__main__':
	main()