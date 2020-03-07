#! /usr/bin/env python
#Do not import from other modules if they import this module, ex: uihoard.py
import os
import random
from . import shconfig
import getpass
#from fileutilhoard import idGen
#from fileutilhoard import findBoolOp
from . import fileutilhoard
from . import uihoard
from . import sshcomhoard

SELECTED_DB = shconfig.SELECTED_DB
wack = shconfig.wack

if shconfig.dbType == "sqlite3":
	import sqlite3

#db query text needs to be cleaned up and run through dbQry() and dbQry decides how to parse the arguments
#based on whether sqlite3 or nosql is being used...

def dbQry(a, b):
	#need to clean inputs before implementing mysql connectors.
	results = []
	c = sqlite3.connect(a)
	cursor = c.cursor()
	cursor.execute(b)
	c.commit()
	results = cursor.fetchall()
	c.close()
	return results
					
def initDB():
	if not os.path.isfile(shconfig.SELECTED_DB):
		print("Creating DB...")
		createFileTable()
		createStorageTable()
		createTagTable()
		createChunkStoreLocTable()
		createConfigTable()
		createAutoDirectoryTable()

def createTagTable():
	c = sqlite3.connect(shconfig.SELECTED_DB)
	cursor = c.cursor()
	cursor.execute("create table tags(id integer primary key autoincrement, filename text, tag text)")
	c.commit()
	c.close()
	
def createAutoDirectoryTable():
	c = sqlite3.connect(shconfig.SELECTED_DB)
	cursor = c.cursor()
	cursor.execute("create table autodir(id integer primary key autoincrement, dir text, autoTag text, onoff int)")
	c.commit()
	c.close()	
	
def createConfigTable():
	#program configuration options that are use by shconfig.py, then made global vars during execution
	c = sqlite3.connect(shconfig.SELECTED_DB)
	cursor = c.cursor()
	q = ('create table config('
		'id integer primary key autoincrement, '
		'retrieveWriteDir text, '
		'workingDatabase text, '
		'databasePath text, '
		'dbType text, '
		'createPkey text, '
		'numOfChunkCopies int, '
		'sshCom text, '
		'bytesStored int, '
		'baseBytesOfFiles int'
		')')
	cursor.execute(q)
	c.commit()
	q = ("insert into config (retrieveWriteDir, workingDatabase, dataBasePath, dbType, numOfChunkCopies, sshCom, bytesStored, baseBytesOfFiles) values ('{}', '{}', '{}', '{}', '{}', 'Off', 0, 0);").format('.', shconfig.SELECTED_DB, '.', 'sqlite3', 3)
	cursor.execute(q)
	c.commit()
	c.close()


def updateConfig(setting, value, typeOfValue="string"):
	configDB = ""
	if typeOfValue == "string":
		q = ("update config set {} = '{}' where rowid = 1" ).format(setting, value)
	if typeOfValue == "int":
		q = ("update config set {} = {} where rowid = 1" ).format(setting, value)
	if setting == "workingDatabase":
		configDB = "sctr.db"
	else:
		configDB = shconfig.SELECTED_DB
	r = dbQry(configDB, q)
	return r
	
def incrementStoredBytes(sizeOfBytes):
	r = getConfig()
	bytesSize = r[0][8]
	bytesSize = bytesSize + sizeOfBytes 		
	if sizeOfBytes != 0:
		updateConfig("bytesStored", bytesSize)
	return bytesSize

def incrementFileTotalBytes(sizeOfBytes):
	#total size of data set, not including redundancy or metadata
	#r = getConfig()
	#print r[0]
	#print r[0][9]
	qry1 = ("select fileSize from files").format()
	r = dbQry(shconfig.SELECTED_DB, qry1)
	bytesSize = 0
	for f in r:
		bytesSize = bytesSize + f[0]
	bytesSize = bytesSize + sizeOfBytes
	if sizeOfBytes != 0:
		updateConfig("baseBytesOfFiles", bytesSize)
	return bytesSize
	
def getConfig():
	results = []
	q = "select * from config"
	results = dbQry(shconfig.SELECTED_DB, q)
	#workingDatabase = results[0][2]
	#databasePath = results[0][3]
	#retrieveWriteDir = results[0][1]
	#dbType = results[0][4]
	#numOfChunkCopies = results[0][6]
	#bytesStored = results[0][8]
	#baseBytesOfFiles = results[0][9]
	#sshCom = results[0][7]
	#dbType = results[0][5]
	return results
	
#def logBatch():

def getBatchNum():
	z = 1
	q = "select MAX(batchNum) from files"
	r = dbQry(shconfig.SELECTED_DB, q)
	if (r[0][0] != None) and (r[0][0] != 0) and (r != []):
		z = r[0][0] + 1
	return z
	
#def undoLastBatch():
	
def createFileTable():
	#filehash is the hash of the files data which has been appended with the password. This way you only know the hash if you know the pass of the file
	c = sqlite3.connect(shconfig.SELECTED_DB)
	cursor = c.cursor()
	cursor.execute("create table files(filename text unique, filehash text, batchNum int, fileidnumber integer primary key autoincrement, fileSize int);")
	c.commit()
	c.close()

def createChunkStoreLocTable():
	#stores the location of data chunks for later look up.
	#if not chunks are found in this list, a storage location "walk" will need to be commandline
	#fileLoc is not the raw directory string, but the UID string that is used to find the directory
	#	this is required so that a storage location can move between computers/fielsystems
	#fileChunk is the name of the chunk file that equals the hash listed in fileChunkHash. this might be a chunk from a previously uploaded file if the hashes
	#	happen to match. This allows for deduplication and space saving when making minor alterations to large files that need to be saved, images,
	#	or ISO's that have a better than average chance of having matching data between revisions.
	c = sqlite3.connect(shconfig.SELECTED_DB)
	cursor = c.cursor()
	cursor.execute("create table fileLoc(fileName text, fileChunk text, fileChunkHash text, fileLoc text, acorn integer, duplicateChunk text)")
	c.commit()
	results = cursor.fetchall()
	#print results
	c.close()
	#except:
	#	print "ERROR: failed to access database: " + str(shconfig.SELECTED_DB)
	
def recordChunkLoc(fileName, fileChunk, chunkHash, uid, duplicatedChunk="Nil"):
	fileChunkEscaped = fileutilhoard.quoteScan(fileChunk)
	if duplicatedChunk != "Nil":
		duplicatedChunk = fileutilhoard.quoteScan(duplicatedChunk)
	fileNameEscaped = fileutilhoard.quoteScan(fileName)
	qry1 = ("select * from fileLoc "
			"where fileChunk = '{}' "
			"and fileChunkHash = '{}' "
			"and fileLoc = '{}';").format(fileChunkEscaped, chunkHash, uid)
	r1 = dbQry(shconfig.SELECTED_DB, qry1)
	if r1 == []:
		directory, sshhost = getDirectoryStorage(uid)
		fullPath = directory + wack + fileChunk
		if duplicatedChunk == "Nil":
			if sshhost not in ["", "Nil"]:
				exists, fileSize = sshcomhoard.doesRemoteFileExist(directory, sshhost, fileChunk)
			else:
				fileSize = os.stat(fullPath).st_size
			incrementStoredBytes(fileSize)
		qry2 = ("insert into fileLoc "
				"(fileName, fileChunk, fileChunkHash, fileLoc, acorn, duplicateChunk) "
				"values('{}','{}','{}','{}', 0, '{}');").format(fileNameEscaped,fileChunkEscaped,chunkHash,uid,duplicatedChunk)
		r2 = dbQry(shconfig.SELECTED_DB, qry2)
		return 1
	else:
		print(("Attempted to record chunk that was already recorded, ignoring: " + fileChunk + " at " + uid))
		return 0
	

def removeChunkLoc(fileChunk, uid):
	fileChunk = fileutilhoard.quoteScan(fileChunk)
	qry1 = ("delete from fileLoc where fileChunk = '{}' AND fileLoc = '{}'").format(fileChunk, uid)
	r = dbQry(shconfig.SELECTED_DB, qry1)
	return r

def removeFileFromFilesTable(file):
	file = fileutilhoard.quoteScan(file)
	q = ("delete from files where filename = '{}'").format(file)
	r = dbQry(shconfig.SELECTED_DB, q)
	q2 = ("delete from tags where filename = '{}'").format(file)
	r2 = dbQry(shconfig.SELECTED_DB, q2)
	if (r != []) and (r2 != []):
		return 1
	else:
		return 0

def dumpFileList():
	qry1 = "select * from files"
	#qry1 = "SELECT name FROM sqlite_master WHERE type='table';"
	result = dbQry(shconfig.SELECTED_DB, qry1)
	#count = 0
	#total = len(result)
	#for r in result:
	#	#print "# " + str(r[0]) + " # " + str(r[1]) + " # " + str(r[2]) + " #"
	#	#fileInfo = getFileInfo(f)
	#	fid =  str(r[3])
	#	batchNum = str(r[2])
	#	fileSize = str(r[4])
	#	fileSizeMB = str(round(r[4]/1048576)) + " MB)"
	#	#print(("File ID#: " + str(fid) + " , FileSize: " + str(fileSize) + " ( ~" + str(round(fileSize/1048576)) + " MB) " + " , Batch#: " + str(batchNum) + " , File Name: " + r[0] + " , File Hash: " + r[1])) 
	#	uihoard.underBox(" " + str(r[0]) + " ", space=3, end=False, nextLine=False)
	#	uihoard.underBox("   File ID: " + fid + " , File Size: " + fileSize + "(~" + fileSizeMB + " , Batch Number: " + batchNum, space=3, end=False, nextLine=True)
	#	uihoard.underBox("   File Hash: " + r[1], space=3, end=False, nextLine=True)
	#uihoard.underBox("   End of files", space=3, end=True)
	return result
	


def dumpChunkList():
	qry1 = "select fileChunk, fileLoc, fileChunkHash, duplicateChunk, rowid, acorn from fileLoc order by rowid;"
	result = dbQry(shconfig.SELECTED_DB, qry1)
	uihoard.inBox("File chunks stored", bottomConnection=3)
	for r in result:
		rowid = str(r[4])
		chunkName = r[0]
		storageLocation = r[1]
		chunkHash = r[2]
		duplicate = r[3]
		acorn = str(r[5])
		uihoard.underBox(" " + chunkName + " ", space=3, end=False, nextLine=False)
		uihoard.underBox("   Row ID: " + rowid + " , Storage Location: " + storageLocation + " , Acorn Count: " + acorn, space=3, end=False, nextLine=True)
		uihoard.underBox("   Chunk Hash: " + chunkHash, space=3, end=False, nextLine=True)
		if duplicate != "Nil":
			uihoard.underBox("   Chunk is duplicate of: " + duplicate, space=3, end=False, nextLine=True)
	uihoard.underBox("   End of chunks", space=3, end=True)	
	
def getRandomChunk(getAllFields=False, getDuplicates=False):
	loopCounter = 0
	selectedChunk = False
	qry1 = ("select MAX(rowid) from fileLoc;")
	highestID = dbQry(shconfig.SELECTED_DB, qry1)
	if highestID[0][0] == 0:
		return 1
	while selectedChunk == False:
		loopCounter = loopCounter + 1
		randomID = random.randint(0, int(highestID[0][0]))
		if getDuplicates == False:
			if getAllFields == False:
				qry2 = ("select fileChunk, fileLoc, acorn from fileLoc where rowid = {} and duplicateChunk = 'Nil';").format(randomID)
			else:
				qry2 = ("select * from fileLoc where rowid = {} and duplicateChunk = 'Nil';").format(randomID)
		else:
			if getAllFields == False:
				qry2 = ("select fileChunk, fileLoc, acorn from fileLoc where rowid = {};").format(randomID)
			else:
				qry2 = ("select * from fileLoc where rowid = {};").format(randomID)
		r2 = dbQry(shconfig.SELECTED_DB, qry2)
		if r2 != []:
			selectedChunk = True
			if getAllFields == False:
				return r2[0][0], r2[0][1], r2[0][2]
			else:
				return r2
			
def getNumberOfRows(table):
	qry1 = ("SELECT Count(*) FROM {};").format(table)
	if table == "fileLoc":
		qry1 = ("SELECT Count(*) FROM {} where duplicateChunk = 'Nil';").format(table)
	r = dbQry(shconfig.SELECTED_DB, qry1)
	#print(("Count rows not nil" + str(r[0][0])))
	return r[0][0]
		
def getChunkInfo(chunkName, UID):
	q = ("select * from fileLoc where fileChunk = '{}' and fileLoc = '{}' limit 1;").format(chunkName, UID)
	r = dbQry(shconfig.SELECTED_DB, q)
	return r		
			
def getChunkList(fileName, getAllFields=False):
	#retrieves the fileLoc data for ALL chunks of a file
	fileName = fileutilhoard.quoteScan(fileName)
	qry = ("select * from fileLoc "
		"where fileName = '{}'").format(fileName)
	r = dbQry(shconfig.SELECTED_DB, qry)
	fileChunkList = []
	highest = 0
	lowest = 0
	current = 0
	if getAllFields == False:
		for line in r:
			chunkMetaData = []
			chunkName = line[1]
			chunkHash = line[2]
			chunkLoc = line[3]
			duplicateChunk = line[5]
			fileName = line[0]
			chunkMetaData.append(chunkName)
			chunkMetaData.append(chunkHash)
			chunkMetaData.append(chunkLoc)
			chunkMetaData.append(duplicateChunk)
			chunkMetaData.append(fileName)
			fileChunkList.append(chunkMetaData)
		return fileChunkList
	else:
		return r
		
def getChunkShortList(fileChunk, fileChunkLoc=""):
	#retrieves the fileLoc data for ONE chunk in a file
	fileChunk = fileutilhoard.quoteScan(fileChunk)
	if fileChunkLoc == "":
		qry = ("select * from fileLoc where fileChunk = '{}'").format(fileChunk)
	else:
		qry = ("select * from fileLoc where fileChunk = '{}' and fileLoc = '{}' ").format(fileChunk, fileChunkLoc)
	r = dbQry(shconfig.SELECTED_DB, qry)
	return r
	
	
def getFileInfo(fileName):
	fileName = fileutilhoard.quoteScan(fileName)
	qry = ("select * from files "
		"where filename = '{}' limit 1").format(fileName)
	r = dbQry(shconfig.SELECTED_DB, qry)
	if r != []:
		#return r[0][1] #fileHash
		return r
	else:
		return 1
	
def getStorageLocUID(path, sshhost="Nil"):
	path = fileutilhoard.quoteScan(path)
	if sshhost in ["Nil",""]:
		qry = ("select uid from storage "
			"where directory ='{}'"
			" and sshhost='Nil';").format(path)
	else:
		qry =("select uid from storage"
			"where directory ='{}'"
			" and sshhost = '{}'").format(path,sshhost)
	r = dbQry(shconfig.SELECTED_DB, qry)
	return r[0]

def getFileExistsByHash(hash):
	q = ("select 1 from files where filehash = '{}'").format(hash)
	r = dbQry(shconfig.SELECTED_DB, q)
	if r == []:
		return 0
	else:
		return 1
		
def getFileNameLike(searchString):
	fileNames = []
	q = ("select filename from files where filename LIKE '%{}%'").format(str(searchString))
	results = dbQry(shconfig.SELECTED_DB, q)
	for r in results:
		fileNames.append(r[0])
	return fileNames

def getHashLike(searchString):
	hashes = []
	q = ("select filename from files where filehash LIKE '%{}%'").format(str(searchString))
	results = dbQry(shconfig.SELECTED_DB, q)
	for r in results:
		hashes.append(r[0])
	return hashes
		
def getFileNameFromID(getID):
	q =  ("select filename from files where fileidnumber = {} limit 1;").format(getID)
	r = dbQry(shconfig.SELECTED_DB, q)
	if r == []:
		return "Nil"
	else:
		#print r[0][0]
		return r[0][0] #filename
		
def getIDFromFileName(fileName):
	fileName = fileutilhoard.quoteScan(fileName)
	q =  ("select fileidnumber from files where filename = '{}' limit 1;").format(fileName)
	r = dbQry(shconfig.SELECTED_DB, q)
	if r == []:
		return "Nil"
	else:
		#print str(r[0][0])
		return r[0][0]
	
def doesChunkHashExist(chunkHash, fileName):
	#only returns information about real chunks, chunk info about duplicates are ignored
	fileName = fileutilhoard.quoteScan(fileName)
	q = ("select * from fileLoc where fileChunkHash = '{}'and duplicateChunk = 'Nil' and not fileName = '{}'").format(chunkHash, fileName)
	r = dbQry(shconfig.SELECTED_DB, q)
	if r == []:
		return "Nil"
	else:
		return r
		
def createStorageTable():
	c = sqlite3.connect(shconfig.SELECTED_DB)
	cursor = c.cursor()
	cursor.execute("create table storage(id integer primary key autoincrement, directory text, uid text, sshhost text, sshuser text, sshport int, readonly int)")
	c.commit()
	c.close()

def createSSHCredTable():
	#For Windows or other OS that don't have ~/.ssh
	#THis might be a security issue, maybe they should just prime the connections each use with username/password.
	c = sqlite3.connect(shconfig.SELECTED_DB)
	cursor = c.cursor()
	cursor.execute("create table sshCreds(id integer primary key autoincrement, sshhost text, pkey text )")
	c.commit()
	c.close()

def getDirectoryStorage(uid):
	qry1 = ("select directory, sshhost from storage where uid = '{}' limit 1;").format(uid)
	r = dbQry(shconfig.SELECTED_DB, qry1)
	if r != []:
		return r[0][0], r[0][1]
	else:
		return "Nil", "Nil"
	
def getUIDStorage(directory):
	directory = fileutilhoard.quoteScan(directory)
	qry1 = ("select uid from storage where directory = '{}' limit 1;").format(directory)
	r = dbQry(shconfig.SELECTED_DB, qry1)
	return r[0][0]

def doesTableExist(db, table):
    c = sqlite3.connect(db)
    cursor = c.cursor()
    cursor.execute("SELECT 1 FROM sqlite_master WHERE name ='" + table + "' and type='table';")
    c.commit()
    r = cursor.fetchall()
    if not r:
        return 0
    else:
        return 1

def isChunkRecorded(chunkName, fileLoc):
	qry2 = ("select * from fileLoc where fileChunk = '{}' and fileLoc = '{}' and duplicateChunk = 'Nil' limit 1").format(chunkName, fileLoc)
	r2 = dbQry(shconfig.SELECTED_DB, qry2)
	if r2 == []:
		return False
	else:
		return True
	
def readTag(tag):
	qry1 = "select * from tags where tag = '" + tag + "'"
	r = dbQry(shconfig.SELECTED_DB, qry1)
	return r

def readAllTags():
	qry1 = "select * from tags"
	r = dbQry(shconfig.SELECTED_DB, qry1)
	return r

def addTag(filename, tag, silent="n"):
	filename = fileutilhoard.quoteScan(filename)
	#if silent == "n":
	#	print(("Added tag: " + filename + ", " + tag))
	qry1 = "select * from files where filename = '" + str(filename) + "' limit 1"
	r = dbQry(shconfig.SELECTED_DB, qry1)
	q2 = ("select filename, tag from tags where filename = '{}' and tag = '{}'").format(filename, tag)
	r2 = dbQry(shconfig.SELECTED_DB, q2)
	if r2 != []:
		print("Tag (" +  tag + ") already exists for file " + filename)
		return 1
	if r == []:
		print("this file doesn't exist: " + str(filename))
		return 1
	else:
		qry2 = "insert into tags (filename, tag) values ('" + str(filename) + "', '" + str(tag) + "')"
		dbQry(shconfig.SELECTED_DB, qry2)
		return 0
		
def getLoginSSH(sshhost, directory):
	directory = fileutilhoard.quoteScan(directory)
	q = ("select sshuser, sshport from storage where sshhost = '{}' and directory = '{}'").format(sshhost, directory)
	r = dbQry(shconfig.SELECTED_DB, q)
	if r != []:
		#return username and port
		return r[0][0], r[0][1]
	else:
		return "Nil", 0
		
def removeFileFromDB(file, onlineStorage):
	print(("removing file from DB: " + file))
	#onlineStorage = backuphoard.initStorage(justDir=False, addReadOnly=True)
	chunkList = getChunkList(file, getAllFields=True)
	dir = ""
	move = 0
	for c in chunkList:
		for d in onlineStorage:
			if d[1] == c[3]:
				dir = d[0]
		if dir != "":
			path = dir + wack + c[1]
			move = fileutilhoard.moveToTrash(path)
			if move == 0:
				removeChunkLoc(c[1], c[3])
	removeFileFromFilesTable(file)
	
def setReadOnlyStorage(uid, onlineStorage=[], readOnly=1):
	if readOnly == 1:
		q = ("update storage set readonly = 1 where uid = '{}'").format(uid)	
		if onlineStorage != []:
			onlineStorage.remove(uid)
	else: 
		q = ("update storage set readonly = 0 where uid = '{}'").format(uid)
	r = dbQry(shconfig.SELECTED_DB, q)
	return r
	
def showNoHash():
	q = "select * from files where filehash = ''"
	r = dbQry(shconfig.SELECTED_DB, q)
	return r
	
def addStorageLocation(directory, sshhost="Nil", username="Nil", sshport=22):
	uid = fileutilhoard.idGen(40)
	directory = fileutilhoard.quoteScan(directory)
	if fileutilhoard.isLastCharWack(directory):
		directory = directory[:len(directory) -1] + directory[len(directory):]
	if sshhost == "Nil":
		if not os.path.exists(directory):
			print("Directory does not exist")
			return 
		if os.path.isfile(directory + shconfig.wack + "uid.txt"):
			try:
				with open(directory + shconfig.wack + "uid.txt",'r') as f:
					uid = f.read()
					f.close()
			except IOError as e:
				print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		else:
			try:
				with open(directory + shconfig.wack + "uid.txt",'w') as f:
					f.write(uid)
					f.close()
			except IOError as e:
				print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		#write code here to check and see if the directory path is already in the storage table...
	else:
		if username == "Nil":
			#sshuser = getpass.getuser()
			username = eval(input('\nPlease enter username for accessing SSH storage location ' + sshhost + ':'))
		dirExists = sshcomhoard.doesRemoteFileExist(directory, sshhost, "Nil", dirOnly=True, sshuser=username)
		if dirExists == False:
			print("Directory at remote host does not exist.")
			return
		if sshcomhoard.doesRemoteFileExist(directory, sshhost, "uid.txt", sshuser=username):
			try:
				fo = sshcomhoard.getChunkSFTP(directory, sshhost, "uid.txt")
				fo.seek(0)
				uid = fo.read()
				uid = uid.decode()
				fo.close()
			except IOError as e:
				print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
				return
		else:
			try:
				sshcomhoard.putTextSFTP(directory, sshhost, "uid.txt", uid)
			except IOError as e:
				print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
				return
	alreadyExists = 0
	qry1 = "select directory, sshhost from storage"
	r1 = dbQry(shconfig.SELECTED_DB, qry1)
	for line in r1:
		if (directory == line[0]) and (sshhost == line[1]):
			alreadyExists = 1
	if alreadyExists == 0:
		qry2 = "insert into storage (directory, uid, sshhost, sshuser, sshport, readonly) values ('" + directory + "', '" + uid + "', '" + sshhost + "', '" + username + "', '" + str(sshport) + "', 0)"
		r2 = dbQry(shconfig.SELECTED_DB, qry2)
		if str(r2) != "[]":
			pass
		return 0
	else:
		qry2 = "update storage set uid = '" + uid + "' where directory = '" + directory + "' and sshhost = '" + sshhost + "';"
		r2 = dbQry(shconfig.SELECTED_DB, qry2)
		print("Storage location already exists. Updated UID in storage DB")
	return 1

def delRow(rowid):
	qry = ("DELETE from files where fileidnumber = {}").format(rowid)
	r = dbQry(shconfig.SELECTED_DB, qry)
	return r

def getAllFromBatch(batch):
	q = ("select filename from files where batchNum = {}").format(str(batch))
	r = dbQry(shconfig.SELECTED_DB, q)
	resultArray = []
	if r != []:
		for r2 in r:
			resultArray.append(r2[0])
	return resultArray


	
