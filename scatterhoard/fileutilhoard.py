#!/usr/bin/python
import string
import random
import hashlib
import os
from . import uihoard
from . import backuphoard
from . import shconfig
import hashlib
from . import dbhoard
from . import sshcomhoard
import tarfile
from io import BytesIO

command = shconfig.command
wack = shconfig.wack

def getExt(file):
	splitFilename = file.split(".")
	extLen = len(splitFilename)
	extension = splitFilename[extLen - 1]
	return extension

	
def checkFileNameChar(string, option=0, allowed=""):
	badChar = 0
	onlySpace = 1
	#options: 1 = plus space and .
	string = quoteScan(string)
	#if type(string) != type("str"):
	#	return 1, 1
	if not isinstance(string, str):
		return 1, 1
	for char in string:
		if char in ["\"", "/", "\\", "%", "?", "*", ":", "|", "<", ">"]:
			badChar = 1
			#, "\'"
		if option == 1:
			if char in [".", " "]:
				badChar = 1
		if (char != " ") and (char != ""):
			onlySpace = 0
	return badChar, onlySpace
	
def quoteScan(string):
	if not isinstance(string, str):
		string = str(string)
		newString = ""
	else:
		newString = ""
	#used for escaping single quotes for sqlite queries.
	#adds an addition ' to the string
	for s in string:
		newString = newString + s
		if s in ["\'", "\""]:
			newString = newString + s
	return newString
	
def checkIfBatch(fileName):
	checkIfBatch = fileName[0:10]
	if checkIfBatch == "<<< BATCH ":
		fileName = fileName.split("<<< BATCH ")
		fileName = str(fileName[1])
		type(fileName)
		fileName = fileName.split(" >>>")
		return fileName[0]
	else:
		return 0
	
def getChunkNumber(fileName):
	try:
		splitFileName =  fileName.split(".")
		sLen =  len(splitFileName)
		if splitFileName[sLen - 1] == "dat":
			chunkNumbers = splitFileName[sLen - 2]
			splitChunkNumbers = chunkNumbers.split("_")
			totalChunks = splitChunkNumbers[1]
			thisChunk = splitChunkNumbers[0]
		elif splitFileName[sLen - 1] == "dup":
			return "dup", 1
		else:
			return "nil", 0
	except:
		print("Error getting chunk info")
		return "nil", 0
	return thisChunk, int(totalChunks)

def isWholeFileAvailable(chunkList, onlineStorage, lastChunkNum):
	#[['laptopbackup1.7z.27_46.dat', '51595e1b2db13cdf699ee5225b05ec322b11c011', '5QV1L8IBWWWCFRD27SEN2MF5VB5UWGM4DJUH0KNE', 'Nil', 'laptopbackup1.7z']]
	#example structure of chunkList with one element. multidimensional array.
	fileName = getFileNameFromDat(chunkList[0][0])
	availableChunks = []
	unavailableChunks = []
	CWDChunks = []
	wholeFileAvailable = True
	for r in range(1, lastChunkNum + 1): #range doesn't include last number
		chunkAvailable = False
		chunkInCWD = 2
		currentChunk = fileName + "." + str(r) + "_" + str(lastChunkNum) + ".dat"
		for c in chunkList:
			if (currentChunk == c[0]): #c[0] is chunkName
				for o in onlineStorage:
					if c[2] == o[1]:
						availableChunks.append(c)
						chunkAvailable = True
						break #end for loop, move to next r in range
			if chunkAvailable == True: break
				#put code here to allow healthcheck of this chunk to make sure there are 3 redundant chunks?
		if chunkAvailable == False:
			chunkInCWD = doesFileExist("." + shconfig.wack + currentChunk, sshhost="Nil", getFileSize=False, ignoreError=True)
		#put code here to allow searching for chunks in acorn tar files in CWD as well
		if chunkInCWD == 0:
			unavailableChunks.append(currentChunk)
			wholeFileAvailable = False
		if chunkInCWD == 1:
			CWDChunks.append(currentChunk)
	if wholeFileAvailable == False:
		return False, availableChunks, unavailableChunks, CWDChunks
	return True, [], [], CWDChunks
	
def moveToTrash(chunk, sshhost="Nil"):
	dir = os.path.dirname(chunk)
	chunkName = os.path.basename(chunk)
	trash = dir + wack + "trash"
	print(("Attempting to move chunk to trash: " + chunk))
	if sshhost in ["","Nil"]:
		if not os.path.isdir(trash):
			try:
				os.mkdir(trash)
			except OSError as e:
				print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		if not os.path.isfile(chunk):
			print(("Chunk missing, marking as a successfull purge anyways... : " + str(chunk)))
			return 0
		try:
			os.rename(chunk, trash + wack + chunkName)
			print(("Moved to trash: " +  trash + wack + chunkName))
			return 0
		except OSError as e:
			print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
			print(("Error while deleting file: " + str(file)))
			return 1
	else:
		return sshcomhoard.remoteMoveToTrashSFTP(dir, sshhost, chunkName, trash)
	
def getFileNameFromDat(datFile):
	splitDatFile = datFile.split(".")
	length = len(splitDatFile)
	stop = 0
	seq = 0
	fileName = ""
	if splitDatFile[length - 1] == "dat":
		for s in splitDatFile:
			seq = seq + 1
			if s == "dat":
				stop = seq - 2
				break
		seq = 0
		for s in splitDatFile:
			if seq < stop:
				fileName = fileName + s
			if seq < (stop - 1):
				fileName = fileName + "."
			seq = seq + 1
	return fileName

def idGen(size=6, chars=string.ascii_uppercase + string.digits):
	return ''.join(random.choice(chars) for _ in range(size))

def delFile(file, verbose="yes", sshhost="Nil"):
	if sshhost in ["","Nil"]:
		try:
			os.remove(file)
			return 0
		except OSError as e:
			if verbose == "yes":
				print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
				print(("Error while deleting file: " + str(file)))
			return 1
			pass
	else:
		return sshcomhoard.delFileSFTP(file, sshhost)

def safeReadFile(a, c):
	#read and load twice into RAM.
	#bit flips in RAM won't produce the same hash
	#ensures hash is correct.
	#returns file object required for chunkUp() and chunkFileSize. Last chunk will usually be smaller than 100MB
	c2 =  c * 104857600
	try:
		with open(a,'rb') as f1:
			f1.seek(c * 104857600)
			buf1 = f1.read(104857600)
			f4 = BytesIO(buf1)
			f4.seek(0)
			f4hash, f4hex = chunkHasherFO(f4)
			f4total = f4.seek(0,2)
	except IOError as e:
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
	try:
		with open(a,'rb') as f2:
			f2.seek(c * 104857600)
			buf2 = f2.read(104857600)
			f5 = BytesIO(buf2)
			f5.seek(0)
			f5hash, f5hex = chunkHasherFO(f5)
			f5.close()
	except IOError as e:
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
	if(f5hex != f4hex):
		f4.close()
		exit("Error while reading file. Hashes are not matching while reading from local file system.")
	chunkfilesize = f4.tell()
	f1.close()
	f2.close()
	return str(chunkfilesize), f4

def hashFile(file, firstChunk=0):
	hash1 = hashlib.sha1()
	stri = "1"
	try:
		finfo = os.stat(file)
	except OSError as e:
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		print("error in hashFile()")
		print(file)
		return "1", "empty"
	if os.path.isfile(file) != True:
		print("\nFile does not exist!\n")
		return "0", "empty"
	filesize1 = finfo.st_size
	if filesize1 == 0:
		print("\nFile is empty.")
		return "0", "empty"
	try:
		fp = open(file, 'rb')
		fp.seek(0)
		counter1 = 0
		while stri != b'':
			stri = fp.read(10485760)
			hash1.update(stri)
			counter1 = counter1 + 10485760
			if int(counter1) >= int(filesize1):
				counter1 = filesize1
			uihoard.printInPlace( "Hashing entire file... " + str(int((float(counter1) / float(filesize1)) * 100)) + "% done." )
			if backuphoard.backup.stopRequest == 1: 
				print("Process halted during hashfile")
				return "process halted"
			if firstChunk == 1: break
		fp.close()
	except OSError as e:
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		#this return value sucks, but I want to keep the type string...
		return "1"
	return hash1.digest(), hash1.hexdigest()

def doesFileExist(fileName, sshhost="Nil", getFileSize=False, ignoreError=False):
	if sshhost in ["Nil",""]:
		if os.path.isfile(fileName) != True:
			if ignoreError == False:
				print(("File missing: " + fileName))
			return 0
	else:
		path = os.path.dirname(fileName)
		target = os.path.basename(fileName)
		exists, fileSize = sshcomhoard.doesRemoteFileExist(path, sshhost, target,ignoreError=ignoreError)
		if getFileSize == False:
			return exists
		else:
			return exists, fileSize
	return 1

def findNumberOfChunks(a):
	#This froze the program when reading the file was too slow. 
	c = 0
	try:
		with open(a, 'rb') as f:
			f.seek(0,2)
			size = f.tell()
			c = size / 104857600
			c2 = size % 104857600
			if (size % 104857600) != 0:
				c = int(c) + 1
			f.close()
	except IOError as e:
		print("findNumberOfChunks():")
		print((str(a)))
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
	return c


def exitOrReturn(exitMessage="Exiting...", returnValue=1):
	#If this program is running in interactive command line (with command[] main loop)
	#then it will not exit, but return to the main loop.
	#Otherwise it will exit
	if shconfig.command == True:
		sys.exit(exitMessage)
	else:
		return returnValue

def isLastCharWack(string):
	stringLen = len(string)
	lastChar = string[stringLen - 1]
	if lastChar == wack:
		return 1
	else:
		return 0
		

def readMetaDataFromDat(datFilePath, sshhost="Nil"):
	hashDataOfFile = "cannot read file"
	try:
		if sshhost in ["","Nil"]:
			with open(datFilePath, 'rb') as f:
				f.seek(-40, 2)
				hashDataOfFile = f.read()
				f.close()
		else:
			fileName = os.path.basename(datFilePath)
			dir = os.path.dirname(datFilePath)
			hashDataOfFile = sshcomhoard.getChunkMetaDataSFTP(dir, sshhost, fileName)
	except IOError as e:
		print("ERROR READING FROM DAT FILE")
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
	return hashDataOfFile

def writeDup(hashOfOriginalChunk, dir, chunkName, duplicateOf, storeLocOfDir, sshhost="Nil"):
	#duplicateOf should not contain directory of path, just filename
	#chunkPath will have directory
	#dup's record the original hash of the chunk they are a dupe of in the first 20byte slot then the
	#	hash of the dupe data itself in the last 20 byte slot at the end of the dupe file.
	#	(payload = duplicateOf)(20byte hashOfOriginalChunk)(20byte dupHash)
	#dupe's are used when recovering from lost .db files. Cataloguing functions will identify the dup files and 
	#	record their info in the fileLoc table using readDup()
	#TODO: a simple table of common hashes  that will be encountered for things like empty files (an indicator of a bad write somewhere)
	#	simple rules should be in place  for encountering each of these hashes...
	chunkPath = dir + wack + chunkName
	dupFileName = chunkName + ".dup"
	if sshhost in ["", "Nil"]:
		try:
			with open(chunkPath + ".dup", 'w') as f:
				f.write(duplicateOf)
				f.close()
		except IOError as e:
			return 1
		dupHash, hexHashDup = hashFile(chunkPath + ".dup")
		try:
			with open(chunkPath + ".dup", 'ab') as f2:
				f2.write(hashOfOriginalChunk + dupHash)
				f2.close()
		except IOError as e:
				return 1
	else:
		try:
			fo = BytesIO()
			fo.write(duplicateOf)
			fo.seek(0)
			dupHash, hexHashDup = chunkHasherFO(fo)
			fo.seek(0, 2)
			fo.write(hashOfOriginalChunk + dupHash)
			fo.seek(0)
			sshcomhoard.putChunkSFTP(path, sshhost, target, fo)
			fo.close()
		except IOError as e:
			print(e)
			return 1
	dbhoard.recordChunkLoc(dupFileName, dupFileName, hexHashDup, storeLocOfDir, duplicatedChunk="Nil")	
	print(("Created .dup file: " + chunkPath + ".dup"))
	return 0
	
def readDup(dupFile, ssh="Nil"):
	if ssh in ["Nil",""]:
		finfo = os.stat(dupFile)
		counter1 = 0
		fileName3 = ""
		originalFileName = ""
		filesize1 = finfo.st_size
		fileName1 = os.path.basename(dupFile)
		dupFile2 = fileName1
		dir = os.path.dirname(dupFile)
		dir2 = dbhoard.getStorageLocUID(dir)
		fileName1 = fileName1.split('.')
	else:
		dir = os.path.dirname(dupFile)
		dir2 = dbhoard.getStorageLocUID(dir)
		fileName1 = os.path.basename(dupFile)
		fileExists, filesize1 = sshcomhoard.doesRemoteFileExist(dir, ssh, fileName1)
		dupFile2 = fileName1
		fileName1 = fileName1.split('.')
	for f in fileName1:
		if f != "dat":
			fileName3 = fileName3 + f + "."
		else:
			fileName3 = fileName3 + "dat"
			break
	originalFileName = getFileNameFromDat(fileName3)
	try:
		if ssh in ["","Nil"]:
			try:
				with open(dupFile, 'rb') as f:
					print(dupFile)
					dupName = f.read(filesize1 - 40)
					dupName = dupName.decode()
					hashOfOriginalChunk = f.read(20)
					hashOfDupFile = f.read(20)
				dupHashed, metaHash, hexHash = datHasher(dupFile)
				f.close()
			except Exception as e:
				print(e)
				return 1
		else:
			dupName, hashOfOriginalChunk, hashOfDupeFile, dupHashed = sshcomhoard.getRemoteDupDataSFTP(dir, ssh, dupFile2, filesize1)
		if dupHashed != hashOfDupFile:
			#move dup to trash and find another copy of the dupFile here
			moveToTrash(dupFile, sshhost=ssh)
			return 1
		else:
			q = ("select fileLoc from fileLoc where fileChunk = '{}';").format(quoteScan(dupName))
			r = dbhoard.dbQry(shconfig.SELECTED_DB, q)
			if r != []:
				for c in r:
					#c[0] is key here. It is not the location of the dup file itself, it is the location of the chunk the dup file refers to
					dbhoard.recordChunkLoc(originalFileName, fileName3, bytes.hex(hashOfOriginalChunk), c[0], duplicatedChunk=dupName)
					dbhoard.recordChunkLoc(dupFile2, dupFile2, bytes.hex(hashOfDupFile), dir2[0], "Nil")
					print(("ADDED DUPE META DATA TO DB: " + dupFile))
			else:
				#this result is common when a chunk is just empty data... this will cause problems retrieving chunks
				#dbhoard.recordChunkLoc(originalFileName, fileName3, binascii.hexlify(hashOfOriginalChunk), "Nil", duplicatedChunk=dupName)
				print(("CHUNK NOT FOUND: Unable to find metadata for " + dupName))
				print("Please run the catalogue command across all storage locations and then proceed with the scrub command")
				return 1
		f.close()
	except OSError as e:
		print(e)
		return 1
	
def writeDat(hashOfFile, hashOfChunk, chunkName2, chunkFO):
	success = 0
	try:
		with open(chunkName2,'wb') as f:
			chunkFO.seek(0)
			f.write(chunkFO.read())
			f.write(hashOfFile + hashOfChunk)
			f.close()
			print(("Created dat file: " + chunkName2))
	except IOError as e:
		print(("ERROR WRITING TO DAT FILE " + str(chunkName2)))
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		if os.path.isfile(chunkName2):
			delFile(chunkName2)
		return 1
	try:
		hashReadFromData, metaHash, hexHash = datHasher(chunkName2)
		if hashOfChunk != hashReadFromData:
			print(hashReadFromData)
			print(("Hash that was written:  " + str(bytes.hex(hashReadFromData))))
			print(("Hash that was supplied: " + str(bytes.hex(hashOfChunk))))
			print("File Writing error detected. Deleting and rewriting file chunks...")
			delFile(chunkName2)
			return 1
		else:
			#success
			return 0
	except IOError as e:
		print("ERROR, could not confirm write was successful, will retry on a different storage location") 
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		return 1

def datHasher(filePath, fo=None, countBytes=False):
	#If a remote ssh chunk needs to be hashed, pass the fo as returned from getChunkSFTP() 
	try:
		fileSize = 0
		hashchunk = hashlib.sha1()
		if fo == None:
			fileSize = os.stat(filePath).st_size
			f = open(filePath, 'rb')
		else:
			f = fo
			f.seek(0,2)
			fileSize = f.tell()
			f.seek(0)
		f2 = f.read(fileSize - 40)
		metaHash = f.read(40)
		hashchunk.update(f2)
		hash = hashchunk.digest()
		hexHash = hashchunk.hexdigest()
		#if fo != None:
		f.close()
	except KeyboardInterrupt:
		print("\nPROCESS HALTED BY USER\n")
		if countBytes == False:
			return 1,1,"interrupted"
		else:
			return 1,1,"interrupted",1
	except:
		outputLongT = """
		 ERROR, could not read from chunk file.
		 I/O error({0}): {1}".format(e.errno, e.strerror)
		"""
		outputLongT = outputLongT + " " + str(filePath)
		print(outputLongT)
		if countBytes == False:
			return 1, 1, 1
		else:
			return 1,1,1,1
	if countBytes == False:
		return hash, metaHash, hexHash
	else:
		return hash, metaHash, fileSize, hexHash

def chunkHasher(filename, c=0):
	#this is one of two primary hashing functions for scatter-hoard
	#it reads from the file path supplied  in 104857600 byte chunks and returns the hash. this is how file corruption is identified 
	#during long term storage. The hashes are appended to file chunks as meta data and stored in the .db file. A 104857600 byte  chunk
	#must always have its hash match this meta data or there is something seriously wrong.
	#this also checksums the file being "retrieved" while its being retrieved
	#c is seek location in bytes, default to 0
	#read twice while keeping variables alive to identify reading/memory problems. Helps prevent corrupted reads and data
	#bit flips in RAM during hashing may still corrupt data if ECC RAM is not used.
	#caution should be used on non server hardware...
	try:
		with open(filename, 'rb') as f:
			hashChunk = hashlib.sha1()
			f.seek(c * 104857600)
			f2 = f.read(104857600)
			hashChunk.update(f2)
			hash = hashChunk.digest()
			hexHash = hashChunk.hexdigest()
			with open(filename, 'rb') as ff:
				hashChunk2 = hashlib.sha1()
				ff.seek(c * 104857600)
				ff2 = ff.read(104857600)
				hashChunk2.update(f2)
				hash2 = hashChunk2.digest()
				if hash != hash2:
					exitOrReturn()
				ff.close()
			f.close()
		return hash, hexHash
	except IOError as e:
		print(("ERROR READING FILE: " + str(filename)))
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		exitOrReturn()
			
def chunkHasherFO(fileobject):
	#works the same as chunkHasher() but accepts python file objections instead of file paths to the local
	#file system. This is used in the  safeRead function that is  used to initially read a file provided by user.
	#this does not hash dat files that have metadata already appended!
	hashchunk = hashlib.sha1()
	fileobject.seek(0)
	f2 = fileobject.read(104857600)
	hashchunk.update(f2)
	hash = hashchunk.digest()
	hexHash = hashchunk.hexdigest()
	return hash, hexHash
	
def healthCheckSingleChunk(filePath, fileLoc, sshhost="Nil", batch=0, quiet=False):
	#hashes the chunk/.dat file that is stored and compares the resulting hash with the hash that is stored in meta data
	#to determine if the chunk has been corrupted/changed in the file system. If a change/corruption has been identified
	#the chunk is deleted from the file system and reference to this chunk is removed from the database.
	#recreating this bad chunk to meet the 3 chunk redundancy minimum is done outside off this function and is triggered by
	# the return value of 0, returns 1 on success 
	chunk = filePath.split(wack)
	justDir = os.path.dirname(filePath)
	chunk = chunk[len(chunk) - 1]
	originalFileName = getFileNameFromDat(chunk)
	hashsHere = readMetaDataFromDat(filePath, sshhost)
	
	if sshhost in ["", "Nil"]:
		fSize = os.path.getsize(filePath)
		hash1, metaHash, hexHash1 = datHasher(filePath)
	else:
		exists, fSize = sshcomhoard.doesRemoteFileExist(justDir, sshhost, chunk, dirOnly=False)
		chunkFO = sshcomhoard.getChunkSFTP(justDir, sshhost, chunk)
		hash1, metaHash, hexHash1 = datHasher(filePath, fo=chunkFO)
		chunkFO.close()
	if hexHash1 == "interrupted":
		return 2
	if hash1 == 1:
		return 0
	hash2 = hashsHere[20:]
	wholeFileHash = hashsHere[:20]
	wholeFileHash = bytes.hex(wholeFileHash)
	if hash1 == hash2:						
		hash3 = bytes.hex(hash1)
		#is this chunk found here at fileLoc recorded in the db? If not, record it. default duplicateChunk to Nil since this is a real chunk
		#qry1 = ("select * from fileLoc where fileChunkHash = '{}' and fileLoc = '{}' and duplicateChunk = 'Nil' limit 1").format(hash3, fileLoc)
		qry1 = ("select * from files where filename = '{}';").format(quoteScan(originalFileName))
		r = dbhoard.dbQry(shconfig.SELECTED_DB, qry1)
		if (r == []):
			#qry2 = ("select * from files where filename = '{}';").format(quoteScan(originalFileName))
			#WHen DIFFing is implemented update this here to check for same filename but different wholehash...
			#r2 = dbhoard.dbQry(shconfig.SELECTED_DB, qry2)
			#if r2 == []:
			if batch == 0:
				batch = dbhoard.getBatchNum()
			thisChunk, chunkCount = getChunkNumber(chunk)
			thisChunk = int(thisChunk)
			chunkCount = int(chunkCount)
			if thisChunk == chunkCount: 
				if chunkCount > 1:
					chunkCount = chunkCount - 1
					mbCount = 104857600 * chunkCount
					mbCount = mbCount + fSize - 40
				else:
					mbCount = fSize - 40
				qry3 = ("insert into files (filename, filehash, batchNum, fileSize) values ('{}', '{}', {}, {});").format(quoteScan(originalFileName), wholeFileHash, batch, mbCount)
				r3 = dbhoard.dbQry(shconfig.SELECTED_DB, qry3)
				dbhoard.incrementFileTotalBytes(mbCount)
		qry2 = ("select * from fileLoc where fileChunkHash = '{}' and fileLoc = '{}' and duplicateChunk = 'Nil' limit 1").format(hash3, fileLoc)
		r2 = dbhoard.dbQry(shconfig.SELECTED_DB, qry2)
		if (r2 == []):
			dbhoard.recordChunkLoc(originalFileName, chunk, hash3, fileLoc)
			if quiet == False:
				print(("Added chunks to DB: " + str(chunk)))
		else:
			if quiet == False:
				print("Chunk has been scanned for errors but already exists in DB: " + str(chunk))
			
		return 1
	else:
		if quiet == False:
			print(("Corrupted chunk detected! Deleting chunk " + originalFileName + " ..."))
		delFile(filePath)
		dbhoard.removeChunkLoc(originalFileName, fileLoc)
		return 0

def acorn(numberofchunks):
	#add function that will walk a random storage location and pull out random .dup files to add to the acorn tar.
	#acorn currently will add multiple copies of the same chunk to the acorn... this is a waste of space
	copyQueue = []
	acornCount = 0
	acornError = 0
	failedChunks = []
	onlineStorage = backuphoard.initStorage(justDir=False)
	locOnline = False
	if onlineStorage == []:
		output("No storage locations found...")
		return 1
	numberOfRowsFileChunks = dbhoard.getNumberOfRows("fileLoc")
	while True:
		acornName = "acorn_" + str(idGen(15)) + ".tar"
		if os.path.isfile(acornName) != True:
			print(("Creating acorn file... " + acornName))
			break
	if numberOfRowsFileChunks < numberofchunks:
		#if you're trying to make an acorn file with more chunks than are backed up do this...
		numberofchunks = numberOfRowsFileChunks
	while acornCount < numberofchunks:
		chunk, loc, chunk2, loc2 = "", "", "", ""
		acorn, acorn2 = -1, -1
		uihoard.printInPlace("Preparing file chunk " + str(acornCount +1) + " of " + str(numberofchunks))
		for n in range(3):
			randomChunk = dbhoard.getRandomChunk()
			if randomChunk == 1:
				print("No files recorded to create acorns of.")
				return 1
			chunk, loc, acorn = dbhoard.getRandomChunk()
			for o in onlineStorage:
				if loc in o:
					locOnline = True
					break
			if locOnline == True:
				if acorn2 == -1:
					loc2 = dbhoard.getDirectoryStorage(loc)
					acorn2 = acorn
					chunk2 = chunk
				if acorn < acorn2:
					acorn2 = acorn
					chunk2 = chunk
					loc2 = dbhoard.getDirectoryStorage(loc)
				locOnline = False
		#print acorn2
		if loc2 not in ["", "Nil"] :
			filePath = loc2[0] + wack + chunk2
			#print filePath
			if (filePath not in copyQueue) and (filePath not in failedChunks):
				hashsHere = readMetaDataFromDat(filePath, sshhost=loc2[1])
				if loc2[1] != "Nil":
					SSHFO = sshcomhoard.getChunkSFTP(loc2[0], loc2[1], chunk2)
					hash1, metaHash, hexHash1 = datHasher(filePath, fo=SSHFO)
					SSHFO.close()
				else:	
					hash1, metaHash, hexHash1 = datHasher(filePath)
				hash2 = hashsHere[20:]
				if hash1 == hash2:
					copyQueue.append(filePath)
					acornCount = acornCount + 1
				else:
					if hexHash1 == "interrupted":
						return 1
					else:
						failedChunks.append(filePath)
						acornError = acornError + 1

	try:
		with tarfile.open(shconfig.retrieveWriteDir + wack + acornName, "w") as tar:
			for filePath2 in copyQueue:
				tar.add(filePath2)
				justDat = os.path.basename(filePath2)
				justDat = quoteScan(justDat)
				qry2 = ("update fileLoc set acorn = {} where fileChunk = '{}';").format(acorn2 + 1, justDat)
				r2 = dbhoard.dbQry(shconfig.SELECTED_DB, qry2)
				print(("Added chunk to acorn file: " + filePath2))
			tar.close()
	except IOError as e:
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
	out1 = "Created acorn file for hoarding called: " + acornName
	out2 = "\nWrote " + str(acornCount) + " dat chunks out of " + str(numberofchunks) + " requested."
	print((out1 + out2))
	if acornError > 0:
		print(("There were " + str(acornError) + " errors while backing up dat chunk files. Please run check-files for further information"))
	
