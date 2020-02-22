#!/usr/bin/python
from . import shconfig
from . import dbhoard
from . import fileutilhoard
from . import sshcomhoard
import sys
import os
import random
from . import fileutilhoard
import time
from io import BytesIO

SELECTED_DB = shconfig.SELECTED_DB
wack = shconfig.wack



class backup():
	#backup() is a class because in the future there may be an implementation allowing multiple backups at the same time 
	#this would be useful since it will utilize the throughput of multiple drives/devices during backup instead of being limited
	#to writing/reading to/from one device at a time. 
	global wack
	global command
	stopRequest = 0    #class wide vars. When stopping, will stop all instances of backup.
	running = 0


	
	def __init__(self):
		backup.stopRequest = 0 #not scoped to instance of class so each instance can shutdown all other instances.
		self.onlineStore = initStorage()
		self.failureList = []
		backup.running = 0
		self.doesntExist = []
		self.finishedChunking = False #used in the retrieve() method and copyChunk() method. Lets the method know chunk write was successful
		self.currentChunkNumber = 1 #used in retrieve()/copyChunk(), keeps tally of which chunk x of y is being worked on.
		self.totalChunks = 0
		self.batchNum = dbhoard.getBatchNum() #gets the last highest batch number so it can be incremented
		self.command = shconfig.command
		self.writeDir = shconfig.retrieveWriteDir

	def chunkUp(self, fileObj, fsize, fileNameOriginal, onlineStorage=[]): 
		#chunkUp() cuts up the file, hashes chunks, records them and spreads them to multiple sotrage locations. Used by backuprun
		running = 0
		counter = 0
		storageCounter = 0
		failedDuplicate = 0 #flag to catch if deduplication methods file, will fall back to creating new chunk as per usually. This will occur if identified previous duplicate is damaged or not existant
		numOfCopies = shconfig.numOfChunkCopies #how many redundant chunks will be stored.
		try:
			if self.onlineStore != []:
				onlineStorage = self.onlineStore
			else:
				onlineStorage = initStorage()
		except:
			fileutilhoard.exitOrReturn()
		justFileName = os.path.basename(fileNameOriginal)
		existsByName = dbhoard.getFileInfo(justFileName)
		if existsByName != 1:
			print("File already exists, " + justFileName)
			return 1
		hashOfFile, hexHashOfFile = fileutilhoard.hashFile(fileNameOriginal)
		#existsByHash = dbhoard.getFileExistsByHash(binascii.hexlify(hashOfFile))
		existsByHash = dbhoard.getFileExistsByHash(hexHashOfFile)
		if existsByHash == 1:
			print("File already exists, " + justFileName + " , " + hexHashOfFile)
			return 1
		if hexHashOfFile == "empty":
			return 1
		print(("Hash of File : " + hexHashOfFile))
		print("Chunking file...")
		storeLen = len(onlineStorage)
		randomNumber = random.randint(0, storeLen - 1)
		storageLoc = onlineStorage[randomNumber]
		counter1 = 0 #counter1 is the chunk number of the file, in 100MB chunks
		counter2 = 0 #counter2 is the failure counter. Count how many failures...
		counter3 = 0 #counter3 counts the number of times a chunk is written. Default minimum is 3 copies of each chunk
		counter4 = 0 #while loop failure catcher
		numberOfChunks = fileutilhoard.findNumberOfChunks(fileNameOriginal)
		chunkFOread = 0
		lastLocStored = []
		while(counter1 < numberOfChunks):
			if self.stopRequest == 1:
				print("Backup halted")
				self.stopRequest = 0
				self.running = 0
				fileObj.close()
				return 1
			if len(onlineStorage) < numOfCopies:
				fileutilhoard.exitOrReturn(exitMessage="Not enough storage locations!\n")
			hashOfChunk, hexHashOfChunk = fileutilhoard.chunkHasher(fileNameOriginal, counter1)
			if failedDuplicate == 0:
				duplicateHash = dbhoard.doesChunkHashExist(hexHashOfChunk, justFileName) #check to see if the the chunk is an exact dupplicate of another chunk already stored....
			chunkName3 = justFileName + "." + str(counter1 + 1) + "_" + str(numberOfChunks) + ".dat"
			chunkName2 = storageLoc[0] + wack + chunkName3
			if duplicateHash == "Nil":
				if chunkFOread == 0:
					fsize1, chunkFO = fileutilhoard.safeReadFile(fileNameOriginal, counter1)
				chunkFOread = 1
				if storageLoc[2] in ["", "Nil"]:
					writeSuccess = fileutilhoard.writeDat(hashOfFile, hashOfChunk, chunkName2, chunkFO)
					if writeSuccess == 1:
						dbhoard.setReadOnlyStorage(storageLoc[1])
						onlineStorage = initStorage()
						self.onlineStore = onlineStorage
					#
					#program hangs here if  chunkName2 is not writeable!
					#
				else:
					#write to ssh storage location
					chunkFO2 = BytesIO() #original chunk file object is read only since it connects directly to file on disk.
					#creates a new chunk file object, reads the data from disk then appends hash data as what would happen in the
					#original writeData() function.
					chunkFO.seek(0)
					chunkFO.seek(0,2)
					chunkFO.seek(0)
					chunkFO2.write(chunkFO.read())
					chunkFO2.write(hashOfFile + hashOfChunk)
					writeSuccess = sshcomhoard.putChunkSFTP(storageLoc[0], storageLoc[2], chunkName3, chunkFO2)
					if writeSuccess == 1:
						dbhoard.setReadOnlyStorage(storageLoc[1])
						onlineStorage = initStorage()
						self.onlineStore = onlineStorage
					chunkFO2.close()
				if writeSuccess == 0:
					counter2 = 0 #reset limit on write failures
					counter4 = 0 # reset limit on finding storageLoc failures
					#storageLocUID = dbhoard.getStorageLocUID(storageLoc[1])
					dbhoard.recordChunkLoc(justFileName, chunkName3, bytes.hex(hashOfChunk), storageLoc[1])
					counter3 = counter3 + 1
					if counter3 == numOfCopies:
						counter1 = counter1 + 1
						failedDuplicate = 0 #flip back incase it was flipped to 1 on a previous deduplication failure
						chunkFOread = 0
						chunkFO.close()
						counter3 = 0
						lastLocStored = []
					if counter3 != 0:
						lastLocStored.append(storageLoc[1])
				else: #If writeSuccess is not successful chunkFOread is not flipped so safeReadFile isn't unnecessarily run multiple times
					#It will then pick another random storage location to write to
					shconfig.readOnlyLoc.append(storageLoc[1])
					counter4 = 0
					counter2 = counter2 + 1
					if counter2 >= 100:
						print("Critical failure to write data.")
						fileutilhoard.exitOrReturn(exitMessage="Too many write failures! EXITING!")
				while True:
					#Find another storage location that hasn't been written to yet and isn't flagged read only.
					storeLen = len(onlineStorage)
					randomNumber = random.randint(0, storeLen - 1)
					try:
						#drives dropping mid backup off can cause this to become an index out of range error...
						storageLoc = onlineStorage[randomNumber]
					except:
						onlineStorage = initStorage()
						storeLen = len(onlineStorage)
						randomNumber = random.randint(0, storeLen - 1)
					if (storageLoc[1] not in lastLocStored) and (storageLoc[1] not in shconfig.readOnlyLoc):
						break
					counter4 = counter4 + 1
					if counter4 >= 10000:
						fileutilhoard.exitOrReturn(exitMessage="Failure while finding storage location. Please review your storage locations.")	
			else: #hash already exists, make dedupe metadata
				#duplcateHash is just the chunk that has a chunk stored in the system that has a matching chunk value
				#basis for the minor deduplication used in scatterhoard.
				dupeCreatedCounter = len(duplicateHash)
				
				dirOfChunk = dbhoard.getDirectoryStorage(duplicateHash[0][3])
				chunkPath = dirOfChunk[0] + wack + duplicateHash[0][1]
				while (len(duplicateHash) < numOfCopies) and (dupeCreatedCounter < numOfCopies):
					healthCheckOnDupe = fileutilhoard.healthCheckSingleChunk(chunkPath, duplicateHash[0][3])
					if healthCheckOnDupe == 1:
						self.duplicateChunk(duplicateHash[0])
						dupeCreatedCounter = dupeCreatedCounter + 1
					if healthCheckOnDupe == 2:
						backup.stopRequest == 1
				if len(duplicateHash) >= numOfCopies:		
					for d in duplicateHash:
						if failedDuplicate == 0:
							dirOfChunk = dbhoard.getDirectoryStorage(d[3])
							chunkPath = dirOfChunk[0] + wack + d[1]
							healthCheckOnDupe = fileutilhoard.healthCheckSingleChunk(chunkPath, d[3])
							if (healthCheckOnDupe == 1) or (healthCheckOnDupe == 2):
								#add code to compare byte by byte (twice in seperate variables to rule out memeory problems) of hash matches to rule out hash collusion of different data!
								print(("Found duplicate chunk data: " + chunkName3 + ", " + d[1]))
								dbhoard.recordChunkLoc(justFileName, chunkName3, bytes.hex(hashOfChunk), d[3], duplicatedChunk=d[1])
								dir = dbhoard.getDirectoryStorage(d[3])
								fileutilhoard.writeDup(hashOfChunk, dir[0], chunkName3, d[1], d[3])
								#dupFileName = d[1] + ".dup"
								#dbhoard.recordChunkLoc(dupFileName, dupFileName, dupHash, d[3], "Nil")
								counter3 = counter3 + 1
								lastLocStored.append(d[3])
								if healthCheckOnDupe == 2:
									backup.stopRequest = 1
							else:
								counter3 = 0
								failedDuplicate = 1
							if counter3 >= numOfCopies:
								counter1 = counter1 + 1
								chunkFOread = 0
								counter3 = 0
								lastLocStored = []


		return bytes.hex(hashOfFile)
				
	def backUpRun(self, fileToEncode, tags=[], batch=0, batchTags=False):
		#fileToEncode is passed from gui if available, it is the full path
		#justFileName is resolved from the other two variables, it doesn't have the path
		#justPath, just the path to the file from fileName/fileToEncode
		global wack
		backup.running = 1
		backup.stopRequest = 0
		hashOfFirstChunk = ""

		if fileToEncode != "":
			fileName = fileToEncode
		if batch != 1:
			print(("Backing up: " + fileName))
		justFileName = os.path.basename(fileName)
		justPath = os.path.dirname(fileName)
		badChar, onlySpace = fileutilhoard.checkFileNameChar(justFileName)
		if badChar == 1:
			if batch != 1:
				print("File name being backed up cannot contain the following characters!")
				print("""
				"\"", "/", "\\", "%", "?", "*", ":", "|", "<", ">", "\'"
					""")
				print("Backup failed")				
			#return 0 why was this 0 originally? o_o
			return 1

		if justPath == "":
			justPath = "." + wack
			#ensures the path is the current working directory of this script!
		qry1 = ("select * from files where filename = '{}'").format(fileutilhoard.quoteScan(justFileName))
		r1 = dbhoard.dbQry(shconfig.SELECTED_DB, qry1)
		if r1 != []:
			#if batch != 1:
			print(("Filename already exists: " + justFileName))
			#print("Please rename file and try again.")
			#compare hash of given file here and what hash is stored in db, if not the same show it.
			self.stopBackup()
			return 1
		fsize = 0
		hashOfFile = ""
		if not os.path.isfile(fileName):
			print("File does not exist Check for typos. Filenames with spaces also require double quotes around filename.")
			backup.running = 0
			self.stopBackup()
			return 1
		fsize = os.path.getsize(fileName)
		checkStop = checkForStop()
		if checkStop == True:
			return 1
		try:
			with open(fileName,'rb') as fileObj:
				hashOfFile = self.chunkUp(fileObj, fsize, fileName)
				if hashOfFile == 1:
					if batch != 1:
						print("Backup halted...")
					self.stopBackup()
					return 1
		except IOError as e:
			print(e)
			print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		except:
			print("Something went wrong")
			print(fileObj)
		checkStop = checkForStop()
		if checkStop == True: return 1
		onlineStorage = self.onlineStore
		insertQry = ("insert into files (filename, filehash, batchNum, fileSize) "
				"values ('{}', '{}', {}, {})").format(fileutilhoard.quoteScan(justFileName), hashOfFile, self.batchNum, fsize)
		dbhoard.dbQry(shconfig.SELECTED_DB, insertQry)
		dbhoard.incrementFileTotalBytes(fsize)
		if (shconfig.args.tags) and (batchTags == False):
			for tag in shconfig.args.tags:
				badChar1, onlySpace1 = fileutilhoard.checkFileNameChar(tag)
				if (tag != "") and (badChar1 != 1):
					dbhoard.addTag(fileutilhoard.quoteScan(justFileName), fileutilhoard.quoteScan(tag))
		print((justFileName + " has been backed up."))	
		self.stopBackup()
		return 0

	def stopBackup(self, retrieveType=False):
		backup.running = 0
		backup.stopRequest = 1
		self.currentChunkNumber = 1 #reset current chunk number to 1 for next retrieve request for this object. used in batch loops
		
	def copyChunk(self, currentChunk, chunkPath, sshhost="Nil"):
		#totalChunks = self.totalChunks
		#copy chunk reads only the payload data of .dat files and appends them to the
		#file being recreated/retrieved. Not the same as duplicateChunk()!
		#currentChunk is the chunk sequence currently being looked for to rebuild file. 
		#chunkPath is the dir and filename of correct chunk resting in storage location to be copied.
		accessType = 'ab'

		#remove chunk numeber and .dat file extensions to remake proper file name
		fileNameSplit = currentChunk.split(".")
		#fileNameSplit.pop()
		#fileNameSplit.pop()
		fileName = ""
		pos = -1
		#fileName = fileNameSplit[0] + "." + fileNameSplit[1]
		for f in fileNameSplit[:-2]:
			pos += 1
			if pos != (len(fileNameSplit[:-2]) -1):
				fileName = fileName + f + "."
			else:
				fileName = fileName + f
		if sshhost in ["", "Nil"]:
			chunkSize = os.stat(chunkPath).st_size
		#
		if checkForStop(): self.stopBackup()
		#
		if self.currentChunkNumber == 1:
			accessType = 'wb'
			seekChunk = 0 #initializing
		with open(self.writeDir + wack + fileName, accessType) as rf:			
			seekChunk = self.currentChunkNumber - 1
			if sshhost in ["", "Nil"]:
				with open(chunkPath, 'rb') as rf2:
					rf.write(rf2.read(chunkSize - 40))
					rf.close()
					fileHashRead = rf2.read(20)
					chunkHashRead = rf2.read(20)				
					rf2.close()
			else:
				sshDir = os.path.dirname(chunkPath)
				chunkFromSSHFO = sshcomhoard.getChunkSFTP(sshDir, sshhost, currentChunk)
				chunkFromSSHFO.seek(0,2)
				chunkSize = chunkFromSSHFO.tell()
				chunkFromSSHFO.seek(0)
				rf.write(chunkFromSSHFO.read(chunkSize - 40))
				fileHashRead = chunkFromSSHFO.read(20)
				chunkHashRead = chunkFromSSHFO.read(20)
				rf.close()
				chunkFromSSHFO.close()
		hashOfJustWritten, hexHash = fileutilhoard.chunkHasher(self.writeDir + wack + fileName, seekChunk)
		if (bytes.hex(hashOfJustWritten) != bytes.hex(chunkHashRead)):
			print((bytes.hex(hashOfJustWritten)))
			print((bytes.hex(fileHashRead))) 
			print((bytes.hex(chunkHashRead))) 
			#fileutilhoard.delFile("." + wack + fileName)
			print(("ERROR detected with chunk: " + chunkPath))
			self.doesntExist.append(chunkPath)
			#self.stopBackup()
			#return 1
			fileutilhoard.exitOrReturn()
		else:
			self.currentChunkNumber = self.currentChunkNumber + 1
			if int(self.currentChunkNumber) > int(self.totalChunks):
				self.finishedChunking = True
			return 0
	
	def getChunkPathRetrieve(self, fileName, chunklist, onlineStorage):
		#chunklist is a list of the file chunks for the CURRENT chunk and not all chunks for total file
		#it will attempt to find one chunk from the chunklist and confirm it exists so it can be copied
		#If the chunk is a duplicate of another chunk it will return the path + filename of the .dat it is a duplicate of.
		for c in chunklist:
			fileExists = 0
			currentChunk = fileName + "." + str(self.currentChunkNumber) + "_" + str(self.totalChunks) + ".dat"
			if (c[0] == currentChunk) and (c[2] not in self.failureList):
				chunkPath = ""
				for d in onlineStorage:
					if (c[2] == d[1]):
						readStorage = d[0]
						if c[3] != "Nil":  #does not refer to sshhost, but to another chunk this chunk is an exact duplicate of (deduplication)
							chunkPath = readStorage + wack + c[3] #return the duiplicateChunk path, the .dat will be different than the file being retrieved...
							print(("Path of chunk duplicate: " + chunkPath))
						else:
							chunkPath = readStorage + wack + c[0]
						if chunkPath not in self.doesntExist:
							try:
								fileExists = fileutilhoard.doesFileExist(chunkPath, d[2])
							except IOError as e:
								print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
								self.doesntExist.append(chunkPath)
					if fileExists:
						return chunkPath, d[2]
					else:
						self.doesntExist.append(chunkPath)
		return "none", "Nil"
	
	def getAltChunkPathRetrieve(self, onlineStorage, currentChunk):
		#backup method to find chunks that aren't found in DB. Is very slow.
		#currentChunk is the name of the chunk that is currently being looked for
		for d in onlineStorage:
			if d[2] in ["", "Nil"]:
				for f in os.listdir(d[0]):
					if os.path.isdir(f):
						# skip directories
						continue
					else:
						#print("File in dir: " + f)
						if f == currentChunk:
							chunkPath = d[0] + wack + f
							print("Retrieving uncatalogued chunk: " + chunkPath )
							writeSuccess = self.copyChunk(currentChunk, chunkPath)
							if writeSuccess == 0:
								return writeSuccess
			else: #d[3] is sshhost, meaning this storage location is remote ssh host
				files = sshcomhoard.getFileListSFTP(d[0], d[2])
				if currentChunk in files:
					chunkPath = d[0] + wack + currentChunk 
					writeSuccess = self.copyChunk(currentChunk, chunkPath, sshhost=d[3])
					print("Retrieving uncatalogued chunk: " + d[3] + ":" + chunkPath )
					if writeSuccess == 0:
						return writeSuccess
		print(("Cannot find chunk in any available storage locations: " + currentChunk))
		self.stopBackup()
		return

	def retrieve(self, fileToRetrieve="", tags=[], getID=0, batch=0):
		commandline = False
		duplicate = ""
		backup.running = 1
		fileRetrieved = False
		self.currentChunkNumber = 1
		if (not shconfig.args.get):
			commandline = True
			fileName = fileToRetrieve
		if shconfig.args.get:
			fileName =  shconfig.args.get
		#structure of online storage described in initStorage() function
		onlineStorage = initStorage(justDir=False, addReadOnly=True)
		fileList = []
		fileInfo = dbhoard.getFileInfo(fileName)
		if fileInfo == 1:
			print("Retrieve failed: no file by that name.")	
			self.stopBackup()
			return 1
		else:
			fileHashDB = fileInfo[0][1]
		#if chunk is not found at location, check the other 2
		#if no chunk still, file cannot be recovered, return retrieve action with critical error.
		chunkList = dbhoard.getChunkList(fileName)
		if chunkList != []:
			firstInList, self.totalChunks = fileutilhoard.getChunkNumber(chunkList[0][0])
		else:
			firstInList, self.totalChunks = fileutilhoard.getChunkNumber(fileName)
		fullyAvailable, availableChunks, unavailableChunks, CWDChunks = fileutilhoard.isWholeFileAvailable(chunkList, onlineStorage, self.totalChunks)
		if fullyAvailable == True:
			pass
		else:
			if not shconfig.args.get: #-g command line arg means non interactive usage
				turnOnResume = input('\nThis file is not fully available with currently online storage locations. \nDo you want to pull available chunks into the current directory to resume the retrieval process later?\n')
				if turnOnResume in ['Y', 'y', 'yes', 'Yes']:
					print("Available chunks will be stored in the current working directory. \nBring more storage locations online and run the get command on this file again until complete")
					for c in availableChunks:
						print("Retrieveing temporary chunk to working directory, will resume later: " + c[0])
						writeSuccess = self.duplicateChunk(c, resume=True)
				else:
					print("Retrieval of file failed, not enough file chunks available. Please consider bringing more storage locations online or repairing data set with Acorn files")
					return 1
			else:
				print("Retrieval of file failed, not enough file chunks available. Please consider bringing more storage locations online or repairing data set with Acorn files")
				return 1
		failureCount = 0
		currentlyChunking = True
		self.finishedChunking = False
		self.doesntExist = []
		failCount = 0
		chunkPath = ""
		while fileRetrieved == False: #batch jobs will run retrieve multiple times...
			if checkForStop(): self.stopBackup()
			if backup.running == 0:
				print("Retrieve request stopped...")
				self.stopBackup()
				return 1
			if firstInList == "dup":
				currentChunk = fileName
			elif firstInList == "nil":
				print("No chunks in DB for file, please run catalogue across all storage locations")
				return 1
			else:
				currentChunk = fileName + "." + str(self.currentChunkNumber) + "_" + str(self.totalChunks) + ".dat"
			readStorage = ""
			self.failureList = []
			self.finishedChunking = False
			if self.currentChunkNumber > self.totalChunks:
				self.finishedChunking = True
			else:
				print(("Retrieving chunk: " + currentChunk))
			########copyChunk section of retrieve
			for c in availableChunks:
				if c[0] == currentChunk:
					chunkPath = shconfig.retrieveWriteDir + wack + currentChunk
					writeSuccessR = self.copyChunk(currentChunk, chunkPath)
					if writeSuccessR == 0:
						fileutilhoard.delFile(chunkPath, verbose="no")
						currentChunk = fileName + "." + str(self.currentChunkNumber) + "_" + str(self.totalChunks) + ".dat"
						print(("Retrieving chunk: " + currentChunk))
			for c in CWDChunks:
				if c == currentChunk:
					chunkPath = shconfig.retrieveWriteDir + wack + currentChunk
					writeSuccessR = self.copyChunk(currentChunk, chunkPath)
					if writeSuccessR == 0:
						fileutilhoard.delFile(chunkPath, verbose="no")
						currentChunk = fileName + "." + str(self.currentChunkNumber) + "_" + str(self.totalChunks) + ".dat"
						print(("Retrieving chunk: " + currentChunk))
			if (self.currentChunkNumber > self.totalChunks):
				self.finishedChunking = True
				currentlyChunking = False
			else:
				chunkPath, sshhost = self.getChunkPathRetrieve(fileName, chunkList, onlineStorage)
				if chunkPath != "none":
					writeSuccessR = self.copyChunk(currentChunk, chunkPath, sshhost=sshhost)
				else:
					for c in chunkList:
						if (currentChunk == c[0]) and (c[3] != "Nil"):
							#c[3] is deduplication chunk filename
							duplicate = c[4]
					if duplicate != "":
						writeSuccessR = self.getAltChunkPathRetrieve(onlineStorage, duplicate)
						#can't find chunk name because it is a duplicate. get deduplication chunk instead
					else:
						writeSuccessR = self.getAltChunkPathRetrieve(onlineStorage, currentChunk)
					if backup.running == 0:
						print("Please run the catalogue command or pull from acorn files")
						return
				if writeSuccessR == 0:
					currentChunk = fileName + "." + str(self.currentChunkNumber) + "_" + str(self.totalChunks) + ".dat"
				if (currentlyChunking == True) and (self.finishedChunking == False):
					unlistedChunks = []
					#currentlyChunking == True at this point means the onlineStorage locations have been exhasted
					#with no matching file chunk listed... time to listDir in all onlineStorage locations and sift through it 
					#to find the file... this is the last resort and much slower.
			if self.finishedChunking == False:
				currentlyChunking = True
			else:
				hashOfCompleted, hexHash = fileutilhoard.hashFile(self.writeDir + wack + fileName)
				if (bytes.hex(hashOfCompleted) != fileHashDB):
					fileutilhoard.delFile("." + wack + fileName)
					print("Retrieve failed. Write error detected during hashing.")
					sys.exit()
					self.stopBackup()
					
				else:
					fileRetrieved = True
					self.finishedChunking
					print(("Hash of retrieved file: " + bytes.hex(hashOfCompleted)))
					print(("Hash of file in DB    : " + fileHashDB))
					print("File is confirmed to be retrieved with 100% data integrity : " + self.writeDir + wack + fileName + ")")
		#print("Retrieve successful. Wrote file " + self.writeDir + wack + fileName )
		#writeLabel("Retrieve successful: " + fileName)
		#self.stopBackup()
		
	def globalCheck(self, numberOfChunks, checkAll=False):
		#if numberOfChunks = 0 this is a global function testing ALL chunks
		onlineStorage = initStorage()
		previouslyChecked = []
		counter1 = 0
		backup.running = 1
		countedBytes = 0
		healthyBytes = 0
		print("\nRunning health check on recorded data chunks...")
		while counter1 < numberOfChunks:
			if checkForStop(): 
				self.stopBackup()
				print("Stopping health check...")
				return 1
			randomChunk = dbhoard.getRandomChunk(getAllFields=True, getDuplicates=False)
			if randomChunk == 1:
				print("No recorded files to check health of.")
				return 1
			chunkList = dbhoard.getChunkShortList(randomChunk[0][1])
			successHealth, healthyBytes = self.healthCheck(chunkList)
			if successHealth == 0:
				counter1 = counter1 + 1
				countedBytes = countedBytes + healthyBytes
				healthyBytes = 0
		if numberOfChunks == 0:
			qry1 = "select * from fileLoc where duplicateChunk = 'Nil';"
			r2 = dbhoard.dbQry(shconfig.SELECTED_DB, qry1)
			if r2 != []:
				matchingChunks = []
				completedChunks = []
				for c in r2:
					if checkForStop(): 
						self.stopBackup()
						return 1
					if c[1] not in completedChunks:
						for d in r2:
							if d[1] == c[1]:
								matchingChunks.append(d)
					if matchingChunks != []:
						successHealth, healthyBytes = self.healthCheck(matchingChunks) #duplicateChunk causes there to be less than 3 chunkHealth checks..
					if successHealth == 0:
						completedChunks.append(c[1])
						countedBytes = countedBytes + healthyBytes
						healthyBytes = 0
					else:
						if backup.stopRequest == 1:
							return 1
					matchingChunks = []
			numberOfChunks = 1
		print("\nFile Health Check completed. ")
		print((str(countedBytes) + " bytes of data (and redundant data) stored with no errors. (" + str(round(countedBytes/1048576)) + " MB)"))
		dbhoard.updateConfig("bytesStored", countedBytes) 
		backup.running = 0
		return 0
		
	def healthCheck(self, chunkList):
		chunkHealth = 0
		badChunks = []
		goodChunks = []
		fileExists = 1
		sshhost = "Nil"
		fileSize = 0
		countedBytes = 0
		for c in chunkList:
			storageOnline = False
			for uid in self.onlineStore:
				if c[3] == uid[1]:
					storageOnline = True	
			for uid in shconfig.readOnlyLoc:
				if c[3] == uid[2]:
					storageOnline = True
			print(("Checking health of file chunk: " + c[1] + " at " + c[3]))
			if checkForStop(): 
				self.stopBackup()
				return 1, 0
			dir, sshhost = dbhoard.getDirectoryStorage(c[3])
			if c[5] != "Nil":
				#C[5] is the chunk this chunk is a duplicate of. There is no chunk to healthcheck in this instance.
				#.dup files can be health checked but will have c[5] set to Nil since there aren't dupes of dupes.
				fullPath = dir + wack + c[5]
				targetFile = c[1]
			else:
				fullPath = dir + wack + c[1]
				targetFile = c[1]
			if storageOnline == True:	
				fileExists = fileutilhoard.doesFileExist(fullPath, sshhost=sshhost)
			else:
				fileExists = 0
			if fileExists in [1, True]:
				if sshhost in ["", "Nil"]:
					hash1, metaHash, fileSize, hexHash1 = fileutilhoard.datHasher(fullPath, countBytes=True)
					hash2 = fileutilhoard.readMetaDataFromDat(fullPath)
					hash2 = hash2[20:]
				else:
					fo = sshcomhoard.getChunkSFTP(dir, sshhost, targetFile)
					if (fo == 1):
						print("Cannot verify health, ending healthcheck command")
						break
					hash1, metaHash, fileSize, hexHash1 = fileutilhoard.datHasher("Nil", fo=fo, countBytes=True)
					hash2 = metaHash[20:]
					fo.close()
				if hash1 == hash2:
					chunkHealth = chunkHealth + 1
					goodChunks.append(c)
					countedBytes = countedBytes + fileSize
					fileSize = 0
				else:
					if hash1 == 1:
						if fileSize == "interrupted":
							backup.stopRequest = 1
							self.stopBackup()
							return 1, 0
					print((bytes.hex(hash1)))
					print((bytes.hex(hash2)))
					print("   Corrupted chunk detected! Deleting chunk...")
					fileutilhoard.delFile(fullPath, sshhost=sshhost)
					dbhoard.removeChunkLoc(c[1], c[3])
					fileSize = 0
			else:
				if sshhost in ["", "Nil"]:
					print(("   Missing chunk, expected at: " + fullPath))	
				else:
					print(("   Missing chunk, expected at: " + sshhost + ":" + fullPath))
				dbhoard.removeChunkLoc(c[1], c[3])
		while chunkHealth < shconfig.numOfChunkCopies:
			if checkForStop():
				self.stopBackup()
				return 1, 0
			if len(goodChunks) > 0:
				dupHash = self.duplicateChunk(goodChunks[0])
				#dupHash = bytes.hex(dupHash)
				if dupHash == goodChunks[0][2]:
					chunkHealth = chunkHealth + 1
					successDuplicate = 1
			else:
				print("File has too many missing or corrupted chunks. Please attempt to find missing chunk in acorn files...")
				return 1, 0
		return 0, countedBytes

	def duplicateChunk(self, goodChunk, resume=False):
		#This function is used to maintain minimum redundancy of chunk files.
		#returns sha1 hash of chunk as hex
		if resume == False:
			selectedPath = ""
		else:
			selectedPath = shconfig.retrieveWriteDir
			#this mutats the goodChunk list for resume functions... this should be resolved when chunks become objects...
			goodChunk2 = goodChunk
			goodChunk = []
			goodChunk.append(goodChunk2[4])
			goodChunk.append(goodChunk2[0])
			goodChunk.append(goodChunk2[1])
			goodChunk.append(goodChunk2[2])
			goodChunk.append(1)
			goodChunk.append(goodChunk2[3])
		readFromPath = ""
		sshhostWrite = ""
		onlineStorage = self.onlineStore
		storeLen = len(onlineStorage)
		if (storeLen < shconfig.numOfChunkCopies) and (resume == False):
			print("Not enough storage locations! Add more using command \' add-storage \'.")
			self.stopBackup()
			return 1
		counter1 = 0
		if resume == False:
			print("   Duplicating chunk to meet minimum redundancy requirements")
		
		#Selecting a place to make a copy to...
		while (selectedPath == "") and (resume == False):
			counter1 += 1
			if counter1 == 1000: return 1 #Failure to duplicate
			randomNumber = random.randint(0, storeLen - 1)
			storageLoc = onlineStorage[randomNumber]
			writeUID = ""
			if storageLoc[1] != goodChunk[3]:
				#Storage location  selected for duplication attempt is not already being used to used to store that 
				#	particular chunk.
				if not fileutilhoard.doesFileExist(storageLoc[0] + wack + goodChunk[1], sshhost=storageLoc[2], ignoreError=True): 
					#making sure the chunk file is not present where we are looking to write it
					#storageLoc[2] is sshhost
					selectedPath = storageLoc[0]
					writeUID =  storageLoc[1]
					sshhostWrite = storageLoc[2]
				else:
					#Does it exist in DB?
					recordList = dbhoard.getChunkShortList(goodChunk[1], storageLoc[1])
					writeUID =  storageLoc[1]
					if recordList == []:
						#print(("      This is an unrecorded redundant chunk... making note of its location in DB: " + str(storageLoc[0] + wack + goodChunk[1])))
						#print("      ... verifying health of chunk first before writing to DB...")
						if storageLoc[2] in ["", "Nil"]:
							hash1, metaHash, hexHash1 = fileutilhoard.datHasher(storageLoc[0] + wack + goodChunk[1])
						else:
							fo = sshcomhoard.getChunkSFTP(storageLoc[0], storageLoc[2], goodChunk[1])
							hash1, metaHash, hexHash1 = fileutilhoard.datHasher(storageLoc[0] + wack + goodChunk[1], fo=fo)
							fo.close()
						
						recorded1 = fileutilhoard.healthCheckSingleChunk(storageLoc[0] + wack + goodChunk[1], storageLoc[1], sshhost=storageLoc[2])
						if recorded1 == 2:
							backup.stopRequest = 1
						if recorded1 == 1:
							return hexHash1			
		if goodChunk[5] != "Nil":
			chunkFileName = goodChunk[5]
		else:
			chunkFileName = goodChunk[1]
		for o in onlineStorage:
			if (goodChunk[3] == o[1]) and (fileutilhoard.doesFileExist(o[0] + wack + chunkFileName, o[2])):
				readFromPath = o[0]
				readFromUID = o[1]
		readFromPath = readFromPath + wack + chunkFileName
		writeToPath = selectedPath + wack + goodChunk[1]
		#THIS AREA NEEDS TO CONFIRM THAT THE WRITE TO PATH IS WRITABLE
		with open(readFromPath, 'rb') as r:
			if (sshhostWrite == "Nil") or (resume == True):
				try:
					with open(writeToPath, 'wb') as w:
						w.write(r.read())
						w.close()
					newHash, metaHash, hexHash = fileutilhoard.datHasher(writeToPath)
				except OSError:
					fileutilhoard.delFile(writeToPath)
					dbhoard.setReadOnlyStorage(writeUID, onlineStorage=self.onlineStore)
					return 1
			else:
				remoteSuccess = sshcomhoard.putChunkSFTP(selectedPath, sshhostWrite, goodChunk[1], r)
				if remoteSuccess == 0:
					datHasherFO = sshcomhoard.getChunkSFTP(selectedPath, sshhostWrite, goodChunk[1])
					newHash, metaHash, hexHash = fileutilhoard.datHasher(writeToPath, datHasherFO)
					datHasherFO.close()
			#if sshhostWrite != "Nil": datHasherFO.close()
			r.close()
		if (hexHash == goodChunk[2]) and (resume == False): 
			#do not record duplicated chunk if using resume feature, these are temporary chunks
			recordSuccess = dbhoard.recordChunkLoc(goodChunk[0], goodChunk[1], goodChunk[2], writeUID)
			if recordSuccess == 1:
				print(("   Created duplicate chunk to meet redundancy requirements: " + str(storageLoc[0] + wack + goodChunk[1])))
		if (hexHash != goodChunk[2]) and (reusme == True):
			print("Failed duplication of chunk. Deleting temporary chunk.")
			fileutilhoard.delFile(writeToPath, verbose="no", sshhost="Nil")
			return 1				
		return hexHash
			

def initStorage(justDir=False, addReadOnly=False):
	#TODO:  If UID.txt is lost for a storage location there needs to be a function that will identify what the UID should be based on the 
	#UID associated with file chunks. This will require "walking" through the file location and comparing the contents to the database
	#q = ("drop table storage").format()
	#r = dbhoard.dbQry(shconfig.SELECTED_DB, q)
	#dbhoard.createStorageTable()
	qry1 = "select * from storage"
	results = dbhoard.dbQry(shconfig.SELECTED_DB, qry1)
	onlineStorage = []
	justStorage = []
	readOnly = []
	for r in results:
		if (r[3] == "Nil") or (r[3] == ""):
			try:
				with open(r[1] + wack + "uid.txt",'r') as f:
					uid = f.read()
					f.close()
			except IOError as e:
				print(("		Cannot access " + r[1] + wack + "uid.txt" + "   ...   I/O error({0}): {1}".format(e.errno, e.strerror)))
		else:
			try:
				uidFO = sshcomhoard.getChunkSFTP(r[1], r[3], "uid.txt")
				uidFO.seek(0)
				uid = uidFO.read()
				uid = uid.decode() #ptyhon3 required this because it returned a byte type that can't be compared to string type
				uidFO.close()
			except IOError as e:
				print(e)
				print("cannot get UID from SSH remote host")
			except AttributeError as e:
				print("Cannot get UID from SSH remote host. If a remote SSH host is unavailable please run the ignore-storage command to continue without it.")
				print((str(r[1])))
				print((str(r[3])))
		if uid == r[2]:
			#structure of onlineStorage array:
			#r[1] is the directory of the storage location
			#r[2] is the UID of the storage location (identifier)
			#r[3] is the ssh host address if available, 'Nil' if not
			if justDir == False:
				if ((r[2] in shconfig.readOnlyLoc) or (r[6] == 1)) and (addReadOnly == False):
					readOnly.append([r[1], r[2], r[3]])
					if r not in shconfig.readOnlyLoc:
						shconfig.readOnlyLoc.append(r)
				else:
					onlineStorage.append([r[1], r[2], r[3]])
					justStorage.append(r[1])
			else:
				if (r[2] in shconfig.readOnlyLoc) and (addReadOnly == False):
					readOnly.append(r[1])
					if r not in shconfig.readOnlyLoc:
						shconfig.readOnlyLoc.append(r)
				else:
					onlineStorage.append(r[1])
	return onlineStorage

def catalogueDir(uid):
	onlineStorage = initStorage(addReadOnly=True)
	fileCounter = 0
	#batchNum = getBatchNum()
	sshhost = ""
	directory = ""
	for d in onlineStorage:
		if d[1] == uid:
			sshhost = d[2]
			directory = d[0]
	if (directory == ""):
		print(("DIRECTORY NOT ONLINE: " + directory))
		return 1
	for storageLoc in onlineStorage:	
		try:	
			if (uid in storageLoc):
				if (sshhost in ["","Nil"]):
					print(("FINDING DAT & DUP FILES IN DIRECTORY: " + directory))
					dir = next(os.walk(directory))
					for file in dir[2]:
						filePath = os.path.join(directory, file)
						originalFileName = fileutilhoard.getFileNameFromDat(file)
						extension = fileutilhoard.getExt(file)
						if extension == "dat":
							fileCounter = fileCounter + 1
							fileLoc = dbhoard.getUIDStorage(directory)
							inDB = dbhoard.isChunkRecorded(file, fileLoc)
							if inDB == False:
								healthReturn1 = fileutilhoard.healthCheckSingleChunk(filePath, uid)
								if healthReturn1 != 1:
									if healthReturn1 == 2:
										backup.stopRequest = 1
										return 1
									print(("FOUND CORRUPTED DAT FILE: " + file))
							else:
								print("File is already recorded in DB: " + file)
						if extension == "dup":
							fileutilhoard.readDup(filePath)
				else:
					#directory is on a remote SSH host
					dir = sshcomhoard.getFileListSFTP(directory, sshhost)
					sshhost2 = sshhost
					for file in dir:
						filePath = os.path.join(directory, file)
						originalFileName = fileutilhoard.getFileNameFromDat(file)
						extension = fileutilhoard.getExt(file)
						if extension == "dat":
							fileCounter = fileCounter + 1
							healthReturn1 = fileutilhoard.healthCheckSingleChunk(filePath, uid, sshhost=sshhost2)
							if healthReturn1 != 1:
								if healthReturn1 == 2:
									backup.stopRequest = 1
									return 1
								print(("FOUND CORRUPTED DAT FILE: " + file))
						if extension == "dup":
							fileutilhoard.readDup(filePath, ssh=sshhost)
		except KeyboardInterrupt:
			print("SCRUB INTERRUPTED BY USER, EXITING")
			return 1
			
def checkForStop():
	#Checks to see if backup class has a cancel request from user
	#changes stopRequest switch which is polled by functions in thread from runThread classes
	#change frame reference to be a reference to an argument. Argument is a frame object. This allows exporting to a module
	if backup.stopRequest == 1:
		#backup.stopRequest = 0
		backup.running = 0
		return True
	else:
		return False

def writeLabel(text):
	#print(shconfig.guiObj)
	#print(shconfig.label)
	#if shconfig.gui == True:
	#	shconfig.label.text = text
	#	shconfig.label.pack()
	return 0

def queueRun():
	b = backup()
	while (shconfig.backJobs != []) or (shconfig.retrieveJobs != []):
		time.sleep(0.1)
		if (backup.running == 0) and (shconfig.backJobs != []):
			b.backUpRun(shconfig.backJobs.pop())
		if (shconfig.backJobs == []) and (backup.running == 0) and (shconfig.retrieveJobs != []):
			b.retrieve(shconfig.retrieveJobs.pop())
		#shconfig.guiObj.backUpTab.queueTxt.Clear()
		#shconfig.guiObj.retrieveTab.fileListField.Clear()
	while (shconfig.tagJobs != []):
		time.sleep(0.1)
		if shconfig.tagLock == False:
			shconfig.tagLock = True
			fileToTag = shconfig.tagJobs.pop()
			if (not os.path.isdir(fileToTag)):
				justFileName = os.path.basename(fileToTag)
				getTagFor = "Set tags for: " + fileutilhoard.maxText(justFileName) + "..."
				tags = uihoard.guiGetTags(message=getTagFor)
				for tag in tags:
					addTag(justFileName, tag)
			shconfig.tagLock = False
			


mainBackObj = None
