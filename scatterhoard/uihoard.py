#!/usr/bin/python
#functions for controlling UI whether in cli or gui
import sys
from . import dbhoard 
from . import shconfig
from . import backuphoard
from . import fileutilhoard
import unicodedata
#from . import webserverhoard
#import webbrowser
import os

try:
	import tkinter, tkinter.filedialog, tkinter.simpledialog
	shconfig.gui = True
	print("GUI support is ON.")
except:
	print("No GUI support is available. Please try sudo apt-get install python-tk for python2 or sudo apt-get install python3-tk for python3")
	
def mainGUI():
	#Not, this and other Tkinter functions are not threaded and will block operation.
	if shconfig.gui == True:
		shconfig.guiObj = tkinter.Tk()
		shconfig.guiObj.title("Scatter-Hoard")
		shconfig.label = tkinter.Label(shconfig.guiObj, fg="black", width=30, height=10, text='SCATTER-HOARD')
		shconfig.label.pack()
		button = tkinter.Button(shconfig.guiObj, text='Exit', width=25, command=shconfig.guiObj.destroy)
		button2 = tkinter.Button(shconfig.guiObj, text='Select Files to Backup', width=25, command=guiPutFiles)
		button3 = tkinter.Button(shconfig.guiObj, text='Search for files to retrieve', width=25, command=guiGetFiles)
		button4 = tkinter.Button(shconfig.guiObj, text='Return to CLI menu', width=25, command=guiGoCLI)
		button2.pack()
		button3.pack()
		button4.pack()
		button.pack()

		shconfig.guiObj.mainloop()

		
	else:
		print("GUI is not available. Please install Tkinter by using the command \'pip install Tkinter\'")
		return 1
		
def clearScreen():
	if shconfig.systemPlatform == "Windows":
		os.system("cls")
	else:
		os.system("clear")
	return 0
		
def writeLabel(text):
	if shconfig.gui == True:
		shconfig.guiObj.label.text = text
		shconfig.guiObj.label.pack()
	return 0
		
def guiPutFiles():
	fileNames = tkinter.filedialog.askopenfilenames()
	if (fileNames != ()) and (fileNames not in ['', None]):
		for f in fileNames:
			print(("Queuing file for backup: " + f))
			shconfig.backJobs.append(f)
		if (backuphoard.backup.running == 0):
			backuphoard.queueRun()

def guiGetFiles():
	fileName = tkinter.simpledialog.askstring("Retrieve File", "Enter file name")
	badChar, spaces = fileutilhoard.checkFileNameChar(fileName)
	if (badChar != 1):
		fileInfo = dbhoard.getFileInfo(fileName)
		if fileInfo == 1:
			print("File not found, searching for close matches...")
			closeF = searchForFiles("",fileName,orderByBatch=False)
			taggedF = searchForFiles(fileName, "", orderByBatch=False)
			closeList = "\n### FILE NOT FOUND. LIST OF CLOSE MATCHES ###\n\n"
			D = tkinter.Tk()
			S = tkinter.Scrollbar(D)
			T = tkinter.Text(D)
			T.insert(tkinter.END, "\n")
			S.pack(side=tkinter.RIGHT, fill=tkinter.Y)
			T.pack(side=tkinter.LEFT, fill=tkinter.Y)
			S.config(command=T.yview)
			T.config(yscrollcommand=S.set)
			if closeF != []:
				for c in closeF:
					closeList = closeList + c + "\n\n"
			if taggedF != []:
				closeList = closeList + "\n### FILES WITH MATCHING TAG ###\n"
				for t in taggedF:
					closeList = closeList + t + "\n\n"
			T.insert(tkinter.END, closeList)
			
		else:
			shconfig.retrieveJobs.append(fileName)
			if (backuphoard.backup.running == 0):
				backuphoard.queueRun()
		#if chunk is not found at location
	else:
		print("Bad input. The following characters are not allowed:")
		print(["\"", "/", "\\", "%", "?", "*", ":", "|", "<", ">", "\'"])
			
		
def guiGoCLI():
	shconfig.guiObj.destroy()
	print("\nType ? for help and q to quit")
	mainLoopCLI()

if len(sys.argv) >= 2:
	commandArg = sys.argv[1]
else:
	commandArg = "none"
	
def showStorage(silent=False):
	qry1 = "select directory, sshhost, sshport, sshuser, uid, readonly from storage"
	r1 = dbhoard.dbQry(shconfig.SELECTED_DB, qry1)
	seq = 0
	readOnly = "No"
	try:
		if (silent == False):
			inBox('List of storage locations', bottomConnection=3)
			for s in r1:
				p = "   Selection #: " + str(seq) + " , Storage Directory: " + s[0] 
				if s[1] != "Nil": 
					url = s[3] + "@" + s[1] + ":" + str(s[2])
					p = p + ", SSH address: " + url
				if str(s[5]) == "1":
					readOnly = "Yes"
				else:
					readOnly = "No"
				p2 = "   Storage UID: " + s[4] + " , ReadOnly: " + readOnly
				underBox(p, space=3, end=False, nextLine=False)
				underBox(p2, space=3, end=False, nextLine=True)
				seq = seq + 1
			underBox("   *STORAGE LOCATIONS*", space=3, end=True)
	except Exception as e:
		print(e)
		return r1
	return r1

def showTags():
	b = inBox('List of all tags for files', bottomConnection=3, returning=True)
	print(("""{}""".format(b)))
	r1 = dbhoard.dbQry("sctr.db", "select tag from tags")
	if r1 != []:
		s1 = set(r1)
		r2 = list(s1)
		for counter, r in enumerate(r2):
			if counter == (len(r2) -1):
				underBox(r[0], space=3, end=True)
			else:
				underBox(r[0], space=3)
	else:
		underBox('No tags have been created.', space=3, end=True)
		

def printInPlace(s):
	if (commandArg != "backup") and (commandArg != "retrieve"):
		sys.stdout.write(s + "                       \r")
		sys.stdout.flush()
	return s + "                        \r"

	
def dirBatch(command):
	filenames = []
	
	for c in command:
		if c == '-t':
			shconfig.addTagsCheck = True
		if c not in ['-t', 'batch']:
			dirArg = c
			dirArg = fileutilhoard.quoteScan(dirArg)
	
	if shconfig.args.tags:
		shconfig.addTagsCheck = True
	
	b = backuphoard.backup()
	try:
		dir = next(os.walk(dirArg))
	except OSError as e:
		print(e)
		return 1
	for file in dir[2]:
		filePath = os.path.join(dirArg, file)
		bResult = b.backUpRun(filePath,batch=1)
		if bResult == 0:
			filenames.append(file)
		#if (frame.dlg1): frame.dlg1.Destroy()
	if (shconfig.addTagsCheck == True) and (len(filenames) > 0):
		for f in filenames:
			justFileName = os.path.basename(f)
			if shconfig.args.tags:
				tags = shconfig.args.tags
				#tags handled in the backuprun() method, but may need to be run again if original batch job failed before tags recorded.
			else:
				print(("Set tags for: " + justFileName))
				print("Seperate with spaces, tags cannot contain spaces")
				tags = input()
				tags = tags.split()
			badChar1, onlySpace1 = fileutilhoard.checkFileNameChar(tags)
			if badChar1 != 0:
				print(("You entered an invalid character for tags. No tags will be added for this file, but tags can be added later. ? or help for more info: " + "\"/\\%?*:|<>\'"))
			for tag in tags:
				scannedTag = fileutilhoard.quoteScan(tag)
				dbhoard.addTag(justFileName, scannedTag)
	shconfig.addTagsCheck = False	


def getQuoteStr(q):
	#takes a string and return list of space delimited strings. Spaces between quotes do not delimit.
	#this is used for filenames stored that have spaces in the filename
	quoteFlag = False
	quoteStr = ""
	q2 = ""
	quoteList = []
	#if (q[0] == "\"") and (q[len(q)-1] == "\""):
	#	q = q[1:]
	#	q = q[:len(q)-1]
	#for c in q:
	#	print type(q)
	#	print c + "a"
	#	if isinstance(c, unicode):
	#		q2 = unicode(q)
	#		print type(q2)
	#		print "unicode"
			#continue
	for c in q:
		if (c not in [" ", "\""]) and (quoteFlag == False):
			quoteStr = quoteStr + c
		if (c == " ") and (quoteFlag == False):
			if (quoteStr != "") and (quoteStr != " "):
				quoteList.append(quoteStr)
			quoteStr = ""
		if c in ["\""]:
			#"'",
			if quoteFlag == False:
				quoteFlag = True
			else:
				quoteFlag = False
				quoteList.append(quoteStr)
				quoteStr = ""
		if (c not in ["\""])and quoteFlag == True:
			#"'", 
			quoteStr = quoteStr + c
	if (quoteStr != "") and (quoteStr != " "):
		quoteList.append(quoteStr)
	return quoteList

def searchForFiles(txtTag,txtName,orderByBatch=False):
	print(txtTag)
	print(txtName)
	badChar1, onlySpace1 = fileutilhoard.checkFileNameChar(txtTag)
	badChar2, onlySpace2 = fileutilhoard.checkFileNameChar(txtName)
	tags = txtTag.split()
	nameSearch = txtName.split()
	notSet = []
	notFlag = False
	andFlag = False
	lastEntryOnlySingleComma = False
	filenames1 = []
	orList = []
	andList = []
	notList = []
	orString = ""
	andString = ""
	notString = ""
	if (badChar1 == 1) or (badChar2 == 1):
		print("Characters not allowed for tags or filenames were provided.")
		return 1
	if (onlySpace1 == 1) and (onlySpace2 == 1):
		fnames3 = []
		qry3 = "select filename from files"
		r3 = dbhoard.dbQry(shconfig.SELECTED_DB, qry3)
		set3 = set(r3)
		fnames3 = list(set3)
		fnames3.sort()
		for filename in fnames3:
			filenames1.append(filename[0])
	if onlySpace1 == 0:
		if tags[len(tags) -1] in ["not", "or", "and", "NOT", "OR", "AND"]:
			tags.remove(len(tags - 1))
		for t in tags:
			fileNameLen = len(t)
			if lastEntryOnlySingleComma == False:
				commaFlag = False
			lastEntryOnlySingleComma = False	
			if (t[fileNameLen - 1] == ","):
				commaFlag = True
				if (t == ",") and (fileNameLen == 1):
					lastEntryOnlySingleComma = True
					continue #just a comma, moving on to next loop iteration with commaFlag in place
			if (t not in ["not", "NOT", "and", "AND"]) and (andFlag == False) and (notFlag == False):
				orList.append(t)
			if notFlag == True:
				if commaFlag == False: #allow comma seperated values to extend NOT and AND boolean operators...
					notFlag = False
				notList.append(t)
			if andFlag == True:
				if commaFlag == False:
					andFlag = False
				andList.append(t)
			if (t == "not") or (t == "NOT"):
				notFlag = True
			if (t == "and") or (t == "AND"):
				andFlag = True

		counter = 0
		orListLen = len(orList) - 1
		andListLen = len(andList) - 1
		notListLen = len(notList) - 1
		for i in orList:
			if counter != orListLen:
				orString = orString + "\'" + i + "\', "
			else:
				orString = orString + "\'" + i + "\'"
			counter = counter + 1
		counter = 0
		for i in andList:
			if counter != andListLen:
				andString = andString + "\'" + i + "\', "
			else:
				andString = andString + "\'" + i + "\'"
			counter = counter + 1
		counter = 0
		for i in notList:
			if counter != notListLen:
				notString = notString + "\'" + i + "\', "
			else:
				notString = notString + "\'" + i + "\'"
			counter = counter + 1
		if orString == "":
			orString = "\'\'"
		if andString == "":
			andString = "\'\'"
		if notString == "":
			notString = "\'\'"
		q = ("select filename from tags where tag IN ({});").format(orString)
		r = dbhoard.dbQry(shconfig.SELECTED_DB, q)
		for r2 in r:
			filenames1.append(r2[0])
		q = ("select filename from tags where tag IN ({});").format(andString)
		r = dbhoard.dbQry(shconfig.SELECTED_DB, q)
		fileNamesTemp = []
		for r2 in r:
			if r2[0] in filenames1:
				fileNamesTemp.append(r2[0])
		if andString != "''":
			filenames1 = fileNamesTemp
		q = ("select filename from tags where tag IN ({});").format(notString)
		r = dbhoard.dbQry(shconfig.SELECTED_DB, q)
		filenames1 = set(filenames1) #remove duplicates
		filenames1 = list(filenames1)
		for r2 in r:
			if r2[0] in filenames1:
				filenames1.remove(r2[0])
	
	if onlySpace2 == 0:
		q = ("select filename from files where filename LIKE '%{}%'").format(str(nameSearch[0]))
		r = dbhoard.dbQry(shconfig.SELECTED_DB, q)
		for r2 in r:
			filenames1.append(r2[0])
	
	if orderByBatch:
		q = "select filename, batchNum from files ORDER by batchNum;"
		r = dbhoard.dbQry(shconfig.SELECTED_DB, q)
		batchFileNames = []
		currentBatch = 0
		for r2 in r:
			for f in filenames1:
				if f == r2[0]:
					if currentBatch != r2[1]:
						currentBatch = r2[1]
						batchFileNames.append("<<< BATCH " + str(currentBatch) + " >>>" )
					batchFileNames.append(f)
		filenames1 = batchFileNames
	return filenames1	
							
def mainLoopCLI():
	while True:
		
		shconfig.command = input('\nsctr:')
		command = shconfig.command
		command = getQuoteStr(command)
		if (command == "") or(command == []):
			continue
		if(command[0] == 'q' or command[0] == 'quit'):
			break
		if(command[0] in ['clr', 'clear']):
			clearScreen()
		if(command[0] == '?' or command[0] == 'help'):
			if len(command) == 1:
				print("To view help on a command or topic below type \'help <command or topic>\'")
	
				inBox('Help Topics: Main Commands', bottomConnection=3)
				underBox('Getting started: start, config, command-args', space=3)
				underBox('Putting files into storage: put, backup, batch', space=3)
				underBox('Retrieving files from storage: get, retrieve, batch, acorn', space=3)
				underBox('Health Checking stored files: health-check, scrub', space=3)
				underBox('Managing storage: add-storage, remove-storage, show-storage, storage-ro-flag', space=3)
				underBox('Tagging files in storage: add-tag, show-tags', space=3)
				underBox('Finding files stored: search, show-files, show-chunks, catalogue, file-stat', space=3)
				underBox('Deletion and Utility: delete, delrow, delete-chunk-table, delete-file-table, show-no-hash', space=3)
				underBox('Start GUI interface for Scatter-Hoard: gui', space=3)
				underBox('Clearing shell screen: clr or clear', space=3)
				underBox('Temporary operation commands: writeto, clr, gui', space=3)
				underBox('Close program: q, quit', space=3, end=True)
				
			else:
				
				if command[1] in ['acorn']:
					print('''
	acorn 
		Arguments : 1 number
		Takes a number as an argument. This creates an "acorn file" by randomly copying chunk files in your data set and putting them
		in a .tar file. This is useful for "cold storage" such as old hard drives you might not want connected to your PC or
		for unsed blank CD's. 
		Each time a chunk is added to an acorn its acorn weight is incremented by one. Chunks with lower acorn
		weights take precedence to ensure that the more acorn files you produce the better redundancy exists.
					''')
				
				
				if command[1] in ['add-storage']:
					print('''
	add-storage
		Arguments : 1 directory path, 1 optional ssh host, 1 optional ssh user, 1 optional ssh port
		This command records a directory as a storage location for your data set. A file called UID.txt is written to this directory
		to identify this directory as a storage location (useful if your path changes or needs to be moved to different drives).
		SSH servers can be added by providing the arguments for the ssh host (domain or IP), the ssh user this program will login as
			and the option ssh port (defaults to 22)
		When accessing the ssh storage for the first time in a session it will require the password for the username provided. This will be storage in 
			RAM and further access will not require it again.
		SSH storage requires the paramiko library to be installed. Placing the paramiko library folder in the same directorya s scatter-hoard will work
			as well as using utilities like pip3.
		Example: add-storage /home/username/archive1 
		Example: add-storage /home/username/archive2 192.168.1.102 remoteusername 22
		Related: remove-storage 
					''')
		
				if command[1] in ['add-tag', 'at']:
					print('''
	add-tag, at
		Arguments : 1 or more
		Takes space seperated descriptions for a file. Files can be searched for using the tag system using the -t flag with the
		search command. Any file that has the tag searched for will be displayed.
		-id allows you to select the file by id number. A seperate prompt will be given to retrieve tags for that file.
		Example: add-tag some-file-to-describe tag1 tag2 tag3 
		Example: at -id 100 
		Related: search, s
					''')
					
				if command[1] in ['batch', 'b']:
					print('''
	batch, b
		Arguments: 1 directory, option -t
		Takes a directory and non recursively finds all listed files and attempts to put them into the archives. It will identify files already present in the
		archive by both hash and file name. It will not traverse subdirectories.
		-t files that are successfully put into the archives will prompt the user to assign them a tag
		Example: batch /home/yourusername/batch1
		Example: b -t /home/yourusername/batch2
		Related: put
					''')
				
				
				
				if command[1] in ['command-args']:
					print('''
	Command line arguments can be viewed by using \'python3 scatterhoard.py --help\'			
					''')
				
				if command[1] in ['config']:
					print('''
	config
		Arguments: None OR 1 option plus 1 variable
		Shows the current configuration for your archives.
		Configuration options can be changed with the config command followed by the option.
		Example: config writedir /home/username/somedirtowriteto
		Example: config
					''')
					
				if command[1] in ['get', 'retrieve', 'g']:
					print('''
	get, retrieve, g
		Arguments: 1 ... n, filenames, delimited by spaces, -id, -b
		Retrieves a file from online storage locations, picking chunks from random storage locations.
		Hash's completely retrieved file to confirm file isn't corrupted.
		Files with spaces in their name are required to be enclosed in double quotes. 
		Two flags are avialable, -id and -b. -id allows retrieval of file by its file ID number in the DB which can be found by commands show-files or search
		-b is the batch flag, when run with a number will get the batch of files with that same number as batch ID. 
		Example: get "Space infilename.txt" someimage.png thirdfiletoberetrieve.pdf (retrieves 3 files with the 3 listed file names)
		Example: retrieve -b 3   (retrieves all files in the 3rd batch of files put into the archives)  
		Example: g -id 44  (retrieves the 44th file added to the archive, does not requires the filename)
		Related: show-files, search
					''')
				
				if command[1] in ['health-chunk', 'scrub']:
					print('''
	health-check, scrub
		Arguments : none or 1 number
		health-check will detect errors in randomly selected chunks and will be done up to the number provided. If no argument is
		provided health-check scrubs the entire data set for errors. scrubbing can take a very long time depending on the size
		of the data set. 
		When errors are detected the bad chunk is deleted (moved to the trash directory located on the storage location directory)
		and then is replaced with a known good chunk elsewhere. This is what is refered to as "self healing" in data storage.
		Alias: scrub
					''')
					
				if command[1] in ['put', 'backup', 'p']:
					print('''
	put, backup, p
		Arguments: 1 ... n, fullpath of files, delimited by spaces, -t
		Hashes files, chops the files in to 100MB chunks (if needed) and delivers multiple copies of them to randomly selected storage locations. Meta data is stored in
		database and is appended to each file chunk. Meta data is used to verify non-corruption of files.
		The full path of files is requires. Paths with spaces in them must be enclosed in double quotes.
		The flag -t will cause the backup procedure to ask for descriptive tags for the archived files. These tags are searchable with the search command.
		Tags can be added later at any point with the add-tag command
		Examples: put /home/yourname/file.ext
				  backup /home/yourname/file.ext /home/yourname/file2.ext -t   (puts two files, requires tags for them)
				  p /home/yourname/file.ext
		Tips: In linux it you can usually right click a file in your file manager, copy it, then control-shift-v to paste the file path into scatter-hoard.
		Related: search, add-tag, gui
					''')

				if command[1] in ['remove-storage']:
					print('''
	remove-storage
		Arguments: none
		Removes the reference in the database to the storage location. This does not delete any information, the unique identifier in that storage location (uid.txt)
		or any user data. This can be revered by using add-storage.
		Related: show-storage, add-storage, storage-ro-flag
				''')
			
				if command[1] in ['search', 's']:
					print('''
	search, s
		arguments: one string, -g, -r, -t, -h
		Finds all files storage in the archives with the string supplied that is present in the file name.
		-t searches by the tags of files instead of the file name.
		-h searches by partial hash of file. You can view hashes with show-files, search and some error messages
		-g or -r will prompt the user if they'd like to retrieve all the files returned by the search. 
		Example: search somestring
		Example: s -t sometag
		Example: s -h 0eae2 -g  (searching by substring of hash and "getting" all results)
		Related: show-files, search
		
					''')
			
				if command[1] in ['show-files']:
					print('''
	show-storage
		Arguments: none
		Completely dumps the list of files stored and recorded in the database. Displays a list of 10 at a time. To find a particular file use the search command.
		Related: search, show-chunks, file-stat 
				''')
				
				if command[1] in ['show-no-hash']:
					print('''
	show-no-hash
		Arguments: none
		Shows a list of files recorded in the database that have no filehash attribute. This is often the result of a put command that is interrupted before
		completion. You can use this to get the ID of a file and then use the delete command with the ID as delete target
		Related: delete
					''')
				
				if command[1] in ['start']:	
					inBox('Fundamentals', bottomConnection=3)
					underBox("1. Before use, at least 3 file storage locations must be designated using the 'add-storage' command. See 'help add-storage' for more information.", space=3)
					underBox('2. Filenames with spaces must be enclosed in double quotes.', space=3)
					underBox('3. The following characters are not allowed in filenames and directories used: ', space=3)
					underBox('" \" ", "/", "\\", "%", "?", "*", ":", "|", "<", ">"', space=3)
					underBox("4. Any number of storage locations can be utilized which includes directories in the local file system or remote directories accessed via SSH.", space=3, end=True)

				if command[1] in ['storage-ro-flag']:
					print('''
	storage-ro-flag
		Arguments: none
		Selects a storage location and flips the read only flag on or off.
		A read only storage location means file chunk data can be retrieved from the storage location but any new data being put into the system will avoid
		all read only storage locations as a place to store chunks. This is used for online storage locations that have no more storage left or you want to
		use the rest of the storage for something else. The flag is shown in show-storage.
		Related: show-storage, add-storage, remove-storage
				''')

				if command[1] in ['writeto']:
					print('''
	writeto
		Arguments: one directory
		Temporarily sets the writing directory for scatter-hoard. What this means is that if you retrieve a file from the archives with get or g it will
		write the desired file to the directory set with writeto.
		Upen exiting the software this change is lost. To set it permanently use the config command.
		Example: writeto /home/username/somedirtowriteto
		Related: config
					''')
					
		if(command[0] == 'commands'):
			print('''
			
				
				

				
			remove-storage
				Arguments : 0
				This command first displays the available storage locations that scatter-hoard uses and then allows the user to select 
				one to be removed from the DB. This does not delete the directory, UID.txt or any data. It simply scrubs the pointer to it
				in the storage locations table in the DB. Any further get or put commands will not use this directory until it is re-added.
				Related: add-storage, add-sftp
			
			''')

		if(command[0] == 'acorn'):
			if len(command) < 2:
				print("acorn requires a number of chunks to be backed up")
				continue
			command1 = int(command[1])
			fileutilhoard.acorn(command1)
		if(command[0] == 'idgen'):
			print((fileutilhoard.idGen(100)))
		if command[0] in ['backup', 'put', 'p']:
			tagPut = False
			backingup = backuphoard.backup()
			for c in command:
				if c in ["--tag", "-t"]:
					tagPut = True
			for c in command:
				if c not in ["p", "put", "backup", "-t", "--tag"]:
					backingup.backUpRun(c)
			for c in command:		
				if (tagPut == True) and (c not in ['p','put','backup','-t','--tag']):
					tag = input('\nPlease supply a tag for the file ' + c + '\nSeperate tags by a space if there are multiple\n ')
					tag = tag.split()
					justFileName = os.path.basename(c)
					justFileName = fileutilhoard.quoteScan(justFileName)
					for t in tag:
						dbhoard.addTag(justFileName, t)
			if (shconfig.backJobs != []) or (shconfig.retrieveJobs != []):
				queueRun()
		if (command[0] == "delete"):
			delID = False
			delBatch = False
			delFileList = []
			delCompleted = False
			store = backuphoard.initStorage(addReadOnly=True)
			for a in command:
				if a == "-id":
					delID = True
				if a == "-b":
					delBatch = True
				if a not in ["-id", "delete", "-b"]:
					if delID == True:
						fileName = dbhoard.getFileNameFromID(a)
						dbhoard.removeFileFromDB(fileName,store)
						delCompleted = True
					if delBatch == True:
						delFileList = dbhoard.getAllFromBatch(a)
						for f in delFileList:
							dbhoard.removeFileFromDB(f, store)
						delCompleted = True
					if delCompleted == False:
						dbhoard.removeFileFromDB(a, store)
		if (command[0] == "gui"):
			mainGUI()
		if command[0] in ['retrieve', 'get', 'g']:
			commandLen = len(command)
			if 1 == commandLen:
				print("Retrieve requests require a filename. You can find file using the search and show-files commands")
				print("For long filenames you may enter the file ID number shown on search of show-files results using idget")
			else:
				getFiles = backuphoard.backup()
				batch = False
				fileID = False
				for a in command:
					if a not in ["get", "retrieve", "g"]:
						if a == "-b": #-b for batch
							batch = True
						if a == "-id": #-id for fileidnumber
							fileID = True
				for a in command:
					if a not in ["get", "retrieve", "-b", "-id", "g"]:
						if batch == True:
							fileList = dbhoard.getAllFromBatch(a)
							for f in fileList:
								getFiles.retrieve(f)
						if (fileID == True) and (batch == False):
							fileName = dbhoard.getFileNameFromID(a)
							getFiles.retrieve(fileName)
				if (batch == False) and (fileID == False):
					for a in command:
						if a not in ["get", "retrieve", "g"]:
							getFiles.retrieve(a)
			if (shconfig.backJobs != []) or (shconfig.retrieveJobs != []):
				backuphoard.queueRun()

		if command[0] == "add-storage":
			sshhost = "Nil"
			sshuser = "Nil"
			sshport = 22
			if len(command) == 1:
				print("Please provide a storage location as an argument")
				print("add-storage <directory> <optional ssh host> <optional ssh user> <optional ssh port>")
				continue
			if len(command) ==  3:
				#print "Is " + command[2] + " an remote location accessable with SSH?"
				sshhost = command[2]
			if len(command) == 4:
				sshhost = command[2]
				sshuser = command[3]
			if len(command) == 5:
				sshhost = command[2]
				sshuser = command[3]
				sshport = int(command[4])
			dbhoard.addStorageLocation(command[1], sshhost=sshhost, username=sshuser, sshport=sshport)
			showStorage()
		if (command[0] == "health-check") or (command[0] == "scrub"):
			b = backuphoard.backup()
			if len(command) > 1:
				try:
					if (int(command[1]) > 0):
						b.globalCheck(int(command[1]))
					else:
						print("Scrub requires a number as an argument")
				except KeyboardInterrupt:
					print("PROCESS HALTED")
				
					
			else:
				print("BEGINNING GLOBAL HEALTH CHECK... THIS MAY TAKE A LONG TIME... CTRL-C TO EXIT EARLY")
				b.globalCheck(0)
			continue
		if command[0] == "remove-storage":
			storageList = showStorage()
			print("   Please enter the selection number to remove that storage location from the list of available storage location.")
			print("   Leave blank for none")
			command2 = eval(input())
			if command2 == "":
				continue
			else:
				try:
					command2 = int(command2)
				except:
					print("selection must be a number")
					continue
				if (command2 >= len(storageList)) or (command2 < 0):
					print("Selection is out of range")
					continue
				qry1 = "delete from storage where directory = '" +  fileutilhoard.quoteScan(str(storageList[command2][0])) + "';"
				r1 = dbhoard.dbQry(shconfig.SELECTED_DB, qry1)
				if r1 != []:
					print(str(r1))
				print('\n')
				showStorage()
				continue
		if command[0] == "show-tags":
			if len(command) > 1:
				#specificTag = command[1]
				#showSpecificFile(command[1])
				fileList = []
				secondList = []
				firstRound = True
				for tag in command:
					if tag == command[0]:
						continue
					q = "select filename from tags where tag = '" + tag + "';"
					r = dbhoard.dbQry(shconfig.SELECTED_DB, q)
					if r != []:
						for fn in r:
							if firstRound == True:
								fileList.append(fn[0])
							else:
								if fn[0] in fileList:
									secondList.append(fn[0])
					if firstRound == False:
						fileList = []
						for fn in secondList:
							fileList.append(fn)
						secondList = []
					firstRound = False
				fileSet = set(fileList)
				fileList2 = list(fileSet)
				for fn in fileList2:
					print(fn)
				del fileSet, fileList2, fileList
			if len(command) == 1:
				showTags()
		if command[0] == "show-files":
			showFilesPaged(dbhoard.dumpFileList())
			
		if command[0] == "show-no-hash":
			noHash = dbhoard.showNoHash()
			for n in noHash:
				print("Filename: " + n[0] + " , ID: " + str(n[3]))
			
		if command[0] == "delete-file-table":
			print("This command only deletes the metadata in the database about your files and not the files themselves.")
			print("Type 'I understand' to continue")
			command2 = input('\n:')
			if command2 == "I understand":
				qry1 = "delete from files"
				r = dbhoard.dbQry(shconfig.SELECTED_DB, qry1)
				print(r)
			else:
				print("Aborted")
				
		if command[0] == "delete-chunk-table":
			print("This command only deletes the metadata in the database about your file chunks and not the chunks themselves.")
			print("Type 'I understand' to continue")
			command2 = input('\n:')
			if command2 == "I understand":
				qry1 = "delete from fileLoc"
				r = dbhoard.dbQry(shconfig.SELECTED_DB, qry1)
			else:
				print("Aborted")
		if command[0] in ["add-tag", "at"]:
			idTag = False
			tagFileName = ''
			if len(command) < 3:
				print("Usage: add-tag <filename> <tags>")
				continue
			for c in command:
				if c == '-id':
					idTag = True
				if (c not in ['add-tag', '-id']) and (idTag == True):
					try:
						idInt = int(c)
					except:
						continue
					tagFileName = dbhoard.getFileNameFromID(idInt)
					tagInput = input('\nEnter tags for file : ' + tagFileName + '\nSeperate tags, if multiple, with spaces\n')
					tagInput = tagInput.split()
					for t in tagInput:
						dbhoard.addTag(tagFileName, t) 
			for arg in command:
				if (arg != "add-tag") and (arg != command[1]) and (idTag == False):
					tagFileName = command[1]
					dbhoard.addTag(tagFileName, arg)
		if command[0] == "show-storage":
			showStorage()
		if command[0] == "show-online-storage":
			storage = backuphoard.initStorage(addReadOnly=True)
			for s in storage:
				print(s)
		if command[0] in ['batch', 'b']:
			if len(command) < 2:
				print("Usage: batch <-t optional> <directory")
				print("-t adds tags to each file backed up individually once backup completes")
				continue
			dirBatch(command)
				
		if command[0] == "delrow":
			r1 = dbhoard.delRow(command[1])
			print(r1)
		if command[0] == "show-chunks":
			#inBox('File chunks stored')
			dbhoard.dumpChunkList()
		if command[0] == "catalogue":
			storageList = showStorage()
			print("\nPlease enter the selection number to catalogue that storage location.")
			print("For multiple storage locations to be catalogued seperate numbers with a space")
			print("Leave blank for none")
			command2 = input()
			command3 = command2.split(" ")
			for c in command3:
				if c == "":
					continue
				try:
					command2 = int(c)
				except:
					print("selection must be a number")
					continue
				if (command2 >= len(storageList)) or (command2 < 0):
					print("Selection is out of range")
					continue
				backuphoard.catalogueDir(storageList[command2][4])
			continue
				
		if command[0] in ["search", "s"]:
			tagSearch = False
			hashSearch = False
			getFiles = False
			totalFileSize = 0
			fileNames = []
			for c in command:
				if c == "-t":
					tagSearch = True
				if c in ['-g', '-r']:
					#retrieves the resulting files of the search
					getFiles = True
				if c == "-h":
					#searches by hash proximity
					hashSearch = True
			for c in command:
				scannedChar = fileutilhoard.quoteScan(c)
				if c not in ["-t", "search", "s", "-g", "-h"]:
					if tagSearch == True:
						#searchResults = searchForFiles(c, "", orderByBatch=shconfig.sortByBatchCheckBox)
						searchResults = dbhoard.readTag(scannedChar)						
						for s in searchResults:
							fileNames.append(s[1])
					elif hashSearch == True:
						searchResults = dbhoard.getHashLike(scannedChar)
						for s in searchResults:
							fileNames.append(s)		
					else:
						#searchResults = searchForFiles("", c, orderByBatch=shconfig.sortByBatchCheckBox)
						searchResults = dbhoard.getFileNameLike(scannedChar)
						for s in searchResults:
							fileNames.append(s)
			counter = 0
			if len(fileNames) != 0:
				inBox('Found Files', bottomConnection=3)
			for f in fileNames:
				#fid = getIDFromFileName(f)
				fileInfo = dbhoard.getFileInfo(f)
				if fileInfo == 1:
					continue
				fid =  fileInfo[0][3]
				batchNum = fileInfo[0][2]
				fileSize = str(fileInfo[0][4])
				totalFileSize = totalFileSize + fileInfo[0][4]
				fileSizeMB = str(round(fileInfo[0][4]/1048576)) + " MB)"
				if fileNames != []:
					p = " " + f
					p2 = "   File ID#: " + str(fid) + " , Batch#: " + str(batchNum) +  " , File Size : " + fileSize + "(" + fileSizeMB
				else:
					p = "No files found!"
					p2 = ""
				counter = counter + 1
				#if counter == len(fileNames):
				underBox(p, end=False, space=3)
				underBox(p2, end=False, space=3, nextLine=True)
				underBox("   File Hash: " + fileInfo[0][1], space=3, end=False, nextLine=True)
				#else:
			underBox("   End of files", space=3, end=True)
			if getFiles == True:
				totalFileSizeMB = str(round(totalFileSize/1048576)) + " MB"
				print('\nTotal filesize of search results: ' + totalFileSizeMB)
				responseGet = input('Would you like to retrieve the above list of files into your current directory?')
				if responseGet in ['y', 'Y', 'yes', 'Yes']:	
					b = backuphoard.backup()
					for f in fileNames:
						b.retrieve(f)
			continue
			
		if command[0] == "file-stat":
			if len(command) >= 2:
				counter = 0
				fileList = []
				batch = False
				fileID = False
				healthCheck = False
				for a in command:
					if a not in ["file-stat"]:
						if a == "-b": #-b for batch
							batch = True
						if a == "-id": #-id for fileidnumber
							fileID = True
						if a == "-h": #-h for health check
							healthCheck = True
				for a in command:
					if a not in ["file-stat", "-b", "-id", "-h"]:
						if batch == True:
							try:
								b = int(a)
								fileList = fileList + dbhoard.getAllFromBatch(b)
							except:
								pass
						if fileID == True:
							try:
								b = int(a)
								fileList.append(dbhoard.getFileNameFromID(b))
							except:
								pass
						if (fileID == False) and (batch == False):
							fileList.append(a)
				for f in fileList:
					chunkList = dbhoard.getChunkList(f)
					if chunkList == []:
						break
					inBox(f, bottomConnection=3)
					counter = counter + 3
					for c in chunkList:
						dirStored = dbhoard.getDirectoryStorage(c[2])	
						if (healthCheck == True) and (c[3] == "Nil"):
							healthy = fileutilhoard.healthCheckSingleChunk(dirStored[0] + shconfig.wack + c[0], c[2], dirStored[1], quiet=True)
						underBox(" " + dirStored[0] + shconfig.wack + c[0], end=False, space=3, nextLine=False)
						if dirStored[1] != "Nil":
							counter = counter + 1
							underBox("   Directory is remote on this server: " + dirStored[1], end=False, space=3, nextLine=True)
						underBox("   Chunk hash: " + c[1] , space=3, nextLine=True, end=False)
						underBox("   Storage Location ID: " + c[2] ,space=3, nextLine=True, end=False)
						if (healthCheck == True) and (c[3] == "Nil"):
							if healthy == 0:
								counter = counter + 1
								underBox("   *FILE CHUNK HAS FAILED A HEALTH CHECK*", space=3, nextLine=True, end=False)
							if healthy == 1:
								underBox("   Chunk is healthy, no errors reported...", space=3, nextLine=True, end=False)
						if c[3] != "Nil":
							#print(('     \\u255f\\u2500Duplicate of chunk: ' + c[3]))
							counter = counter + 1
							underBox("   Duplcate chunk of : " + c[3], space=3, nextLine=True, end=False)
						#print(('     \\u255f\\u2500File Chunk: ' + c[0]))
						#print(('     \\u255f\\u2500Chunk hash: ' + c[1]))
						#print(('     \\u2559\\u2500Storage location ID#: ' + c[2]))
						#for c2 in c:
						#	c2 = c2 + " " + c2 + " "
						counter = counter + 3
						#if counter >= 40:
						#	counter = 0
						#	#com = eval(input("\nPress enter key to continue..."))
						#	com = input(underBox("   ***Press enter key to continue***", space=3,nextLine=False,end=False))
				if healthCheck == True:
					underBox("   End of File Health Check", space=3, nextLine=True, end=True)
				else:
					underBox("   End of File Stat", space=3, nextLine=True, end=True)
			else:
				print("Stat requires one filename as argument")
				continue
				
		if command[0] == "config":
			changeWriteDir = False
			changeWorkingDB = False
			argCount = 0
			if len(command) == 3:
				if command[1] == "-b":
					argCount+= 1
					if command[2] == "on":
						shconfig.sortByBatchCheckBox = True
						print("Sorting by batch number is on")
					else:
						shconfig.sortByBatchCheckBox = False
						print("Sorting by Batch number is off")
				for c in command:
					if c in ["writedir"]:
						changeWriteDir = True
						argCount+= 1
				for c in command:
					if c in ['workingDB']:
						changeWorkingDB = True
						argCount+= 1
				if argCount > 1:
					print("Only one option can be changed at a time.")
					continue
				for c in command:
					if c not in ["config", "writedir", "workingDB"]:
						if changeWriteDir == True:
							dbhoard.updateConfig('retrieveWriteDir', c)
							shconfig.retrieveWriteDir = c
						if changeWorkingDB == True:
							dbhoard.updateConfig('workingDatabase', c)
							shconfig.workingDatabase = c
							results = dbhoard.getConfig()
							shconfig.databasePath = results[0][3]
							shconfig.retrieveWriteDir = results[0][1]
							shconfig.dbType = results[0][4]
							shconfig.numOfChunkCopies = results[0][6]
							shconfig.bytesStored = results[0][8]
							shconfig.baseBytesOfFiles = results[0][9]
							shconfig.sshCom = results[0][7]
							shconfig.dbType = results[0][5]
							shconfig.SELECTED_DB =  shconfig.databasePath + shconfig.wack + shconfig.workingDatabase
							dbhoard.initDB()
			else:
				chunkTotal = dbhoard.incrementStoredBytes(0)
				fileTotal = dbhoard.incrementFileTotalBytes(0)
				inBox('Config', bottomConnection=3)
				underBox("   [sortbatch] Sort by batch: " + str(shconfig.sortByBatchCheckBox))
				underBox("   [workingDB] Current working database: " + shconfig.workingDatabase)
				underBox("   [writedir] Write retrieved files to: " + shconfig.retrieveWriteDir)
				underBox("   [replicate] Replication number of chunks files: " + str(shconfig.numOfChunkCopies)) 
				underBox("   Total bytes of stored data chunks: " +str(chunkTotal) + " ( ~" + str(round(chunkTotal/1048576)) + " MB) ")
				underBox("   Total bytes of files stored (Non redundant dataset): " + str(fileTotal) + " ( ~" + str(round(fileTotal/1048576)) + " MB) ")
				underBox("   Items in brackets are editable with the config command. Add them as arguments to the \'config\' command. ", end=True)
				continue
		if command[0] == "writeto":
			#temporarily sets the directory to write retrieved files to. resets upon closing program.
			if len(command) == 2:
				shconfig.retrieveWriteDir = command[1]
			
		if command[0] == "errors":
			for e in shconfig.errQ:
				print((e + "\n"))
		if command[0] == "storage-ro-flag":
			storageList = showStorage()
			print("   Please enter the selection number to change the read only flag.")
			print("   Leave blank for none")
			command2 = eval(input())
			setRO = 1
			if command2 == "":
				continue
			else:
				try:
					command2 = int(command2)
				except:
					print("selection must be a number")
					continue
				if (command2 >= len(storageList)) or (command2 < 0):
					print("Selection is out of range")
					continue
				if storageList[command2][5] == 1:
					setRO = 0
				else:
					setRO = 1
				qry1 = ("update storage set readonly = {} where uid = '{}'").format(setRO, str(storageList[command2][4]))
				r1 = dbhoard.dbQry(shconfig.SELECTED_DB, qry1)
				showStorage()
				continue
			#setReadOnlyStorage(uid, onlineStorage=[], readOnly=1):		
	return 0

def inBox(s, bottomConnection=-1, returning=False):
	#print title in a box
	nameLen = len(s)
	bar = u'\u2554'
	for c in range(0, nameLen + 2):
		bar = bar + u'\u2550'
	bar = bar + u'\u2557'
	if returning == False:
		print(bar)
		bar = ''
	else:
		bar = bar + '\n'
	if returning == False:
		print((u'\u2551 ' + s + u' \u2551'))
	else:
		bar = bar + u'\u2551 ' + s + ' u\u2551' + '\n'
	for c in range(0, nameLen + 4):
		if c == bottomConnection:
			bar = bar + u'\u2566'
		elif c == 0:
			bar = bar + u'\u255A'
		elif c == nameLen + 3:
			bar = bar + u'\u255D'
		else:
			bar = bar + u'\u2550'
	if returning == False:
		print(bar)
	else:
		return bar

def showFilesPaged(fileList):
	counter = 0
	pages = 0
	endFor = False
	for r in fileList:
		endFor = False
		if counter == 0:
			pages = pages + 1
			inBox("List of all files scattered, page (" + str(pages) + ")", bottomConnection=3)
		#print "# " + str(r[0]) + " # " + str(r[1]) + " # " + str(r[2]) + " #"
		#fileInfo = getFileInfo(f)
		fid =  str(r[3])
		batchNum = str(r[2])
		fileSize = str(r[4])
		fileSizeMB = str(round(r[4]/1048576)) + " MB)"
		#print(("File ID#: " + str(fid) + " , FileSize: " + str(fileSize) + " ( ~" + str(round(fileSize/1048576)) + " MB) " + " , Batch#: " + str(batchNum) + " , File Name: " + r[0] + " , File Hash: " + r[1])) 
		underBox(" " + str(r[0]) + " ", space=3, end=False, nextLine=False)
		underBox("   File ID: " + fid + " , File Size: " + fileSize + "(~" + fileSizeMB + " , Batch Number: " + batchNum, space=3, end=False, nextLine=True)
		underBox("   File Hash: " + r[1], space=3, end=False, nextLine=True)
		counter = counter + 1
		if counter == 10:
			endFor = True
			underBox("   Hit ENTER to continue, Q to end command", space=3, end=True)
			i = input()
			if i in ['q', 'Q']:
				return 0
			counter = 0
	if fileList == []:
		print("No files listed")
		return
	if (endFor == False):
		underBox("   End of show files command", space=3, end=True)	

def underBox(s, space=-1, end=False, nextLine=False):
	#print entries under the title box
	bar = ''
	slen = len(s)
	lastSpace = True
	space2 = 0
	if space == -1:
		for c in s:
			if (c == ' ') and (lastSpace == True):
				space2 = space2 + 1
				s = s[1:]
			else:
				lastSpace = False
		space = space2		
	for n in range(0, space):
		bar = bar + " "
	if end == False:
		if nextLine == False:
			bar = bar + u'\u255f\u2500' + s
		else:
			bar = bar + u'\u2551' + s
	else:
		bar = bar + u'\u2559\u2500' + s

	print(bar)

#def underBoxNextLine(s, space=-1):
	


def errQAdd(errMes):
	errQSize = len(shconfig.errQ)
	#add an error message to shconfig.errQ
	if errQSize < 1000:
		shconfig.errQ.append(str(errMes))
	else:
		shconfig.errQ.pop(0)
		shconfig.errQ.append(str(errMes))
		
