#!/usr/bin/python
import platform
import sys
from os import path
from os import system
import sqlite3
#from scatterhoard.dbhoard import initDB


#Configure wack usage based on OS. / for linux/unix/macos and \ for Windows
wack = "/"
systemPlatform = 'Linux'
if platform.system() == 'Windows':
	wack = "\\"
	systemPlatform =  'Windows'
	system("mode 200,200")
#Number of times a chunk from a file is backed up. Default is 3.
numOfChunkCopies = 3

#sctr.db is the default database. It must exist as the main config data is stored there. When using other databases (as a way to have seperate namespaces)
#You set the workingDatabase option to the other database from within config table in db sctr.db
#If ./sctr.db is not found workingDatabase defaults to sctr.db and is then created automatically during initialize() on program startup.
#path is the path to the directory, alter if you have a specific location for your database instead of the current working directory which is default
#directory retrieved files will be written to retrieveWriteDir. Default is cwd of the executable python script.
#Database types includes MySQL, PostGres, Sqlite3, nosql
if not path.isfile('.' + wack + 'sctr.db'):
	workingDatabase = "sctr.db"
	databasePath = "."
	retrieveWriteDir = "."
	dbType = "sqlite3"
else:
	results = []
	c = sqlite3.connect('.' + wack + 'sctr.db')
	cursor = c.cursor()
	q = "select * from config"
	cursor.execute(q)
	c.commit()
	results = cursor.fetchall()
	c.close()
	workingDatabase = results[0][2]
	databasePath = results[0][3]
	if workingDatabase != "sctr.db":
		try:
			c = sqlite3.connect(databasePath + wack + workingDatabase)
			cursor = c.cursor()
			q = "select * from config"
			cursor.execute(q)
			c.commit()
			results = cursor.fetchall()
			c.close()
			databasePath = results[0][3]
		except:
			workingDatabase = 'sctr.db'
	retrieveWriteDir = results[0][1]
	dbType = results[0][4]
	numOfChunkCopies = results[0][6]
	bytesStored = results[0][8]
	baseBytesOfFiles = results[0][9]
	sshCom = results[0][7]
	

#Select Database. Includes current working directory. Alter if you have a specific directory.
SELECTED_DB = databasePath + wack + workingDatabase

#interactive cli switch
command = False

#Whether to add tags to files being backed up. Useful to turn off if you're doing a large batch that don't need tags...
addTagsCheck = False

#When clicking "search" in the retrieve tab whether the search sorts alphabetically or by most recently added
sortByBatchCheckBox = False

#Flag when running acorn command in gui
acornRunning = False

#flag when a bad read/write IO function is detected
failedIO = False

#array showing what storage locations should be treated as read only... locations added when errors in writing occur. 
readOnlyLoc = []

#Job lists for retrieve() and backUpRun()
#these possibly should be moved into main program...
backJobs = []
retrieveJobs = []
tagJobs = []
tagLock = False # used for processing tagJobs, prevents mutple queueRuns stumbling over eachother

#Threading object. Used for running localhost webserver in background
threadH = None

#local webserver port when using the web UI
#webPort = 8127

#GUi availability via Tkinter
#imagePreview requires python-pil.imagetk package under linux
gui = False
guiObj = None
imagePreview = False

#httpd object that serves web UI. Accessed from a thread to prevent non blocking. Currently not used.
httpd = None

#error queue is an array of error messages. Rather than being dumped to stdout some functions will append to this array instead.
#holds 1000 errors before removing old messages
errQ = []

#sshclient variables, passwords, hosts
#multidimensional. Put another list inside sshConnections such that
# [host, username, password, pkey, port]
sshCreds = []
#connection objects. Whenever using ssh functions look through the sshConnections list of objects first
#to determine if a live connection exists to that storage location.
sshConnections = []
#If paramiko is not available this is false. Prevents using ssh storage locations
sshAvailable = False
#If pkey fails to produce a valid key the first time, it will not continuously read from ~/.ssh/id_rsa
sshPkeyFailed = False


