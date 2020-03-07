#!/usr/bin/python
import getpass
import stat
from . import shconfig
from . import fileutilhoard
import os
from . import dbhoard
from io import BytesIO

#wack = shconfig.wack
#SSH is assumed to be running on Mac, BSD or Linux with forward wack "/"
wack = "/"


try:
	import paramiko
	print("SSH support is ON (via paramiko)")
	shconfig.sshAvailable = True
	paramiko.util.log_to_file("paramikoSSH.log")
except:
	print("SSH support is OFF.")
	print("To enable SSH communication please run pip install paramiko or")
	print("place the paramiko directory in the directory of the python executable")


def getRSAKEY():
	k = "Nil"
	#if shconfig.systemPlatform == 'Windows':
	#	id_rsa = ".\\id_rsa"
	#else:
	#	id_rsa = "~/.ssh/id_rsa"
	#print str(os.path.isfile("~/.ssh/id_rsa"))
	#print os.listdir("~/.ssh")
	id_rsa = os.path.expanduser("~/.ssh/id_rsa")
	if os.path.isfile(id_rsa):
		try:
			with open(id_rsa) as f:
				k = f.read()
		except IOError as e:
			print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		f.close()
		shconfig.sshKeys.append(k)
		return k
	else:
		return k

def getChunkMetaDataSFTP(path, sshhost, target):
	sshObj, rFlag = sshCom(sshhost)
	sftpCom  = sshObj.open_sftp()
	filepath = path + wack + target
	with sftpCom.open(filepath, 'rb') as f:
		f.seek(-40, 2)
		hashDataOfFile = f.read()
		f.close()
	sftpCom.close()
	sshObj.close()
	return hashDataOfFile

def getRemoteDupDataSFTP(path, sshhost, target, filesize):
	sshObj, rFlag = sshCom(sshhost)
	sftpCom  = sshObj.open_sftp()
	try:
		f = sftpCom.open(path + wack + target, 'rb')
		dupName = f.read(filesize - 40)
		hashOfOriginalChunk = f.read(20)
		hashOfDupFile = f.read(20)
		f.seek(0)
		dupHashed, metaData, hexDupHashed = fileutilhoard.datHasher("NA", fo=f)
		f.close()
	except:
		print(("Error reading .dup file " + target))
	sftpCom.close()
	sshObj.close()
	return dupName, hashOfOriginalChunk, hashOfDupFile, dupHashed

def doesRemoteFileExist(path, sshhost, target, dirOnly=False, sshuser="Nil", ignoreError=False):
	sshObj, rFlag = sshCom(sshhost, username=sshuser)
	sftpCom = sshObj.open_sftp()
	try:
		if dirOnly == False:
			sftpCom.chdir(path)
			fileStat = sftpCom.stat(target).st_mode
			fileSize = sftpCom.stat(target).st_size
		else:
			fileStat = sftpCom.stat(path).st_mode
	except IOError as e:
		if ignoreError == False:
			print(e)
			print(("Cannot find target/directory: " + path + "/" + target))
			print(("... on : " + sshhost))
		return 0, 0
	sftpCom.close()
	sshObj.close()
	if dirOnly == False:
		return stat.S_ISREG(fileStat), fileSize
	else:
		return stat.S_ISDIR(fileStat)
		
def remoteMoveToTrashSFTP(path, sshhost, target, trash):
	sshObj, rFlag = sshCom(sshhost)
	sftpCom  = sshObj.open_sftp()
	fileStat = sftpCom.stat(path + wack + target).st_mode
	trashStat = sftpCom.stat(trash).st_mode
	fileExists = stat.S_ISREG(fileStat)
	trashExists = stat.S_ISDIR(trashStat)
	if not trashExists:
		try:
			sftpCom.mkdir(trash,mode=664)
		except IOError as e:
			print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
	if not fileExists:
		print(("Chunk missing, marking as a successfull purge anyways... : " + str(target)))
		sftpCom.close()
		sshObj.close()
		return 0
	try:
		sftpCom.posix_rename(path + wack + target, trash + wack + target)
		print(("Moved to trash: " +  trash + wack + target))
		sftpCom.close()
		sshObj.close()
		return 0
	except IOError as e:
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		print(("Error while deleting file: " + str(target)))
		sftpCom.close()
		sshObj.close()
		return 1

def delFileSFTP(pathTarget, sshhost):
	sshObj, rFlag = sshCom(sshhost)
	sftpCom  = sshObj.open_sftp()
	try:
		sftpCom.remove(pathTarget)
		sftpCom.close()
		sshObj.close()
		return 0
	except IOError as e:
		print(("I/O error({0}): {1}".format(e.errno, e.strerror)))
		print(("Error while deleting file: " + str(target)))
		sftpCom.close()
		sshObj.close()
		return 1

def getFileListSFTP(path, sshhost):
	files = []
	sshObj, rFlag = sshCom(sshhost)
	sftpCom  = sshObj.open_sftp()
	sftpCom.chdir(path)
	for i in sftpCom.listdir():
		lstatout=str(sftpCom.lstat(i)).split()[0]
		if 'd' not in lstatout:
			files.append(i)
	return files

def putTextSFTP(path, sshhost, target, text):
	fo = BytesIO()
	fo.write(text)
	fo.seek(0)
	putReturn = putChunkSFTP(path, sshhost, target, fo, integrity=False)
	fo.close()
	return putReturn

def putChunkSFTP(path, sshhost, target, writeFO, integrity=True, silent=0):
	#writeFO is an open fileobject containing chunk data.
	writeFO.seek(0)
	writeFO.seek(0,2)
	writeFO.seek(0)
	try:
		sshObj, rFlag = sshCom(sshhost)
		sftpCom  = sshObj.open_sftp()
		fileToPut = path + wack + target
		sftpCom.putfo(writeFO, fileToPut)
		writeFO.close()
	except:
		print("Failed to FTP chunk data")
		delFileSFTP(path + wack + target, sshhost)
		writeFO.close()
		return 1
	#file has been put, now to pull it back from the path to test for integrity...
	if integrity == True:
		readFO = BytesIO()
		if fileutilhoard.isLastCharWack(path):
			chunkToGet = path + target
		else:
			chunkToGet = path + wack + target
		sftpCom.getfo(chunkToGet, readFO)
		readFO.seek(0)
		hash, metaHash, hexHash = fileutilhoard.datHasher(chunkToGet, fo=readFO)
		sftpCom.close()
		sshObj.close()
		if hash != metaHash[20:]:
			print("Error sending chunk to remote SSH location. Deleting failed chunk...")
			print(hash)
			print((metaHash[20:]))
			delFileSFTP(chunkToGet, sshhost)
			return 1
		else:
			if silent == 0:
				print(("Created dat file: " + sshhost + ":" + path + target)) 
			return 0
	else:
		sftpCom.close()
		sshObj.close()
		return 0
	
def getChunkSFTP(path, sshhost, target):
	#also works with just normal files... doesn't have to be file chunks.
	username, port = dbhoard.getLoginSSH(sshhost, path)
	sshObj, rFlag = sshCom(sshhost, username=username, sshport=port)
	try:
		sftpCom  = sshObj.open_sftp()
	except AttributeError:
		print("Error getting chunk from SSH host. If one or more SSH servers are down please use the command ignore-storage to prevent accessing this device")
		return 1
	fo = BytesIO()
	if fileutilhoard.isLastCharWack(path):
		chunkToGet = path + target
	else:
		chunkToGet = path + wack + target
	sftpCom.getfo(chunkToGet, fo)
	sshObj.close()
	fo.seek(0)
	return fo #file object got
		
def sshCom(sshhost, username="Nil", sshport=22):
	#target, file being get or put via sftp
	#command, sftp or another command?
	#sshport defaults to 22 
	if sshport == 0:
		sshport = 22
	credsActive = False
	password = "Nil"
	pkey = "Nil"
	for c in shconfig.sshCreds:
		if sshhost == c[0][0]: #c[0] is the url of the ssh server storage location
			username = c[1][0]
			if c[2] != "Nil":
				password = c[2][0]
			if c[3] != "Nil":
				pkey =  c[3][0]
			if c[4] != 22:
				port = c[4][0]
			credsActive = True
	if (credsActive == False) and (shconfig.sshPkeyFailed == False):
		try:
			id_rsa = os.path.expanduser("~/.ssh/id_rsa")
			pkey = paramiko.RSAKey.from_private_key_file(id_rsa)
			print("Using RSA private key for authentication. To turn this off type the command \'pkey-off\'")
		except:
			pkey = "Nil"
			shconfig.sshPkeyFailed = True
	if credsActive == False:
		if pkey == "Nil":
			print("No private RSA keys available. When generating private keys please use the command ssh-keygen -t rsa, otherwise Paramiko will reject it...")
		if username == "Nil":
			username = eval(input('\nPlease enter username for accessing SSH storage location ' + sshhost + ':'))
		#this input needs to be sanitized...
		if pkey == "Nil":
			password = getpass.getpass("Password for " +username + " at " + sshhost + " :")
	try:
		#Paramiko.SSHClient can be used to make connections to the remote server and transfer files
		sshObj = paramiko.SSHClient()
		#Parsing an instance of the AutoAddPolicy to set_missing_host_key_policy() changes it to allow any host.
		sshObj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		#Connect to the server
		if (password in ["", "Nil"]) and (pkey != "Nil"):
			sshObj.connect(hostname=sshhost, port=sshport, username=username, pkey=pkey, look_for_keys=False)
			if credsActive == False:
				shconfig.sshCreds.append([[sshhost],[username],["Nil"],[pkey],[sshport]])
		else:
			sshObj.connect(hostname=sshhost, port=sshport, username=username, password=password, allow_agent=False,look_for_keys=False)    
			if credsActive == False:
				shconfig.sshCreds.append([[sshhost],[username],[password],["Nil"],[sshport]])
		transPortObj = sshObj.get_transport()
		#print(transPortObj)
		#print(str(transPortObj.compression))
		#print(str(transPortObj.default_window_size))
	except paramiko.AuthenticationException:
		print("Authentication failed, please verify your credentials")
		result_flag = False
		sshObj.close()
	except paramiko.SSHException as sshException:
		print(("Could not establish SSH connection: %s" % sshException))
		result_flag = False
		sshObj.close()
	#except socket.timeout as e:
	#	print "Connection timed out"
	#	result_flag = False
	except Exception as e:
		print("Exception in connecting to the server")
		print(("Error:",e))
		result_flag = False
		sshObj.close()
	else:
		result_flag = True
	#sshc.close()
	return sshObj, result_flag
