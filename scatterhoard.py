#    Scatter Hoard
#    Copyright (C) 2016-2020  Gregory Dutra
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License along
#    with this program; if not, write to the Free Software Foundation, Inc.,
#    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
#! /usr/bin/env python3
#more portable to use environment lookup for python...

# todo
#protect against disk full errors... this should be complete now..
#allow get function to resume when chunks aren't available, allowing for offline ssh storage locations or systems with few usb slots left..
#	put should never allow resume...
#FUSE implementation
#change form paramiko to ssh2-python
#import bencode
#import libtorrent, allow downloading of files from remote storage locations via bittorrent. this are untrusted sources where you know the infohash but perhaps have
#lost the data chunks. 
#	infohash of datachunks files (the 100mb scatterhoard chunks) will be stored and looked up, and then when confirmed, distributed locally for redundancy
#	and after that then piped into the re
#drag and dropping file into cli puts it around single quotes, currently only double quotes are accepted and this needs to either be switched or fixed.
#
#need a cli argument function that will understand get g retrieve but allow filenames called just g get or retrieve. right now those filenames aren't possible
#during batch runs, control c only cancels the current file but the file is still recorded in the DB...
#acorn argument to create acorns of a certain size rather than number of chunks.

import sys
import shutil
import random
import sqlite3
import datetime
import string
import shutil
import platform
import threading
from io import BytesIO
import argparse
import os.path
import signal

import scatterhoard.shconfig as shconfig
import scatterhoard.sshcomhoard as sshcomhoard
import scatterhoard.backuphoard as backuphoard
import scatterhoard.fileutilhoard as fileutilhoard
import scatterhoard.dbhoard as dbhoard
import scatterhoard.uihoard as uihoard
	
SELECTED_DB = shconfig.SELECTED_DB
wack = shconfig.wack

def signalHandle(signum, frame):
	print("\nSIGINT detected! stopping scatterhoard...")
	sys.exit(0)



def initialize():
	print("Starting Scatter-hoard...")
	dbhoard.initDB()
	signal.signal(signal.SIGINT, signalHandle)

def threadRun(targetArg, argArg):
	if (argArg == "Nil"):
		shconfig.threadH = threading.Thread(target=targetArg)
	else:	
		shconfig.threadH = threading.Thread(target=targetArg, args=argArg)
	shconfig.threadH.daemon = True
	shconfig.threadH.start()

def main():
	
	initialize()
	if args.put:
		try:
			tags1 = []
			backingup = backuphoard.backup()
			if args.tags:
				tags1 = args.tags
			backingup.backUpRun(args.put[0], tags=tags1)
		except KeyboardInterrupt:
			print("\nProgram interupted by user, exiting...\n")
			sys.exit()
		
	if not args.put:
		if args.start:
			try:
				mainLoopCLI()
			except KeyboardInterrupt:
				print("\nProgram interupted by user, exiting...\n")
				sys.exit()
		if args.dirbatch:
			if args.tags:
				print(args.tags)
			uihoard.dirBatch([str(args.dirbatch[0])])

	noArgs = True
	for a in args.__dict__:
		if args.__dict__[a] not in [None, False]:
			noArgs = False
	if noArgs == True:
		print("\n")
		uihoard.inBox('Scatter Hoard')
		print("\ntype ? or help for instructions. Type quit or q to quit")
		print("type \'gui\' to get a simple file uploader window.")
		#threadRun(webGui,())
		uihoard.mainLoopCLI()
		

if __name__ == "__main__":
	#command line arguments to dictate how program is run, as gui or cli and with other options
	shconfig.aparser = argparse.ArgumentParser()
	aparser = shconfig.aparser
	aparser.add_argument("-p", "--put", help="Backup file, put file.", nargs=1)
	aparser.add_argument("-g", "--get", help="Retrieve file, get file.", nargs=1)
	aparser.add_argument("-t", "--tags", nargs="+", help="Appends the tag to the file's metadata in the database. Requires filename argument. This helps with organization.")
	#aparser.add_argument("-t", "--tags", action="store_true", help="Flags where put argument or 
	aparser.add_argument("-s", "--start", action="store_true", help="Start with looping cli menu")
	aparser.add_argument("-d", "--dirbatch", help="Backup all files recursively in directory.", nargs=1)
	#aparser.add_argument("-e", "--delrow", help="Delete specified row in fileLoc table.", nargs=1)
	shconfig.args = aparser.parse_args()
	args = shconfig.args
	main()
	








