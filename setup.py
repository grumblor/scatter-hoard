from setuptools import setup

setup(
	name="scatter-hoard",
	version='0.02',
	description='file archiver, organizer',
	author='Greg Dutra',
	author_email='greg.dutra@gmail.com',
	py_modules=[
		'scatter-hoard.scatter-hoard',
		'scatter-hoard.dbhoard',
		'scatter-hoard.uihoard',
		'scatter-hoard.fileutilhoard',
		'scatter-hoard.shconfig',
		'scatter-hoard.backuphoard',
		'scatter-hoard.sshcomhoard'
		],
	install_requires=[
		'paramiko',
		'Tkinter'
	]
)
