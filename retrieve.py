#!/usr/bin/python3
#
# 301 IBL Group 7 Project
#
# Bufkit retrieval and extraction script
#
# From a given model time, download the NAM and GFS bufkit archives every
# 6h leading up to the given time period (if it is not already in the
# storage archive), extract the archive, and save the KDSM file in the save
# directory for later processing.

import os, subprocess, shlex, sys, urllib.request, shutil

# Get input time
timestring = str(input("Enter Time of Interest (YYYY-MM-DD-HH): "))
(year, month, day, hour) = timestring.split("-")

# Check for valid time
if int(hour) % 6 != 0:
	sys.exit("Invalid Time Entered!!")

if 2017 < int(year) < 2005:
	sys.exit("Bufkit Data Unavailable!!")
else:
	if (year == "2016" and int(month) > 4) or (year == "2006" and month == "01"):
		sys.exit("Bufkit Data Unavailable!!")

# Loop through time intervals before to assemble timestamps to collect
timestamps = []
for offset in range(6,54,6):
	this_hour = (int(hour)-offset) % 24
	this_day = int(day) + (int(hour)-offset) // 24
	this_month = int(month)
	this_year = int(year)
	# Handle month overflow
	if this_day < 1:
		this_month = this_month - 1
		if this_month in (0,1,3,5,7,8,10,12):
			this_day = this_day + 31
		elif this_month == 2:
			if this_year in (2008,2012,2016):
				this_day = this_day + 29
			else:
				this_day = this_day + 28
		else:
			this_day = this_day + 30
	# Handle year overflow
	if this_month == 0:
		this_month = 12
		this_year = this_year - 1

	timestamp = str(this_year).zfill(2) + str(this_month).zfill(2) + str(this_day).zfill(2) + str(this_hour).zfill(2)
	timestamps.append(timestamp)

# Present Update
print("Time Stamps to Obtain:")
print(timestamps)

# Now, loop through all time stamps and check if present in the save
# directory, and if not, to download it
for time in timestamps:
	# Check for files of the type {model}_{time}_kdsm.buf
	if not os.path.isfile("save/gfs3_{}_kdsm.buf".format(time)) or not os.path.isfile("save/nam_{}_kdsm.buf".format(time)):
		# Have to load the files now
		# Will do so based on time
		if int(time) < 2009120400:
			# eta and etam, and gfs3 are good, in http://mesonet.agron.iastate.edu/archive/data/2015/03/27/bufkit/{model}_{time}.zip

			# get date for url
			urldate = "{}/{}/{}".format(time[0:4],time[4:6],time[6:8])

			# get, extract, and copy gfs
			print("Downloading GFS for {}...".format(time))
			urllib.request.urlretrieve("http://mesonet.agron.iastate.edu/archive/data/{}/bufkit/gfs3_{}.zip".format(urldate,time), "temp/gfs.zip")
			subprocess.call(shlex.split("unzip gfs.zip"), cwd="temp")
			subprocess.call(shlex.split("cp gfs3_{}_kdsm.buf ../save/.".format(time)), cwd="temp")
			shutil.rmtree("temp")
			subprocess.call(shlex.split("mkdir temp"))
			print("GFS for {} Saved".format(time))

			if int(time[8:10]) % 12 == 0:
				# get, extract, and copy  eta
				print("Downloading ETA/NAM for {}...".format(time))
				urllib.request.urlretrieve("http://mesonet.agron.iastate.edu/archive/data/{}/bufkit/eta_{}.zip".format(urldate,time), "temp/eta.zip")
				subprocess.call(shlex.split("unzip eta.zip"), cwd="temp")
				subprocess.call(shlex.split("cp eta_{}_kdsm.buf ../save/nam_{}_kdsm.buf".format(time,time)), cwd="temp")
				shutil.rmtree("temp")
				subprocess.call(shlex.split("mkdir temp"))
				print("ETA/NAM for {} Saved".format(time))
			else:
				# get, extract, and copy etam
				print("Downloading ETAM/NAM for {}...".format(time))
				urllib.request.urlretrieve("http://mesonet.agron.iastate.edu/archive/data/{}/bufkit/etam_{}.zip".format(urldate,time), "temp/etam.zip")
				subprocess.call(shlex.split("unzip etam.zip"), cwd="temp")
				subprocess.call(shlex.split("cp etam_{}_kdsm.buf ../save/nam_{}_kdsm.buf".format(time,time)), cwd="temp")
				shutil.rmtree("temp")
				subprocess.call(shlex.split("mkdir temp"))
				print("ETAM/NAM for {} Saved".format(time))

		if 2009120400 <= int(time) < 2010012900:
			print("NAM Data Unavailable!!")
		if 2010012900 <= int(time) <= 2015032918:
			# nam and gfs3 are good, in http://mesonet.agron.iastate.edu/archive/data/2015/03/27/bufkit/{model}_{time}.zip

			# get date for url
			urldate = "{}/{}/{}".format(time[0:4],time[4:6],time[6:8])

			# get, extract, and copy gfs
			print("Downloading GFS for {}...".format(time))
			urllib.request.urlretrieve("http://mesonet.agron.iastate.edu/archive/data/{}/bufkit/gfs3_{}.zip".format(urldate,time), "temp/gfs.zip")
			subprocess.call(shlex.split("unzip gfs.zip"), cwd="temp")
			subprocess.call(shlex.split("cp gfs3_{}_kdsm.buf ../save/.".format(time)), cwd="temp")
			shutil.rmtree("temp")
			subprocess.call(shlex.split("mkdir temp"))
			print("GFS for {} Saved".format(time))

			# get, extract, and copy nam
			print("Downloading NAM for {}...".format(time))
			urllib.request.urlretrieve("http://mesonet.agron.iastate.edu/archive/data/{}/bufkit/nam_{}.zip".format(urldate,time), "temp/nam.zip")
			subprocess.call(shlex.split("unzip nam.zip"), cwd="temp")
			subprocess.call(shlex.split("cp nam_{}_kdsm.buf ../save/.".format(time)), cwd="temp")
			shutil.rmtree("temp")
			subprocess.call(shlex.split("mkdir temp"))
			print("NAM for {} Saved".format(time))

		if 2015032918 < int(time):
			# nam and gfs are good, in http://mtarchive.geol.iastate.edu/2015/04/01/bufkit/00/gfs/gfs3_kdsm.buf

			# get date for url
			urldate = "{}/{}/{}".format(time[0:4],time[4:6],time[6:8])
			hour = time[8:10]

			# get gfs
			print("Downloading GFS for {}...".format(time))
			urllib.request.urlretrieve("http://mtarchive.geol.iastate.edu/{}/bufkit/{}/gfs/gfs3_kdsm.buf".format(urldate,hour), "save/gfs3_{}_kdsm.buf".format(time))
			print("GFS for {} Saved".format(time))

			# get nam
			print("Downloading NAM for {}...".format(time))
			if int(time[8:10]) % 12 == 0:
				label = "nam"
			else:
				label = "namm"
			urllib.request.urlretrieve("http://mtarchive.geol.iastate.edu/{}/bufkit/{}/nam/{}_kdsm.buf".format(urldate,hour,label), "save/nam_{}_kdsm.buf".format(time))
			print("NAM for {} Saved".format(time))

print("##########################################")
print("## Download for {} Complete! ##".format(timestring))
print("##########################################")