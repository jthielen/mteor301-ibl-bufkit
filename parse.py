#!/usr/bin/python3
#
# 301 IBL Group 7 Project
#
# Bufkit parsing script
#
# From a given model time, use the downloaded bufkit files to assemble a CSV
# of the variables of interest in the runs leading up to the 3-hr period of interest


import os, subprocess, shlex, sys

# Get input time
timestring = str(input("Enter Time of Interest (YYYY-MM-DD-HH): "))
(year, month, day, hour) = timestring.split("-")

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
missing_times = []
for time in timestamps:
	# Check for files of the type {model}_{time}_kdsm.buf
	if not os.path.isfile("save/gfs3_{}_kdsm.buf".format(time)) or not os.path.isfile("save/nam_{}_kdsm.buf".format(time)):
		missing_times.append(time)
		print("File not found! Please use retrieve.py to retrieve the data first, otherwise unexpected results may occur!")

# Now, actually do the neat stuff
# That is, loop through the timestamps and parse the data
all_data = []
t = 0
for time in timestamps:
	t = t + 6
	data = {"time of interest": timestring, "model lead time": t, "model time": time}

	############
	# NAM Data #
	############

	if time in missing_times:
		file = []
	else:
		with open("save/nam_{}_kdsm.buf".format(time), "r") as ins:
		    file = []
		    for line in ins:
		        file.append(line)

	target_line_1 = "STID = KDSM STNM = 725460 TIME = {}{}{}/{}00".format(year[2:4],month,day,hour)
	if hour == "00":
		if day == "01":
			this_month = int(month)-1
			if this_month in (0,1,3,5,7,8,10,12):
				this_day = "31"
			elif this_month == 2:
				if this_year in (2008,2012,2016):
					this_day = "29"
				else:
					this_day = "28"
			else:
				this_day = "30"
			target_line_2 = "725460 {}{}{}/2200".format(year[2:4],str(this_month).zfill(2),this_day)
		else:
			target_line_2 = "725460 {}{}{}/2200".format(year[2:4],month,str(int(day)-1).zfill(2))
	else:
		target_line_2 = "725460 {}{}{}/{}00".format(year[2:4],month,day,str(int(hour)-2).zfill(2))

	counter = 0
	counter_target = 1000000 # a very large number
	task = "maxtemp start"
	has_completed = False
	max_temp = -9999.99
	qpf = -9999.99

	for line in file:
		if not has_completed:
			if line[0:44] == target_line_1:
				# Start sounding line offset
				counter = 1
				counter_target = 11
			elif 0 < counter < counter_target:
				# Counting!
				counter = counter + 1
			elif counter == counter_target and task == "maxtemp start":
				# First meaningful sounding line (sfc) has been reached. Start the rest of the process.
				variables = line.split(" ")
				sfc_pres = float(variables[0])
				max_temp = float(variables[1])
				counter = 1
				counter_target = 2
				task = "maxtemp search"
			elif counter == counter_target and task == "maxtemp search":
				counter = 1
				variables = line.split(" ")
				if float(variables[0]) < sfc_pres - 500:
					# We now have max_temp from the lowest 500 mb, so move on to qpf
					counter = 0
					task = "qpf start"
				elif float(variables[1]) > max_temp:
					# If this temp is higher than the previous max temp, make it the new max temp
					max_temp = float(variables[1])
			elif line[0:18] == target_line_2:
				# Found the lower extra data
				counter = 1
				counter_target = 1
			elif counter == counter_target and task == "qpf start":
				# Found first line of interest
				variables = line.split(" ")
				qpf = float(variables[0])
				hours = 1
				counter = 1
				counter_target = 6
				task = "qpf add"
			elif counter == counter_target and task == "qpf add":
				# found next line of hourly qpf
				variables = line.split(" ")
				qpf = qpf + float(variables[0])
				hours = hours + 1
				counter = 1
				if hours > 3:
					sys.exit("Something went wrong here...")
				elif hours == 3:
					# All done here!
					has_completed = True

	if has_completed:
		data["nam_qpf"] = round(qpf, 2)
		data["nam_maxtemp"] = round(max_temp + 273.15, 2)
	else:
		data["nam_qpf"] = "Missing"
		data["nam_maxtemp"] = "Missing"

	

	############
	# GFS Data #
	############

	with open("save/gfs3_{}_kdsm.buf".format(time), "r") as ins:
	    file = []
	    for line in ins:
	        file.append(line)

	target_line_1 = "STID = STNM = 725460 TIME = {}{}{}/{}00".format(year[2:4],month,day,hour)
	target_line_2 = "725460 {}{}{}/{}00".format(year[2:4],month,day,hour)

	counter = 0
	counter_target = 1000000 # a very large number
	task = "maxtemp start"
	has_completed = False
	max_temp = -9999.99
	qpf = -9999.99

	for line in file:
		if not has_completed:
			if line[0:39] == target_line_1:
				# Start sounding line offset
				counter = 1
				counter_target = 11
			elif 0 < counter < counter_target:
				# Counting!
				counter = counter + 1
			elif counter == counter_target and task == "maxtemp start":
				# First meaningful sounding line (sfc) has been reached. Start the rest of the process.
				variables = line.split(" ")
				sfc_pres = float(variables[0])
				max_temp = float(variables[1])
				counter = 1
				counter_target = 2
				task = "maxtemp search"
			elif counter == counter_target and task == "maxtemp search":
				counter = 1
				variables = line.split(" ")
				if float(variables[0]) < sfc_pres - 500:
					# We now have max_temp from the lowest 500 mb, so move on to qpf
					counter = 0
					task = "qpf start"
				elif float(variables[1]) > max_temp:
					# If this temp is higher than the previous max temp, make it the new max temp
					max_temp = float(variables[1])
			elif line[0:18] == target_line_2:
				# Found the lower extra data! Get the 3-hr QPF
				variables = line.split(" ")
				qpf = float(variables[7])
				has_completed = True

	if has_completed:
		data["gfs_qpf"] = round(qpf, 2)
		data["gfs_maxtemp"] = round(max_temp + 273.15, 2)
	else:
		data["gfs_qpf"] = "Missing"
		data["gfs_maxtemp"] = "Missing"

	# Save to all data
	all_data.append(data)

# Output CSV
print("")
print("Time of Interest, Model Timestamp, Model Leadtime (hr), NAM QPF (mm), NAM MAX T (K), GFS QPF (mm), GFS MAX T (K)")
for data in all_data:
	print("{}\t{}\t{}\t{}\t{}\t{}\t{}".format(data['time of interest'], data['model time'], data['model lead time'], data['nam_qpf'], data['nam_maxtemp'], data['gfs_qpf'], data['gfs_maxtemp']))
