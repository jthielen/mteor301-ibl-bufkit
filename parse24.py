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
for offset in range(1,3):
	this_day = int(day) - offset
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

	timestamp = str(this_year).zfill(2) + str(this_month).zfill(2) + str(this_day).zfill(2) + hour
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
t_run = 0
for time in timestamps:
	t_run = t_run + 24
	data = {"time of interest": timestring, "model lead time": t_run, "model time": time}

	############
	# NAM Data #
	############

	total_qpf = 0
	total_snow = 0

	# Loop over model output times
	for t in range(0,24):

		if time in missing_times:
			file = []
		else:
			with open("save/nam_{}_kdsm.buf".format(time), "r") as ins:
			    file = []
			    for line in ins:
			        file.append(line)

        # Get target time
		this_hour = (int(hour)-t) % 24
		this_day = int(day) + (int(hour)-t) // 24
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

		# Convert back to strings
		this_year = str(this_year)
		this_month = str(this_month).zfill(2)
		this_day = str(this_day).zfill(2)
		this_hour = str(this_hour).zfill(2)

		target_line_1 = "STID = KDSM STNM = 725460 TIME = {}{}{}/{}00".format(this_year[2:4],this_month,this_day,this_hour)
		target_line_2 = "725460 {}{}{}/{}00".format(this_year[2:4],this_month,this_day,this_hour)

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
					max_temp = float(variables[1]) + 273.15
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
					elif float(variables[1]) +273.15 > max_temp:
						# If this temp is higher than the previous max temp, make it the new max temp
						max_temp = float(variables[1]) + 273.15
				elif line[0:18] == target_line_2:
					# Found the lower extra data
					counter = 1
					counter_target = 1
				elif counter == counter_target and task == "qpf start":
					# Found first line of interest
					variables = line.split(" ")
					qpf = float(variables[0])
					has_completed = True

		if has_completed:
			# Found everything
			total_qpf = total_qpf + qpf
			# Calculate Kuchera Ratio
			if max_temp > 271.16:
				total_snow = total_snow + qpf * (12.0 + 2.0 * (271.16-max_temp))
			else:
				total_snow = total_snow + qpf * (12.0 + (271.16-max_temp))
		else:
			# something's missing. For now, just panic
			sys.exit("Panic! Something's Missing!")

	# End time loop
	data["nam_qpf"] = round(total_qpf, 2)
	data["nam_snow"] = round(total_snow, 2)

	############
	# GFS Data #
	############

	total_qpf = 0
	total_snow = 0

	# Loop over model output times
	for t in range(0,24,3):

		if time in missing_times:
			file = []
		else:
			with open("save/gfs3_{}_kdsm.buf".format(time), "r") as ins:
			    file = []
			    for line in ins:
			        file.append(line)

        # Get target time
		this_hour = (int(hour)-t) % 24
		this_day = int(day) + (int(hour)-t) // 24
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

		# Convert back to strings
		this_year = str(this_year)
		this_month = str(this_month).zfill(2)
		this_day = str(this_day).zfill(2)
		this_hour = str(this_hour).zfill(2)

		target_line_1 = "STID = STNM = 725460 TIME = {}{}{}/{}00".format(this_year[2:4],this_month,this_day,this_hour)
		target_line_2 = "725460 {}{}{}/{}00".format(this_year[2:4],this_month,this_day,this_hour)

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
					max_temp = float(variables[1]) + 273.15
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
					elif float(variables[1]) + 273.15 > max_temp:
						# If this temp is higher than the previous max temp, make it the new max temp
						max_temp = float(variables[1]) + 273.15
				elif line[0:18] == target_line_2:
					# Found the lower extra data! Get the 3-hr QPF
					variables = line.split(" ")
					qpf = float(variables[7])
					has_completed = True

		if has_completed:
			# Found everything
			total_qpf = total_qpf + qpf
			# Calculate Kuchera Ratio
			if max_temp > 271.16:
				total_snow = total_snow + qpf * (12.0 + 2.0 * (271.16-max_temp))
			else:
				total_snow = total_snow + qpf * (12.0 + (271.16-max_temp))
		else:
			# something's missing. For now, just panic
			sys.exit("Panic! Something's Missing!")

	# End time loop
	data["gfs_qpf"] = round(total_qpf, 2)
	data["gfs_snow"] = round(total_snow, 2)

	# Save to all data
	all_data.append(data)

# Output CSV
print("")
print("Time of Interest, Model Timestamp, Model Leadtime (hr), NAM QPF (mm), NAM Snow (mm), GFS QPF (mm), GFS Snow (mm)")
for data in all_data:
	print("{}\t{}\t{}\t{}\t{}\t{}\t{}".format(data['time of interest'], data['model time'], data['model lead time'], data['nam_qpf'], data['nam_snow'], data['gfs_qpf'], data['gfs_snow']))
