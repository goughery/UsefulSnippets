import re, glob, traceback,os

apslist = ["ArchiveProd", "BIProd", "DQProd", "ITMetricsProd", "Prod", "ReportingDev", "RiskModelDev", "StatsProd"]
#apslist = ["Prod"]
for APSDatabase in apslist:

	#APSDatabase = "StatsProd"
	RawPath = "Raw\\Temp Table SPROCs\\"+APSDatabase+"\\SPs"
	ConvertedPath = "Converted\\Temp Table SPROCs\\"+APSDatabase+"\\SPs"
	#ManualPath = "Manual Remediation\\CRTAS SPROCs\\Prod\\SPs"
	LogFileName = "LogFiles\\"+APSDatabase+"Log.csv"

	#configfilename = "Configfiles\\crtasconfigProd.csv"

	files = glob.glob(RawPath + "\\*.dsql")
	#files = glob.glob(RawPath + "\\dbo_rpt_BulkUpload_ActivityOverageCloseoutCRTAS_DML.dsql")

	#todo convert = replicate) to convert = round_robin)
	#thestring = "SourceSQLSproc, ScriptType, TargetSchemaName, TargetTableName, Status, SourceContainerName, ArchiveContainerName, SourceSchemaName, SQLDebug, SourceUserName, SourcePassword, DestinationServerName, DestinationPassword, SourceServerName, SourceDatabaseName, DestinationUserName, DestinationDatabaseName, TargetExtSchema, StorageAccountName, ResourceGroupName"
	CSVString = "APS Database,APS Schema,ADW Schema,ADW SprocName,FileName,Converted,Temp Table Conversion Count,Exception"
	for fileName in files:
		convertCount = 0
		try:
			print(fileName)


			
			file = open(fileName, "rb")
			text = file.read()
			text = text.decode("utf-16")
			text = text.encode('ascii', 'ignore')
			file.close()

			partWhole = text

			# if (b"@stagingtable" in text.lower()):
			# 	raise Exception("@StagingTable")
			# if (b"create remote table" not in text):
			# 	raise Exception("create remote table not in text")
			
			#get the sproc name
			sprocFullName = re.search(b"[\[](.*)[\]](\s+)[@|AS]", partWhole).group(1).replace(b"]",b"").replace(b"[",b"").strip().split(b" ")[0]
			sprocSchemaName = sprocFullName.split(b".")[0]
			sprocName = sprocFullName.split(b".")[1]
			print("created sproc name")
			#split the sproc into parts




			
			#throw exception if @stagingdatabase in file
			threepart = ""
			# if re.search(b"[@](.*)[Database](\s+){0,}[+](\s+){0,}['.]", partWhole, flags=re.IGNORECASE):
			# 	#raise Exception("File contains 3 part name")
			if b"Database + " in partWhole or b"DatabaseName + " in partWhole or b"Database+" in partWhole or b"DatabaseName+" in partWhole:
				threepart = "File contains 3 part name. No exception raised."
			
			#if using drop object if exists
			dropSprocList = []
			if re.search(b"util_dropobjectifexists", partWhole, flags = re.IGNORECASE):
				sprocParts = re.split(b"util_dropobjectifexists", partWhole, flags=re.IGNORECASE)
				dropSprocPrefix = re.search(b"[\r\n]{0,}(.*)util_dropobjectifexists",partWhole, flags=re.IGNORECASE).group(1).strip()
				print("finished creating drop sproc prefix")
				

				#iterate through the parts
				print("Beginning iteration")
				for part in sprocParts:
					if re.search(b"distribution(\s+){0,}=(\s+){0,}replicate", part, flags=re.IGNORECASE) and re.search(b"create(\s+)table(\s+)#", part, flags=re.IGNORECASE):
						convertCount += 1
						print("starting replacements")
						
						#get the relevant names from the part
						schemaName = part.split(b",")[0].strip().replace(b"'",b"")
						print(schemaName)
						tableName = re.search(schemaName + b"(.*)[\r\n]{0,}", part, flags=re.IGNORECASE).group(1).replace(b"'",b"").replace(b",",b"").replace(b";",b"").strip()
						print(tableName)
						newTableName = tableName.replace(b"#",b"") + b"_" + sprocName
						newSchemaName = b"work"
						print(newSchemaName + b"','" + newTableName)

						
						#check to see if drop table exists outside of the first call
						#partt = re.split(dropSprocPrefix + b"util_DropObjectIfExists(\s+){0,}'" + schemaName + b"'(\s+){0,},(\s+){0,}'" + tableName, partWhole, flags = re.IGNORECASE)

						if not re.search(dropSprocPrefix + b"util_DropObjectIfExists(\s+){0,}'" + schemaName + b"'(\s+){0,},(\s+){0,}'" + tableName + b"'", partWhole, flags=re.IGNORECASE):
						# if (dropSprocPrefix + b"util_DropObjectIfExists '" + schemaName + b"','" + tableName) not in partt[1]:
							dropSprocList.append(b"\n" + dropSprocPrefix + b"util_DropObjectIfExists '" + newSchemaName + b"','" + newTableName + b"';")
							print(b"\n" + dropSprocPrefix + b"util_DropObjectIfExists '" + newSchemaName + b"','" + newTableName + b"';")
							print("Appended to drop list")

						#replace the drop portion
						partWhole = re.sub(b"util_DropObjectIfExists(\s+){0,}" + b"'" + schemaName + b"'(\s+){0,},(\s+){0,}'" + tableName + b"'",
						b"util_DropObjectIfExists '" + newSchemaName + b"','" + newTableName + b"'", partWhole, flags=re.IGNORECASE)

						print("Replaced drop objects")
						#replace the create table portion
						partWhole = re.sub(b"create(\s+)table(\s+)" + tableName + b"(\s+|\r\n)", b"create table " + newSchemaName + b"." + newTableName + b"\n", partWhole, flags=re.IGNORECASE)

						print("Replaced create table objects")
						#replace any instances of the temp table name  
						#removed 12-2
						#partWhole = re.sub(b"(\s+)" + tableName + b"[']{0,}[;]{0,1}(\s+|\r\n)", b" " + newSchemaName + b"." + newTableName + b"\n" , partWhole, flags=re.IGNORECASE)
						
						#semicolon optional, tick mandatory, newline
						if re.search(b"(\s+)" + tableName + b"[;]{0,}['][;]{0,1}[\r\n]", partWhole, flags=re.IGNORECASE):
							partWhole = re.sub(b"(\s+)" + tableName + b"[;]{0,}['][;]{0,1}[\r\n]", b" " + newSchemaName + b"." + newTableName + b"'\n", partWhole, flags=re.IGNORECASE)
						
						#semicolon optional, tick mandatory, space
						if re.search(b"(\s+)" + tableName + b"[;]{0,}['][;]{0,1}(\s+)", partWhole, flags=re.IGNORECASE):
							partWhole = re.sub(b"(\s+)" + tableName + b"[;]{0,}['][;]{0,1}(\s+)", b" " + newSchemaName + b"." + newTableName + b"' ", partWhole, flags=re.IGNORECASE)

						#semicolon optional, then newline
						if re.search(b"(\s+)" + tableName + b"[;]{0,1}[\r\n]", partWhole, flags=re.IGNORECASE):
							partWhole = re.sub(b"(\s+)" + tableName + b"[;]{0,1}[\r\n]", b" " + newSchemaName + b"." + newTableName + b"\n", partWhole, flags=re.IGNORECASE)
						
						#semicolon optional, then space
						if re.search(b"(\s+)" + tableName + b"[;]{0,1}(\s+)", partWhole, flags=re.IGNORECASE):
							partWhole = re.sub(b"(\s+)" + tableName + b"[;]{0,1}(\s+)", b" " + newSchemaName + b"." + newTableName + b" ", partWhole, flags=re.IGNORECASE)

						#sometimes at the end of the file, there's no call to drop the temp table. we're putting that in here.
						#exec [archive_dbo].util_DropObjectIfExists 'work','StagingBC01_BANKRUPTCY_CASE_RECORD_procname';
						
						
						
					else:
						print("String pattern not found in  part")

			#############if not using drop object utility####################
			#if(\s+)object_id\((.*)\)(\s+)IS(\s+)NOT(\s+)NULL(\s+)[\r\n]{0,}(begin){0,}(\s+){0,}drop(\s+)table(\s+)#RecoveryRate(\s+)(end){0,}
			if re.search(b"if(\s+)object_id(.*)(\s+)IS(\s+)NOT(\s+)NULL[\r\n]{0,}(\s+){0,}(begin){0,}(\s+)drop(\s+)table", partWhole, flags=re.IGNORECASE):
				sprocParts = re.split(b"if(\s+)object_id(.*)(\s+)IS(\s+)NOT(\s+)NULL[\r\n]{0,}(\s+){0,}(begin){0,}(\s+)drop(\s+)table", partWhole, flags=re.IGNORECASE)
				#dropsprocprefix
				print("Conventional SQL")
				for part in sprocParts:
					if(part):
						if re.search(b"distribution(\s+){0,}=(\s+){0,}replicate", part, flags=re.IGNORECASE) and re.search(b"create(\s+)table(\s+)#", part, flags=re.IGNORECASE):
							print("########################################################################################################")
							convertCount += 1
							tableName = re.search(b"#(.*)[\r\n]{1,}", part, flags=re.IGNORECASE).group(1).replace(b"'",b"").replace(b",",b"").replace(b";",b"").replace(b"end", b"").strip()
							tableName = b"#" + tableName
							print(tableName)
							
							newTableName = tableName.replace(b"#",b"") + b"_" + sprocName
							newSchemaName = b"work"
							#create drop table portion, added to list
							dropCode = b"\nif object_id('" + newSchemaName + b"." + newTableName + b"') is not null\ndrop table " + newSchemaName + b"." + newTableName + b"\n"
							dropSprocList.append(dropCode)


							#replace drop table portion
							partWhole = re.sub(b"if(\s+)object_id(.*)" + tableName + b"(.*)[\r\n]{0,}(\s+)drop(\s+)table(\s+)" + tableName, dropCode, partWhole, flags=re.IGNORECASE)
							#replace create table portion
							partWhole = re.sub(b"create(\s+)table(\s+)" + tableName + b"(\s+|\r\n)", b"create table " + newSchemaName + b"." + newTableName + b"\n", partWhole, flags = re.IGNORECASE)
							#replace any other occurrences
							#first if dynamic sql, note the ', gotta add it back in

							#semicolon optional, tick mandatory, newline
							if re.search(b"(\s+)" + tableName + b"[;]{0,}['][;]{0,1}[\r\n]", partWhole, flags=re.IGNORECASE):
								partWhole = re.sub(b"(\s+)" + tableName + b"[;]{0,}['][;]{0,1}[\r\n]", b" " + newSchemaName + b"." + newTableName + b"'\n", partWhole, flags=re.IGNORECASE)
							#semicolon optional, tick mandatory, space
							if re.search(b"(\s+)" + tableName + b"[;]{0,}['][;]{0,1}(\s+)", partWhole, flags=re.IGNORECASE):
								partWhole = re.sub(b"(\s+)" + tableName + b"[;]{0,}['][;]{0,1}(\s+)", b" " + newSchemaName + b"." + newTableName + b"' ", partWhole, flags=re.IGNORECASE)	

							#semicolon optional, then newline
							if re.search(b"(\s+)" + tableName + b"[;]{0,1}[\r\n]", partWhole, flags=re.IGNORECASE):
								partWhole = re.sub(b"(\s+)" + tableName + b"[;]{0,1}[\r\n]", b" " + newSchemaName + b"." + newTableName + b"\n", partWhole, flags=re.IGNORECASE)
							#semicolon optional, then space
							if re.search(b"(\s+)" + tableName + b"[;]{0,1}(\s+)", partWhole, flags=re.IGNORECASE):
								partWhole = re.sub(b"(\s+)" + tableName + b"[;]{0,1}(\s+)", b" " + newSchemaName + b"." + newTableName + b" ", partWhole, flags=re.IGNORECASE)

			
			partWhole = re.sub(b"location(\s+){0,}=(\s+){0,}user_db,(\s+){0,}", b"", partWhole, flags=re.IGNORECASE) 
			
			
			partWhole = re.sub(b"exec [\[]{0,1}dba[\]]{0,1}.[\[]{0,1}dbo[\]]{0,1}.util_CreateUpdateStats", b"exec dbo.util_CreateUpdateStats", partWhole, flags=re.IGNORECASE)
			partWhole = re.sub(b"[\[]{0,1}dba[\]]{0,1}.[\[]{0,1}dbo[\]]{0,1}.util_CreateUpdateStats", b"dbo.util_CreateUpdateStats", partWhole, flags=re.IGNORECASE)
			partWhole = re.sub(b"[\[]{0,1}dba[\]]{0,1}.[\[]{0,1}dbo[\]]{0,1}.CopyTables", b"dbo.CopyTables", partWhole, flags=re.IGNORECASE)
			partWhole = re.sub(b"[\[]{0,1}dba[\]]{0,1}.[\[]{0,1}dbo[\]]{0,1}.usp_CopyTable", b"dbo.usp_CopyTable", partWhole, flags=re.IGNORECASE)
			partWhole = re.sub(b"[\[]{0,1}dba[\]]{0,1}.[\[]{0,1}dbo[\]]{0,1}.Numbers", b"dbo.Numbers", partWhole, flags=re.IGNORECASE)
			#replace biqa
			partWhole = re.sub(b"[\[]{0,1}biqa[\]]{0,1}.", b"", partWhole, flags=re.IGNORECASE)
			#3 part database names in variables
			partWhole = re.sub(b"['](\s){0,}[+](\s){0,}[@](.*)Database(.*)(\s){0,}[+](\s){0,}['][.]", b"", partWhole, flags=re.IGNORECASE)

			if convertCount > 0:
				partWhole = re.sub(b"(\s+|\r\n)AS(\s+|\r\n)",b" AS\n--This script has been converted to replace temp tables with physical ones when using a replicate distribution.\n",partWhole,1,flags=re.IGNORECASE)
			#append drop sproc calls at the end
			#removed because we don't need to drop sprocs again. 
			# for call in dropSprocList:
			# 	partWhole += call
		

			if convertCount == 0:
				CSVString += "\n" + APSDatabase + ",Unknown," + sprocSchemaName.decode() + "," + sprocName.decode() + "," + fileName.split("\\")[-1] + "," + "N," + str(convertCount) + "," + threepart
				#raise Exception("File does not contain any issues, or is manual remediation")
			else:
				CSVString += "\n" + APSDatabase + ",Unknown," + sprocSchemaName.decode() + "," + sprocName.decode() + "," + fileName.split("\\")[-1] + "," + "Y," + str(convertCount) + "," + threepart
			####file
			fileName = fileName.split("\\")[-1]
			os.makedirs(os.path.dirname(ConvertedPath + "\\" + fileName), exist_ok=True)
			file = open(ConvertedPath + "\\" + fileName, "wb")
			file.write(partWhole)
			#print(text)
			file.close()

		except Exception as e:
			CSVString += "\n" + APSDatabase + ",Unknown," + sprocSchemaName.decode() + "," + sprocName.decode() + "," + fileName.split("\\")[-1] +"," + "N," + str(0) + "," + str(e)
			fileName = fileName.split("\\")[-1]

			fileName = fileName.split("\\")[-1]
			os.makedirs(os.path.dirname(ConvertedPath + "\\" + fileName), exist_ok=True)
			file = open(ConvertedPath + "\\" + fileName, "wb")
			file.write(partWhole)
			#print(text)
			file.close()

			# os.makedirs(os.path.dirname(ManualPath + "\\" + fileName), exist_ok=True)
			# file = open(ManualPath + "\\" + fileName, "wb")
			# file.write(partWhole)
			# file.close()
			traceback.print_exc()
			print(e)
		finally:
			pass

	file = open(LogFileName, "w+")
	file.write(CSVString)
	file.close()