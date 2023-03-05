# Copyright © 2007-2018 Apex Resource Management Solution Ltd. (ApexRMS). All rights reserved.
# A script to update configure a master LANDFIRE library.
#
# This script can be run once against a project in the master library.  Suggested workflow:
#
# 1.  Make a copy of the BpSModels project in the master (e.g. BpSModels - Copy)
# 2.  Run this script.  If there are any problems then start again at step 1
#
# Required Packages: dplyr, rsyncrosim

library(dplyr)
library(rsyncrosim)

Ses = session()
ScriptsDir = "C:/svnprojects/ProjectScripts/A172"
WorkingDir = "C:/Users/Alex/Desktop/TNC/Final"
FileName = "master.ssim"
ProjectName = "BpSModels - Copy"

setwd(WorkingDir)

source(paste(ScriptsDir, "common.R", sep = "/"))
source(paste(ScriptsDir, "master.R", sep = "/"))
source(paste(ScriptsDir, "import.R", sep = "/"))
source(paste(ScriptsDir, "postproc.R", sep = "/"))

# Update the project in the master library
logfile <- file("prep.log")
sink(logfile, append=TRUE)
sink(logfile, append=TRUE, type="message")

cat("IMPORTING REVIEWED MODEL LIBRARIES\n")
cat("==================================\n")
SSim_ImportProjectFolder(FileName, ProjectName, Ses)

cat("\n\nUPDATING DEFINITIONS IN MASTER\n")
cat("==================================\n")
SSim_UpdateMasterDefinitions(FileName, ProjectName)

cat("\n\nDELETING AND RENAMING STRATA\n")
cat("================================\n")
SSim_DeleteRenameFromConfig(FileName, projectName)

sink() 
sink(type="message")

# Load SyncroSim objects
Lib = ssimLibrary(FileName)
Prj = project(Lib, project=ProjectName)
Sid = SSim_GetSingleChildScenarioId(FileName, Prj@projectId, ProjectName)
Scn = scenario(Prj, Sid)

# Initial conditions
stratumSheet = datasheet(Prj, name = "STSim_Stratum")
totalStrata = nrow(stratumSheet)
ICsheet = "STSim_InitialConditionsNonSpatial"
ICdata = datasheet(Scn, ICsheet)
ICdata$TotalAmount = 1000 * totalStrata
ICdata$NumCells = 1000 * totalStrata
ICdata$CalcFromDist = NA
saveDatasheet(Scn, ICdata, ICsheet)
datasheet(Scn, ICsheet)

# Initial Conditions Distribution (may differ by model)
SSim_UpdateICFromDT(Prj, Scn)

# Time since transition settings 
TSTGsheet = "STSim_TimeSinceTransitionGroup"
TSTGdata = datasheet(Scn, TSTGsheet)
TSTGdata <- TSTGdata[0,]                           # clear existing rows in the TSTGsheet 
testrow<-c("Alternative Succession","All Fire")    # create a row with the required values
TSTGdata <-addRow(TSTGdata, testrow)
saveDatasheet(Scn, TSTGdata, TSTGsheet)
datasheet(Scn, TSTGsheet)

# Randomize TST to the maximum minTST used in the model (differs by model)
# For each BpS find the max TSTMin value used and put them in the table maxTSTminTable

PTRANsheet = "STSim_Transition"
PTRANdata = datasheet(Scn, PTRANsheet)

maxTSTminTable<-PTRANdata %>%
  group_by(StratumIDSource) %>%
  summarise(maxTSTmin= max(TSTMin))

maxTSTminTable<-maxTSTminTable[maxTSTminTable$maxTSTmin>0,]   #keep values > 0 in maxTSTminTable
names(maxTSTminTable)<-c("StratumID","MaxInitialTST")         #rename the fields in maxTSTminTable to match TSTRsheet fields
TSTRsheet = "STSim_TimeSinceTransitionRandomize"              #Open the TST Randomize sheet and set it equal to the maxTSTminTable, save
saveDatasheet(Scn,as.data.frame(maxTSTminTable),TSTRsheet)    #This gives warning on a larger database but appears to work as expected
datasheet(Scn, TSTRsheet)

