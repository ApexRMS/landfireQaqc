# Copyright © 2007-2018 Apex Resource Management Solution Ltd. (ApexRMS). All rights reserved.
#
# Example script for running the single child scenario of a master LANDFIRE project.
# Note: You must prepare the master library using the prepareLandFireMaster.R script
# before running this script.
#
# Required Packages: rsyncrosim

library(rsyncrosim)

Ses = session()
ScriptsDir = "C:/svnprojects/ProjectScripts/A172"
WorkingDir = "C:/Users/Alex/Desktop/TNC/Final"
FileName = "master.ssim"
ProjectName = "BpSModels - Copy"

# Set working directory
setwd(WorkingDir)
source(paste(ScriptsDir, "common.R", sep = "/"))

# Load SyncroSim Objects
Lib = ssimLibrary(FileName)
Prj = project(Lib, project=ProjectName)
Sid = SSim_GetSingleChildScenarioId(FileName, Prj@projectId, ProjectName)
Scn = scenario(Prj, Sid)

# Congigure Run control
RCsheet = "STSim_RunControl"
RCdata = datasheet(Scn, RCsheet)
RCdata$MinimumIteration = 1
RCdata$MaximumIteration = 2
RCdata$MinimumTimestep = 0
RCdata$MaximumTimestep = 2
saveDatasheet(Scn, RCdata, RCsheet)
datasheet(Scn, RCsheet)

# Configure Output Options
OOsheet = "STSim_OutputOptions"
OOdata = datasheet(Scn, OOsheet)
OOdata$SummaryOutputSC = T 
OOdata$SummaryOutputSCTimesteps = 1
OOdata$SummaryOutputSCZeroValues = T   
OOdata$SummaryOutputTR = T 
OOdata$SummaryOutputTRTimesteps = 1
OOdata$SummaryOutputTRIntervalMean = T
OOdata$SummaryOutputTRSC = NA 
OOdata$SummaryOutputTRSCTimesteps = NA 
OOdata$SummaryOutputSA = NA 
OOdata$SummaryOutputSATimesteps = NA 
OOdata$SummaryOutputTA = NA 
OOdata$SummaryOutputTATimesteps = NA 
OOdata$SummaryOutputOmitSS = NA 
OOdata$SummaryOutputOmitTS = NA 
saveDatasheet(Scn, OOdata, OOsheet)

# Run the scenario
resultScenario = run(Scn)

# Output Examples
q1 = sqlStatement(
  groupBy=c("Iteration","Timestep", "StratumID", "StateLabelXID"), 
  aggregate=c("Amount"),
  where=list(Timestep=c(0)))

oss1 = datasheet(Prj, scenario=resultScenario@scenarioId, name="STSim_OutputStratumState", sqlStatement = q1)

q2 = sqlStatement(
  groupBy=c("ScenarioID","Iteration","Timestep","StateLabelXID"), 
  aggregate=c("Amount"),
  where=list(Timestep=c(1,2),Iteration=c(1)))

oss2 = datasheet(Prj, scenario=resultScenario@scenarioId, name="STSim_OutputStratumState", sqlStatement = q2)

sc = SSim_GetStateClassID(Prj, "Early1:ALL")

q3 = sqlStatement(
  groupBy=c("ScenarioID","Iteration","Timestep","StateLabelXID"), 
  aggregate=c("Amount"),
  where=list(Timestep=c(1,2),Iteration=c(1), StateClassID=c(sc)))

oss3 = datasheet(Prj, scenario=resultScenario@scenarioId, name="STSim_OutputStratumState", sqlStatement = q3)

st = SSim_GetStratumID(Prj, "0511080")

q4 = sqlStatement(
  groupBy=c("ScenarioID","Iteration","Timestep","StateLabelXID"), 
  aggregate=c("Amount"),
  where=list(Timestep=c(1,2),Iteration=c(1), StratumID=c(st)))

oss4 = datasheet(Prj, scenario=resultScenario@scenarioId, name="STSim_OutputStratumState", sqlStatement = q4)
