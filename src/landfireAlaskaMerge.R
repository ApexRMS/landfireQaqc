# Copyright � 2007-2023 Apex Resource Management Solution Ltd. (ApexRMS). All rights reserved.
# A script to update configure a master LANDFIRE library.
#
# This script can be run once against a project in the master library.  Suggested workflow:
#
# 1.  Make a backup of the BpSModels project in the master (e.g. BpSModels - Copy)
# 2.  Set the working directory path, library name, and the final project/scenario
#     names below.
# 2.  Run this script.  If there are any problems then start again at step 1
#
# Required Packages: dplyr, rsyncrosim

# Load R libraries
library(dplyr)
library(rsyncrosim)

# TODO: User sets local vars 
WorkingDir <- "C:/projects/A281"
FileName <- "LANDFIRE Alaska 2023-03-30.ssim"
finalProjectName <- "BpSModels"
finalScenarioName <- "AllModels"

# Set up environment
mySession <- session()
setwd(WorkingDir)

# Load SyncroSim library + final project and scenario
myLibrary <- ssimLibrary(FileName)
finalProject <- project(myLibrary, project=finalProjectName)
finalScenario <- scenario(finalProject, scenario=finalScenarioName)

# Find all projects based on BpS codes
projectNames <- project(myLibrary) %>%
  filter(Name != finalProjectName) %>%
  pull(Name)

# Update final project primary stratum
finalProjectPrimaryStratum <- datasheet(finalProject, name="stsim_Stratum")
projectNamesDF <- as.data.frame(projectNames)
colnames(projectNamesDF) <- c("Name")
finalProjectPrimaryStratum <- finalProjectPrimaryStratum %>% bind_rows(projectNamesDF)
saveDatasheet(finalProject, finalProjectPrimaryStratum, "stsim_Stratum")

# Update final scenario datasheets, including deterministic transitions, 
# probabilistic transitions, initial conditions non spatial, and initial
# conditions non spatial distribution
finalDetTransitions <- datasheet(finalScenario, name="stsim_DeterministicTransition")
finalProbTransitions <- datasheet(finalScenario, name="stsim_Transition")
finalInitConds <- datasheet(finalScenario, name="stsim_InitialConditionsNonSpatial")
finalInitCondsDist <- datasheet(finalScenario, name="stsim_InitialConditionsNonSpatialDistribution")

for (p in projectNames){
  
  # Load model project and scenario
  modelProj <- project(myLibrary, project=p)
  scnName <- scenario(modelProj) %>%
    filter(IsResult == "No",
           grepl("FINAL", Name)) %>%
    pull(Name)
  modelScn <- scenario(modelProj, scenario=scnName)
  
  # Retrieve datasheets to update
  modelDetTransitions <- datasheet(modelScn, name="stsim_DeterministicTransition")
  modelProbTransitions <- datasheet(modelScn, name="stsim_Transition")
  modelInitConds <- datasheet(modelScn, name="stsim_InitialConditionsNonSpatial")
  modelInitCondsDist <- datasheet(modelScn, name="stsim_InitialConditionsNonSpatialDistribution")

  # Append individual model datasheets to final datasheets
  finalDetTransitions <- finalDetTransitions %>% bind_rows(modelDetTransitions)
  finalProbTransitions <- finalProbTransitions %>% bind_rows(modelProbTransitions)
  finalInitCondsDist <- finalInitCondsDist %>% bind_rows(modelInitCondsDist)
  
  # Add area and cells to initial conditions non spatial
  finalInitConds$TotalAmount <- finalInitConds$TotalAmount + modelInitConds$TotalAmount
  finalInitConds$NumCells <- finalInitConds$NumCells + modelInitConds$NumCells
}

# Save all updated final datasheet
saveDatasheet(finalScenario, finalDetTransitions, "stsim_DeterministicTransition")
saveDatasheet(finalScenario, finalProbTransitions, "stsim_Transition")
saveDatasheet(finalScenario, finalInitConds, "stsim_InitialConditionsNonSpatial")
saveDatasheet(finalScenario, finalInitCondsDist, "stsim_InitialConditionsNonSpatialDistribution")

# Delete all projects / scenarios that are not in the final project
for (p in projectNames){
  delete(myLibrary, project=p, force=TRUE)
}