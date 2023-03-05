# Copyright © 2007-2018 Apex Resource Management Solution Ltd. (ApexRMS). All rights reserved.
# Routines to import multiple LANDFIRE libraries as new strata for a master LANDFIRE library.
# 
# Required Packages: RSQLite, rsyncrosim

SSim_FixupCSV <- function(fileName, stratumName, tableName){
  
  dat = read.csv(fileName, header = TRUE)
  
  # Update the stratum name(s)
  dat$StratumIDSource = stratumName
  dat$StratumIDDest = stratumName
  
  # Remove slashes from Transition Type
  if (tableName == "STSim_Transition"){
    dat$TransitionTypeID = gsub("/", "", dat$TransitionTypeID, fixed = TRUE)    
  }

  write.csv(dat, fileName, row.names=FALSE, na="")
}

SSim_SelectNewStratumNameSource <- function(projectName, scenarioName){
  
  if (grepl("_", projectName, fixed = TRUE) || grepl("-", projectName, fixed = TRUE)){
    return (projectName)
  }else if (grepl("_", scenarioName, fixed = TRUE) || grepl("-", scenarioName, fixed = TRUE)){
    return (scenarioName)
  }else{
    stop(sprintf("ERROR: Expecting '-' or _' in either project name or scenario name: %s -> %s", projectName, scenarioName))    
  }
}

SSim_GetTargetStratumName <- function(projectName, scenarioName){
  
  src = SSim_SelectNewStratumNameSource(projectName, scenarioName)
  src = gsub("-", "_", src, fixed = TRUE)
  
  return (gsub(" ", "", src, fixed = TRUE))
}

SSim_AppendScenarioData <- function(
  sourceConn, sourceLibraryName, sourceScenarioId, 
  targetLibraryName, targetScenarioId, targetStratumName, 
  tableName, activeSession, tempFolderName){
  
  if (SSim_TableHasData(sourceConn, tableName, sourceScenarioId)){
    
    d = paste0(tempFolderName, "\\",targetStratumName)
    dir.create(d, showWarnings = FALSE, recursive = TRUE)
    f = paste0(d, "\\", tableName, ".csv")
    
    a = sprintf("--export --lib=\"%s\" --sheet=%s --file=\"%s\" --sid=%d", sourceLibraryName, tableName, f, sourceScenarioId)
    out = command(args=a, session=activeSession)
    
    if(!identical(out,"saved")){
      stop(out)
    }
    
    SSim_FixupCSV(f, targetStratumName, tableName)
    
    a = sprintf("--import --lib=\"%s\" --sheet=%s --file=\"%s\" --sid=%d --append", targetLibraryName, tableName, f, targetScenarioId)
    out = command(args=a, session=activeSession)
    
    if(!identical(out,"saved")){
      stop(out)
    }
    
  }else{
    stop(sprintf("ERROR: No '%s' data found for scenario: %d", tableName, sourceScenarioId))
  }
}

SSim_DeleteObsoleteStrata <- function(stratumName, targetCon, targetStrata){
  
  ObsoleteList = SSim_GetOldFormDeleteNames(stratumName)
  
  for (i in 1:length(ObsoleteList)){
    
    ObsoleteId = NA
    ObsoleteName = ObsoleteList[[i]]
    defrow = subset(targetStrata, Name==ObsoleteName)
    
    if (nrow(defrow) == 1){
      ObsoleteId = as.integer(defrow["StratumID"])
    }
    
    if (!is.na(ObsoleteId)){
      cat(sprintf("Deleting obsolete stratum: %s -> (ID = %d)\n", ObsoleteName, ObsoleteId))
      SSim_DeleteStratumCon(targetCon, ObsoleteId)      
    }else{
      cat(sprintf("*** The obsolete stratum was not found: %s\n", ObsoleteName))
    }
  }
}

SSim_ImportProjects <- function(
  targetLibraryName, targetCon, targetProjectId, targetScenarioId, targetStrata, 
  sourceLibraryName, activeSession, tempFolderName){
  
  # Imports all projects in 'sourceLibraryName' into the target project as follows:
  #
  # (1.) Each source project's single scenario becomes a new stratum in the target.
  #
  # (2.) The data from various multi-row data feeds in the single source scenario
  #      is appened to the single scenario in the target project as a new stratum.
  
  ConnSource = SSim_GetConnection(sourceLibraryName)
  on.exit(function(){dbDisconnect(ConnSource)}) 
  SourceProjects = SSim_GetProjects(sourceLibraryName) 

  for (i in 1:nrow(SourceProjects)){
    
    SourceProjectName = SourceProjects[i, "Name"]
    SourceProjectId = SourceProjects[i, "ProjectID"]  
    SourceScenarioId = SSim_GetSingleChildScenarioIdCon(ConnSource, SourceProjectId, SourceProjectName) 
    SourceScenarioName = SSim_GetScenarioDataFrame(ConnSource, SourceProjectId, SourceProjectName)$Name
    SourceStratumName = SSim_SelectNewStratumNameSource(SourceProjectName, SourceScenarioName)
    TargetStratumName = SSim_GetTargetStratumName(SourceProjectName, SourceScenarioName)
    TargetStratumExists = (nrow(subset(targetStrata, Name==TargetStratumName)) == 1)
    
    if (TargetStratumExists){
      stop(sprintf("ERROR: The target stratum already exists in the master: %s", TargetStratumName))
    }
      
    cat(sprintf("\nImporting stratum '%s'\n", SourceStratumName))
    invisible(SSim_CreateDefinition(targetCon, targetProjectId, "STSim_Stratum", "StratumID", TargetStratumName))
    
    SSim_AppendScenarioData(
      ConnSource, sourceLibraryName, SourceScenarioId, 
      targetLibraryName, targetScenarioId, TargetStratumName, 
      "STSim_DeterministicTransition", activeSession, tempFolderName)
    
    SSim_AppendScenarioData(
      ConnSource, sourceLibraryName, SourceScenarioId, 
      targetLibraryName, targetScenarioId, TargetStratumName, 
      "STSim_Transition", activeSession, tempFolderName)
    
    SSim_DeleteObsoleteStrata(TargetStratumName, targetCon, targetStrata)
  }
}

SSim_ImportProjectFolder <- function(targetLibraryName, targetProjectName, activeSession){
  
  # Imports all projects in the \Libs subdirectory of the targetLibraryName folder.
  #
  # Args:
  #   targetLibraryName   : The target library name
  #   targetProjectName   : The target project name
  
  if (is.null(targetLibraryName)){stop("The targetLibraryName is required.")}
  if (is.null(targetLibraryName)){stop("The targetProjectName is required.")}
  
  LibFolderName = file.path(WorkingDir, "Libs")
  TempFolderName = file.path(WorkingDir, "IMPORT")
  
  unlink(TempFolderName, recursive = T)
  TargetCon = SSim_GetConnection(targetLibraryName)
  on.exit(function(){dbDisconnect(TargetCon)})
  
  TargetProjectId = SSim_GetProjectCon(TargetCon, targetProjectName)
  TargetScenarioId = SSim_GetSingleChildScenarioIdCon(TargetCon, TargetProjectId, targetProjectName)
  TargetStrata = SSim_GetDefinitionsCon(TargetCon, "STSim_Stratum", TargetProjectId)
  ImportLibs <- dir(LibFolderName, pattern =".ssim")
  
  for(i in 1:length(ImportLibs)){
    
    LibName = file.path(LibFolderName, ImportLibs[i])
    cat(sprintf("Importing from: %s\n", LibName))
    
    SSim_ImportProjects(
      targetLibraryName, TargetCon, TargetProjectId, TargetScenarioId, TargetStrata, 
      LibName, activeSession, TempFolderName)
  }
}



