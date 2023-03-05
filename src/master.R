# Copyright © 2007-2018 Apex Resource Management Solution Ltd. (ApexRMS). All rights reserved.
#
# Routines to update a master LANDFIRE SyncroSim library.  Specifically, this script contains
# routines to update the Transition Types, Groups, and Type-Group linkages; and there is also
# a function to replace the initial conditions distribution values based on the deterministic
# transition records.
#
# Required Packages: rsyncrosim

SSim_ValidateDefinitions <- function(project = NULL, tableName = NULL, definitionNames = NULL) {

  if (is.null(project)){stop("The project is required.")}
  if (is.null(tableName)){stop("The table name is required.")}
  if (is.null(definitionNames)) { stop("The set of definition names is required.") }

  con = SSim_GetConnection(project@filepath)
  on.exit(dbDisconnect(con))
  defs = SSim_GetDefinitionsCon(con, tableName, project@projectId)

  NotFound <- list()
  Extra <- list()

  for (i in 1:length(definitionNames)) {

    NewName = as.character(definitionNames[[i]]$NewName)

    if (nrow(subset(defs, Name == NewName)) == 0) {
      NotFound[length(NotFound) + 1] <- NewName
    }
  }

  for (i in 1:nrow(defs)) {

    Name = as.character(defs[i, "Name"])
    found = FALSE

    for (j in 1:length(definitionNames)){
      if (definitionNames[[j]]$NewName == Name){
        found = TRUE
        break
      }
    }

    if (!found){
      Extra[length(Extra) + 1] <- Name
    }
  }

  if (length(NotFound) > 0) {

    cat("\nERROR: The following definitions were not found:\n\n")
    paste(NotFound, collapse = '\n') %>% cat()
    cat("\n\n")

    stop()
  }

  if (length(Extra) > 0) {

    cat("\nERROR: The following unexpected definitions were found:\n\n")
    paste(Extra, collapse = '\n') %>% cat()
    cat("\n\n")

    stop()
  }
}

SSim_SetDefinitions <- function(project=NULL, tableName=NULL, pkName=NULL, definitionNames=NULL){

  if (is.null(project)){stop("The project is required.")}
  if (is.null(tableName)){stop("The table name is required.")}
  if (is.null(pkName)){stop("The primary key column name is required.")}
  if (is.null(definitionNames)){stop("The set of definition names is required.")}

  con = SSim_GetConnection(project@filepath)
  on.exit(dbDisconnect(con))
  defs = SSim_GetDefinitionsCon(con, tableName, project@projectId)

  for (i in 1:length(definitionNames)) {

    OldName = as.character(definitionNames[[i]]$OldName)
    NewName = as.character(definitionNames[[i]]$NewName)
    DefRow = subset(defs, Name==OldName)

    if (nrow(DefRow) == 1){

      if (nrow(subset(defs, Name==NewName)) == 1){
        stop(paste0("ERROR: Cannot rename definition to existing name: ", NewName))
      }

      pkid = as.integer(DefRow[pkName])
      q = sprintf("UPDATE %s SET Name='%s' WHERE %s=%d", tableName, NewName, pkName, pkid)
      SSim_ExecuteNonQueryCon(con, q)
    }
  }

  NotFound <- list()

  for (i in 1:length(definitionNames)) {

    OldName = as.character(definitionNames[[i]]$OldName)

    if (nrow(subset(defs, Name==OldName)) == 0){
      NotFound[length(NotFound)+1] <- OldName
    }
  }

  if (length(NotFound) > 0){

    cat("\nERROR: The following definitions were not found:\n\n")
    paste(NotFound, collapse = '\n') %>% cat()
    cat("\n\n")

    stop()
  }
}

SSim_ResetTransitionTypeGroups <- function(project, typeGroups){

  if (is.null(project)){stop("The project is required.")}
  if (is.null(typeGroups)){stop("The type/group data is required.")}

  # This table is just a joiner table do it is OK to delete all records

  q = sprintf("DELETE FROM STSim_TransitionTypeGroup WHERE ProjectID=%d", project@projectId)
  SSim_ExecuteNonQuery(project@filepath, q)

  # Add all the requested linkages.  If there are any that don't match actual types
  # or groups then SynroSim will reject the import.

  ds = datasheet(project, "STSim_TransitionTypeGroup", empty=T, optional=T)

  for (i in 1:length(typeGroups)) {

    tt = as.character(typeGroups[[i]]$tt)
    tg = as.character(typeGroups[[i]]$tg)
    ip = as.character(typeGroups[[i]]$ip)

    if (ip == "No"){ip = 0}
    else (ip = -1)

    ds = addRow(ds, data.frame(TransitionTypeID=tt, TransitionGroupID=tg, IsPrimary=ip, IsAuto=NA))
  }

  saveDatasheet(project, ds, "STSim_TransitionTypeGroup")
}

SSim_AddPrefix <- function(
  fileName=NULL, tableName=NULL, fieldName=NULL,
  prefix=NULL, projectId=NA){

  q = sprintf(
    "UPDATE %s SET %s='%s' || %s WHERE %s NOT LIKE '%s%%'",
    tableName, fieldName, prefix, fieldName, fieldName, prefix)

  if (!is.na(projectId)){
    q = paste0(q, sprintf(" AND ProjectId=%d", projectId))
  }

  SSim_ExecuteNonQuery(fileName, q)
}

SSim_RemovePrefix <- function(
  fileName=NULL, tableName=NULL, fieldName=NULL,
  prefix=NULL, projectId=NA){

  q = sprintf(
    "UPDATE %s SET %s=SUBSTR(%s, %d) WHERE %s LIKE '%s%%'",
    tableName, fieldName, fieldName, nchar(prefix) + 1, fieldName, prefix)

  if (!is.na(projectId)){
    q = paste0(q, sprintf(" AND ProjectId=%d", projectId))
  }

  SSim_ExecuteNonQuery(fileName, q)
}

SSim_PrefixField <- function(
  fileName=NULL, tableName=NULL, fieldName=NULL,
  prefix=NULL, add=TRUE, projectId=NA, allProjects=FALSE){

  if (is.null(tableName)){stop("The tableName is required.")}
  if (is.null(fieldName)){stop("The fieldName is required.")}
  if (is.null(prefix)){stop("The prefix is required.")}

  if (allProjects){
    if (!is.na(projectId)){
      stop("Not expecting a projectId when allProjects=T")
    }
  }else{
    if (is.na(projectId)){
      stop("The projectId is required if allProjects=F.")
    }
  }

  if (!is.na(projectId)){

    df = SSim_GetProjects(fileName)

    if (NROW(subset(df, ProjectID == projectId)) == 0){
      stop(paste0("The project does not exist: ", projectId))
    }
  }

  if (add){
    SSim_AddPrefix(
      fileName=fileName, tableName=tableName, fieldName=fieldName,
      prefix=prefix, projectId=projectId)
  }
  else{
    SSim_RemovePrefix(
      fileName=fileName, tableName=tableName, fieldName=fieldName,
      prefix=prefix, projectId=projectId)
  }
}

SSim_AddStratumPrefix <- function(
  fileName=NULL, prefix=NULL, projectId=NA, allProjects=FALSE){

  # Adds a prefix to an ST-Sim Stratum (if that prefix does not already exist)
  #
  # Args:
  #   fileName    : The ST-Sim library file name
  #   prefix      : The prefix to add
  #   projectId   : The Id of the target project (optional)
  #   allProjects : Whether or not to update all projects (projectId ignored if T)

  SSim_PrefixField(
    fileName=fileName, tableName="STSim_Stratum", fieldName="Name",
    prefix=prefix, add=T, projectId=projectId, allProjects = allProjects)
}

SSim_RemoveStratumPrefix <- function(
  fileName=NULL, prefix=NULL, projectId=NA, allProjects=FALSE){

  # Removes a prefix from an ST-Sim Stratum
  #
  # Args:
  #   fileName    : The ST-Sim library file name
  #   prefix      : The prefix to remove
  #   projectId   : The Id of the target project (optional)
  #   allProjects : Whether or not to update all projects (projectId ignored if T)

  SSim_PrefixField(
    fileName=fileName, tableName="STSim_Stratum", fieldName="Name",
    prefix=prefix, add=F, projectId=projectId, allProjects = allProjects)
}

SSim_UpdateMasterDefinitions <- function(fileName, projectName){
  
  if (projectName == "BpSModels"){
    stop("Please target a copy of original project 'BpSModels' (e.g. 'BpSModels - Copy')")
  }

  if (!SSim_ProjectExists(fileName, projectName)){
    stop(sprintf("The project does not exist: %s", projectName))
  }

  Lib = ssimLibrary(fileName)
  Prj = project(Lib, project=projectName)
  
  cat("Updating Transition Type names\n")
  
  TypeNames = list(
    data.frame(OldName="AltSuccession", NewName="Alternative Succession"),
    data.frame(OldName="CompetitionMaint", NewName="Competition or Maintenance"),
    data.frame(OldName="InsectDisease", NewName="Insects or Disease"),
    data.frame(OldName="MixedFire", NewName="Mixed Fire"),
    data.frame(OldName="NativeGrazing", NewName="Native Grazing"),
    data.frame(OldName="Optional1", NewName="Optional 1"),
    data.frame(OldName="Optional2", NewName="Optional 2"),
    data.frame(OldName="ReplacementFire", NewName="Replacement Fire"),
    data.frame(OldName="SurfaceFire", NewName="Surface Fire"),
    data.frame(OldName="WindWeatherStress", NewName="Wind or Weather or Stress"))

  SSim_SetDefinitions(Prj, "STSim_TransitionType", "TransitionTypeID", TypeNames)
  SSim_ValidateDefinitions(Prj, "STSim_TransitionType", TypeNames)
  
  cat("Updating Transition Group names\n")

  GroupNames = list(
    data.frame(OldName="AllFire", NewName="All Fire"),
    data.frame(OldName="AllTransitions", NewName="All Transitions"),
    data.frame(OldName="AlternativeSuccession", NewName="Alternative Succession"),
    data.frame(OldName="Competition/Maintenance", NewName="Competition or Maintenance"),
    data.frame(OldName="Insect/Disease", NewName="Insect or Disease"),
    data.frame(OldName="MixedFire", NewName="Mixed Fire"),
    data.frame(OldName="NativeGrazing", NewName="Native Grazing"),
    data.frame(OldName="NonFireDisturbances", NewName="Non Fire Disturbances"),
    data.frame(OldName="NonReplacementFire", NewName="Non Replacement Fire"),
    data.frame(OldName="Optional1", NewName="Optional 1"),
    data.frame(OldName="Optional2", NewName="Optional 2"),
    data.frame(OldName="OptionalTypes", NewName="Optional Types"),
    data.frame(OldName="ReplacementFire", NewName="Replacement Fire"),
    data.frame(OldName="SurfaceFire", NewName="Surface Fire"),
    data.frame(OldName="Wind/Weather/Stress", NewName="Wind or Weather or Stress"))

  SSim_SetDefinitions(Prj, "STSim_TransitionGroup", "TransitionGroupID", GroupNames)
  SSim_ValidateDefinitions(Prj, "STSim_TransitionGroup", GroupNames)
  
  cat("Creating Transition Type-Group linkages\n")

  TypeGroups = list(
    data.frame(tt="Mixed Fire", tg="All Fire", ip="No"),
    data.frame(tt="Replacement Fire", tg="All Fire", ip="No"),
    data.frame(tt="Surface Fire", tg="All Fire", ip="No"),
    data.frame(tt="Alternative Succession", tg="All Transitions", ip="Yes"),
    data.frame(tt="Competition or Maintenance", tg="All Transitions", ip="Yes"),
    data.frame(tt="Insects or Disease", tg="All Transitions", ip="Yes"),
    data.frame(tt="Mixed Fire", tg="All Transitions", ip="Yes"),
    data.frame(tt="Native Grazing", tg="All Transitions", ip="Yes"),
    data.frame(tt="Optional 1", tg="All Transitions", ip="Yes"),
    data.frame(tt="Optional 2", tg="All Transitions", ip="Yes"),
    data.frame(tt="Replacement Fire", tg="All Transitions", ip="Yes"),
    data.frame(tt="Surface Fire", tg="All Transitions", ip="Yes"),
    data.frame(tt="Wind or Weather or Stress", tg="All Transitions", ip="Yes"),
    data.frame(tt="Alternative Succession", tg="Alternative Succession", ip="No"),
    data.frame(tt="Competition or Maintenance", tg="Competition or Maintenance", ip="No"),
    data.frame(tt="Insects or Disease", tg="Insect or Disease", ip="No"),
    data.frame(tt="Mixed Fire", tg="Mixed Fire", ip="No"),
    data.frame(tt="Native Grazing", tg="Native Grazing", ip="No"),
    data.frame(tt="Competition or Maintenance", tg="Non Fire Disturbances", ip="No"),
    data.frame(tt="Insects or Disease", tg="Non Fire Disturbances", ip="No"),
    data.frame(tt="Native Grazing", tg="Non Fire Disturbances", ip="No"),
    data.frame(tt="Optional 1", tg="Non Fire Disturbances", ip="No"),
    data.frame(tt="Optional 2", tg="Non Fire Disturbances", ip="No"),
    data.frame(tt="Wind or Weather or Stress", tg="Non Fire Disturbances", ip="No"),
    data.frame(tt="Mixed Fire", tg="Non Replacement Fire", ip="No"),
    data.frame(tt="Surface Fire", tg="Non Replacement Fire", ip="No"),
    data.frame(tt="Optional 1", tg="Optional 1", ip="No"),
    data.frame(tt="Optional 2", tg="Optional 2", ip="No"),
    data.frame(tt="Optional 1", tg="Optional Types", ip="No"),
    data.frame(tt="Optional 2", tg="Optional Types", ip="No"),
    data.frame(tt="Replacement Fire", tg="Replacement Fire", ip="No"),
    data.frame(tt="Surface Fire", tg="Surface Fire", ip="No"),
    data.frame(tt="Wind or Weather or Stress", tg="Wind or Weather or Stress",	ip="No"))

  SSim_ResetTransitionTypeGroups(Prj, TypeGroups)
}

SSim_UpdateICFromDT <- function(project, scenario) {
  
  # assign the relative amount for each bps/slcass using the forumla:
  # 1/(the total number of sclasses for each stratum in ST-Sim's deterministic transitions)
  # Note: The ages will only be tracked in deterministic transisitons table
  
  # rsyncrosim (version 1.0.3 or earlier) has a bug where nothing is returned for the ICD table if the Stratum Name is a number.
  # Work around is prefixing your Stratum Name with a letter (e.g. "Bps-"). See apex forum post:
  # http://www.apexrms.com/forums/topic/what-is-the-name-of-the-initial-conditions-distribution-table-in-r/#post-5544
  # this version of the script has the prefix workaround; consider change when software is fixed
  
  SSim_AddStratumPrefix(project@filepath, "BpS", Prj@projectId, allProjects = F)
  on.exit(SSim_RemoveStratumPrefix(project@filepath, "BpS", Prj@projectId, allProjects = F))
  
  stratumSheet = datasheet(Prj, name = "STSim_Stratum")
  deterministicSheet = datasheet(Scn, name = "STSim_DeterministicTransition")
  ICDsheet = "STSim_InitialConditionsNonSpatialDistribution"
  ICDdata = datasheet(scenario, ICDsheet, empty = T)
  
  df <- data.frame(
    Iteration = integer(), StratumID = integer(), SecondaryStratumID = integer(), TertiaryStratumID = integer(),
    StateClassID = integer(), AgeMin = integer(), AgeMax = integer(), RelativeAmount = numeric(),
    stringsAsFactors = FALSE)
  
  for (i in 1:nrow(stratumSheet)) {
    
    strat = stratumSheet[i, "Name"]
    dtrows = subset(deterministicSheet, StratumIDSource == strat)
    
    if (nrow(dtrows) == 0) {
      next
    }
    
    for (j in 1:nrow(dtrows)) {
      
      it = NA
      
      if (!is.null(dtrows[j, "Iteration"])) {
        it = as.integer(dtrows[j, "Iteration"])
      }
      
      data = list(
        Iteration = it,
        StratumID = as.character(dtrows[j, "StratumIDSource"]),
        SecondaryStratumID = NA,
        TertiaryStratumID = NA,
        StateClassID = as.character(dtrows[j, "StateClassIDSource"]),
        AgeMin = NA,
        AgeMax = NA,
        RelativeAmount = 1 / nrow(dtrows)
      )
      
      df[nrow(df) + 1,] <- data
    }
  }
  
  ICDdata = merge(ICDdata, df, all=T)
  saveDatasheet(Scn, ICDdata, ICDsheet)
}

