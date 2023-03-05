# Copyright © 2007-2018 Apex Resource Management Solution Ltd. (ApexRMS). All rights reserved.
# Common routines for all LANDFIRE scripts.
#
# Required Packages: RSQLite

library(RSQLite)

SSim_GetConnection <- function(fileName=NULL){
  
  if (is.null(fileName)) {
    stop("The fileName is required.")
  }
  
  if (!file.exists(fileName)){
    stop(paste0("The file does not exist: ", fileName))
  }  
  
  return (dbConnect(RSQLite::SQLite(), fileName))
}

SSim_GetDataFrameCon <- function(con, query){
  return(dbGetQuery(con, query))  
}

SSim_GetDataFrame <- function(fileName, query){
  
  con = SSim_GetConnection(fileName)
  on.exit(dbDisconnect(con))
  
  return(SSim_GetDataFrameCon(con, query))
}

SSim_ExecuteNonQueryCon <- function(con, query){
  
  res = dbSendQuery(con, query)
  dbClearResult(res) 
}

SSim_ExecuteNonQuery <- function(fileName, query){
  
  con = SSim_GetConnection(fileName)
  on.exit(dbDisconnect(con))
  
  SSim_ExecuteNonQueryCon(con, query)
}

SSim_TableHasData <- function(con, tableName, scenarioId){

  q = sprintf("SELECT ScenarioID FROM %s WHERE ScenarioID=%d LIMIT 1", tableName, scenarioId)
  return (nrow(SSim_GetDataFrameCon(con, q)) == 1)
}

SSim_GetNextSequenceId <- function(con){
  
  SSim_ExecuteNonQueryCon(con, "DELETE FROM SSim_Sequence")
  SSim_ExecuteNonQueryCon(con, "INSERT INTO SSim_Sequence(Value) VALUES(0)")
  df = SSim_GetDataFrameCon(con, "SELECT ID FROM SSim_Sequence")
  
  return(df[1, "ID"])   
}

SSim_CreateDefinition <- function(con, projectId, tableName, pkColName, definitionName){
  
  pk = SSim_GetNextSequenceId(con)
  q = sprintf("INSERT INTO %s(%s, ProjectID, Name) VALUES(%d, %d, '%s')", tableName, pkColName, pk, projectId, definitionName)
  SSim_ExecuteNonQueryCon(con, q)
  
  return(pk)
}

SSim_ProjectExists <- function(fileName, projectName){
  
  p = SSim_GetProjects(fileName)
  s = subset(p, Name==projectName)
  
  return (nrow(s) == 1)
}

SSim_GetProjects <- function(fileName=NULL){
  
  q = "SELECT ProjectID, Name FROM SSim_Project ORDER BY ProjectID"
  return(SSim_GetDataFrame(fileName, q))
}

SSim_GetProjectsCon <- function(con){
  
  q = "SELECT ProjectID, Name FROM SSim_Project ORDER BY ProjectID"
  p = SSim_GetDataFrameCon(con, q)
}

SSim_GetProjectCon <- function(con, projectName){
  
  p = SSim_GetProjectsCon(con)
  s = subset(p, Name==projectName)
  
  if (nrow(s) == 0){
    stop (paste0("The project was not found: ", projectName))
  }
  
  return (s[1, "ProjectID"])
}

SSim_GetDefinitionsCon <- function(con=NULL, tableName=NULL, projectId=NA){
  
  q = sprintf("SELECT * FROM %s WHERE ProjectID=%d", tableName, projectId)
  return(SSim_GetDataFrameCon(con, q))
}

SSim_GetDefinitions <- function(fileName=NULL, tableName=NULL, projectId=NA){
  
  con = SSim_GetConnection(fileName)
  on.exit(dbDisconnect(con))
  
  q = sprintf("SELECT * FROM %s WHERE ProjectID=%d", tableName, projectId)
  return(SSim_GetDataFrameCon(con, q))
}

SSim_GetDefinitionID <- function(project, tableName, definitionName, pkColumnName) {
  
  defs = SSim_GetDefinitions(project@filepath, tableName, project@projectId)
  defrow = subset(defs, Name==definitionName)
  
  if (nrow(defrow) == 1){
    return (as.integer(defrow[pkColumnName]))
  }else{
    stop(paste0("The definition was not found: ", definitionName))
  }
}

SSim_DefinitionExists <- function(project, tableName, definitionName, pkColumnName){
  
  defs = SSim_GetDefinitions(project@filepath, tableName, project@projectId)
  defrow = subset(defs, Name==definitionName)
  
  (return (nrow(defrow) == 1))
}

SSim_StratumExistsDirect <- function(con, stratumName, projectId){
  
  Query = sprintf("SELECT * FROM STSim_Stratum WHERE Name='%s' AND ProjectID=%d", stratumName, as.integer(projectId))
  StratumRow = SSim_GetDataFrameCon(con, Query)
  return (nrow(StratumRow) == 1)
}

SSim_GetStratumID <- function(project, stratumName){
  return(SSim_GetDefinitionID(project, "STSim_Stratum", stratumName, "StratumID"))
}

SSim_GetStateClassID <- function(project, stateClassName){
  return(SSim_GetDefinitionID(project, "STSim_StateClass", stateClassName, "StateClassID"))
}

SSim_DeleteStratumCon <- function(con, stratumId){
  
  SSim_ExecuteNonQueryCon(con, sprintf("DELETE FROM STSim_Stratum WHERE StratumID=%d", stratumId))
  SSim_ExecuteNonQueryCon(con, sprintf("DELETE FROM STSim_TimeSinceTransitionGroup WHERE StratumID=%d", stratumId))
  SSim_ExecuteNonQueryCon(con, sprintf("DELETE FROM STSim_TimeSinceTransitionRandomize WHERE StratumID=%d", stratumId))
  SSim_ExecuteNonQueryCon(con, sprintf("DELETE FROM STSim_InitialConditionsNonSpatialDistribution WHERE StratumID=%d", stratumId))
  SSim_ExecuteNonQueryCon(con, sprintf("DELETE FROM STSim_Transition WHERE StratumIDSource=%d OR StratumIDDest=%d", stratumId, stratumId))
  SSim_ExecuteNonQueryCon(con, sprintf("DELETE FROM STSim_DeterministicTransition WHERE StratumIDSource=%d OR StratumIDDest=%d", stratumId, stratumId))
}

SSim_GetScenarioDataFrame <- function(con, projectId, projectName){
  
  q <- sprintf("SELECT ScenarioID, Name FROM SSim_Scenario WHERE ProjectID=%d AND RunStatus=0", projectId)
  scens = SSim_GetDataFrameCon(con, q) 
  
  if (nrow(scens) == 0){
    stop(sprintf("The project has no scenarios: %s (%d)", projectName, projectId))
  }
  
  if (nrow(scens) > 1){
    stop(sprintf("Not expecting multiple scenarios in project: %s (%d)", projectName, projectId))
  }
  
  return (scens)
}

SSim_GetSingleChildScenarioId <- function(fileName, projectId, projectName){
  
  con = SSim_GetConnection(fileName)
  on.exit(dbDisconnect(con))
  
  return (SSim_GetSingleChildScenarioIdCon(con, projectId, projectName))
}

SSim_GetSingleChildScenarioIdCon <- function(con, projectId, projectName){
  
  scens = SSim_GetScenarioDataFrame(con, projectId, projectName)
  return(scens$ScenarioID)
}

SSim_GetOldFormDeleteNames <- function(stratumName){
  
  if (!grepl("_", stratumName, fixed=TRUE)){
    stop(sprintf("Expecting '_' in stratum name: %s", stratumName))
  }
  
  c = list()
  split = strsplit(stratumName, "_", fixed = T)
  prefix = as.numeric(split[[1]][1])
  
  if (is.na(prefix)){
    stop(sprintf("The stratum name is not in the correct format: %s", stratumName))
  }
  
  len = length(split[[1]])
  
  if (len >= 2){
    
    for(index in 2:len){
      
      num = as.numeric(split[[1]][index])
      
      if (is.na(num)){
        stop(sprintf("The stratum name is not in the correct format: %s", stratumName))
      }   
      
      c[index-1] <- sprintf("%02d%d", num, prefix)
    }
    
  }
  
  return (c)
}

SSim_GetOldFormStratumName <- function(stratumName){
  
  if (!grepl("_", stratumName, fixed=TRUE)){
    stop(sprintf("Expecting '_' in stratum name: %s", stratumName))
  }
  
  split = strsplit(stratumName, "_", fixed = T)
  prefix = as.numeric(split[[1]][1])
  firstnum = as.numeric(split[[1]][2])
  
  if (is.na(prefix) || is.na(firstnum)){
    stop(sprintf("The stratum name is not in the correct format: %s", stratumName))
  }
  
  return (sprintf("%02d%d", firstnum, prefix))
}
