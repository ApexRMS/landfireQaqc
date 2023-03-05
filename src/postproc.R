# Copyright © 2007-2018 Apex Resource Management Solution Ltd. (ApexRMS). All rights reserved.
# Post processing routines for a fully configured master LANDFIRE library.
#
# Required Packages: RSQLite

SSim_StratumIDFromCodeAndZone <- function(split, index, code, strata, isRename){
  
  zone = as.integer(split[[1]][index])
  
  if (is.na(zone)){
    stop(sprintf("ERROR: The ZONE value is not in the correct format: %s", zone))
  }   
  
  StratumName = sprintf("%02d%d", as.integer(zone), as.integer(code))
  StratumRow = subset(strata, Name==StratumName)
  
  if (nrow(StratumRow) == 1){
    return(StratumRow$StratumID)
  }else{
    
    prefix = "DELETE"
    
    if (isRename){
      prefix = "RENAME"
    }
  
    cat(sprintf("*** %s: Cannot find stratum - Please review XLS data: %s\n", prefix, StratumName))
    return(NA)
  }  
}

SSim_ExplicitDeleteStrata <- function(con, code, zones, strata){
  
  # The strata named ZONE{1..}+BPS_CODE are specifically marked for deletion
 
  split = strsplit(zones, "|", fixed = T)
  len = length(split[[1]])
  
  for(index in 1:len){
    
    zone = as.integer(split[[1]][index])
    DeleteStratumNameOrg = sprintf("%02d%d", as.integer(zone), as.integer(code))
    id = SSim_StratumIDFromCodeAndZone(split, index, code, strata, FALSE)
    
    if (!is.na(id)){
      cat(sprintf("Deleting stratum (EXPLICIT): %s\n", DeleteStratumNameOrg))
      SSim_DeleteStratumCon(con, id)      
    }
  }
}

SSim_GetNewStratumName <- function(code, zones){
  
  zonepart = gsub("|", "_", zones, fixed = TRUE)
  zonepart = gsub(" ", "", zonepart, fixed = TRUE)
  
  return (sprintf("%s_%s", code, zonepart))
}

SSim_RenameAndDeleteStrata <- function(con, code, zones, strata, projectId){
  
  # The stratum named  + ZONE{1}+BPS_CODE must be renamed to BPS_CODE_ZONE{1}
  # The other strata (ZONE{2..}+BPS_CODE) must be deleted
  
  split = strsplit(zones, "|", fixed = T)
  len = length(split[[1]])
  
  # Rename First Stratum
  FirstZone = as.integer(split[[1]][1])
  NewStratumName = SSim_GetNewStratumName(code, zones)
  
  # When we import we import to the new name and then delete the old one (and
  # any others that are obsolete).  In this case, the old stratum will not be there
  # anymore even though the XLS says it should be there and needs to be renamed.
  # So, we check to see if the target name is already in the database, and if it
  # is then we skip the rename operation.
  
  if (!SSim_StratumExistsDirect(con, NewStratumName, projectId)){
    
    FirstStratumID = SSim_StratumIDFromCodeAndZone(split, 1, code, strata, TRUE)
    
    if (!is.na(FirstStratumID)){
      
      FirstStratumNameOrg = sprintf("%02d%d", as.integer(FirstZone), as.integer(code))
      Query = sprintf("UPDATE STSim_Stratum SET Name = '%s' WHERE StratumID=%d", NewStratumName, as.integer(FirstStratumID))
      cat(sprintf("Renaming stratum: %s -> %s\n", FirstStratumNameOrg, NewStratumName))
      SSim_ExecuteNonQueryCon(con, Query)    
     
    }    
  }
  
  #Delete the others
  
  if (len >= 2){
    
    for(index in 2:len){
      
      zone = as.integer(split[[1]][index])
      DeleteStratumNameOrg = sprintf("%02d%d", as.integer(zone), as.integer(code))
      id = SSim_StratumIDFromCodeAndZone(split, index, code, strata, FALSE)
      
      if (!is.na(id)){
        cat(sprintf("Deleting obsolete stratum: %s\n", DeleteStratumNameOrg))
        SSim_DeleteStratumCon(con, id)        
      }
    }
    
  }
}

SSim_DeleteRenameFromConfig <- function(fileName, projectName){
  
  if (!file.exists("lfconfig.csv")){
    stop("ERROR: Cannot find file 'lfconfig.csv'")
  }
  
  Con = SSim_GetConnection(fileName)
  on.exit(function(){dbDisconnect(Con)})  
  dat = read.csv("lfconfig.csv", header = TRUE)
  ProjectId = SSim_GetProjectCon(Con, ProjectName)
  Strata = SSim_GetDefinitionsCon(Con, "STSim_Stratum", ProjectId)

  for (i in 1:nrow(dat)){
    
    # Model #,BPS_CODE,ZONES,DELETE,NOTES,,,
    # KB,10080,1| 2| 3| 7,,10080_1_2_3_7,,,
    # ,10090,19,,10090_19,,,
    
    BPS_CODE = as.character(dat[i, "BPS_CODE"])
    ZONES = as.character(dat[i, "ZONES"])
    DELETE = as.character(dat[i, "DELETE"])
    
    if (!is.na(DELETE) && DELETE == "TRUE"){
      SSim_ExplicitDeleteStrata(Con, BPS_CODE, ZONES, Strata)
    }else{
      SSim_RenameAndDeleteStrata(Con, BPS_CODE, ZONES, Strata, ProjectId)
    }
  }
}