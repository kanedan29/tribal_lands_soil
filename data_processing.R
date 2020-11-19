# Load packages, specify fxn for finding the mode, create a list of files ----

library(tidyverse)
library(sf)
library(fasterize)
library(raster)
library(tigris)
library(parallel)
library(gdalUtils)

#

soil <- raster::stack(x = list.files("data/us_soilgrids/", full.names = TRUE))


data(fips_codes)
fips_codes 
fips_codes <- fips_codes %>%
  dplyr::filter(!state %in% c("AS","GU","MP","PR","UM","VI")) %>%
  dplyr::mutate(CH.GEOID = paste(state,county_code, sep = ""), 
                GEOID = paste(state_code,county_code, sep = "")) %>%
  dplyr::mutate(county = str_remove(county, " County"))


county.bounds <- counties(state = unique(fips_codes$state_code), cb = FALSE)

county.bounds <- st_transform(county.bounds, crs = proj4string(soil))

county.bounds.list <- split(county.bounds, f = county.bounds$GEOID)

  
# SOC
mclapply(county.bounds.list, mc.cores = 15, FUN = function(x){
  
  dst.filename <- paste("data/county_clips/soc",x[["GEOID"]], ".tif", sep = "")
  
  gdalwarp(overwrite = TRUE,
           srcfile ="data/us_soilgrids/soc.tif",
           dstfile = dst.filename,
           te = c(extent(x)@xmin,
                  extent(x)@ymin, 
                  extent(x)@xmax,
                  extent(x)@ymax), 
           tr = c(100,100))
  
  
})

# clay
mclapply(county.bounds.list, mc.cores = 15, FUN = function(x){
  
  dst.filename <- paste("data/county_clips/clay",x[["GEOID"]], ".tif", sep = "")
  
  gdalwarp(overwrite = TRUE,
           srcfile ="data/us_soilgrids/clay.tif",
           dstfile = dst.filename,
           te = c(extent(x)@xmin,
                  extent(x)@ymin, 
                  extent(x)@xmax,
                  extent(x)@ymax), 
           tr = c(100,100))
  
  
})

# sand
mclapply(county.bounds.list, mc.cores = 15, FUN = function(x){
  
  dst.filename <- paste("data/county_clips/sand",x[["GEOID"]], ".tif", sep = "")
  
  gdalwarp(overwrite = TRUE,
           srcfile ="data/us_soilgrids/sand.tif",
           dstfile = dst.filename,
           te = c(extent(x)@xmin,
                  extent(x)@ymin, 
                  extent(x)@xmax,
                  extent(x)@ymax), 
           tr = c(100,100))
  
  
})


# ph
mclapply(county.bounds.list, mc.cores = 15, FUN = function(x){
  
  dst.filename <- paste("data/county_clips/ph",x[["GEOID"]], ".tif", sep = "")
  
  gdalwarp(overwrite = TRUE,
           srcfile ="data/us_soilgrids/ph.tif",
           dstfile = dst.filename,
           te = c(extent(x)@xmin,
                  extent(x)@ymin, 
                  extent(x)@xmax,
                  extent(x)@ymax), 
           tr = c(100,100))
  
  
})

# ec
mclapply(county.bounds.list, mc.cores = 15, FUN = function(x){
  
  dst.filename <- paste("data/county_clips/ec",x[["GEOID"]], ".tif", sep = "")
  
  gdalwarp(overwrite = TRUE,
           srcfile ="data/us_soilgrids/ec.tif",
           dstfile = dst.filename,
           te = c(extent(x)@xmin,
                  extent(x)@ymin, 
                  extent(x)@xmax,
                  extent(x)@ymax), 
           tr = c(100,100))
  
  
})

#


county.soil.stats <- mclapply(names(county.bounds.list), mc.cores = 50, FUN = function(x){
  
  temp <- stack(list.files("data/county_clips/", pattern = x, full.names = TRUE))
  
  temp2 <- mask(temp, mask = county.bounds.list[[x]])
  names(temp2) <- str_sub(string = names(temp2), end = -6)
  
  soil.stats.temp <- cbind(x,
                           as.data.frame(t(cellStats(temp2, 'mean'))) %>%
                             rename_all(function(.) paste(.,"mean", sep = "_")),
                           as.data.frame(t(cellStats(temp2, 'sd'))) %>%
                             rename_all(function(.) paste(.,"sd", sep = "_")))
  names(soil.stats.temp)[1] <- "FIPS"
  
  # Return
  return(soil.stats.temp)
  
})

#

county.soil.stats.df <- plyr::ldply(county.soil.stats)[,1:11]

write_csv(county.soil.stats.df, "data/county_soil_stats.csv")

