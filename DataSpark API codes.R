library(httr)
library(dplyr)
library(jsonlite)

#parameters
datastreamx.key = "b7NQjKGGW3clAWKzXmekkToRj6J4mEJ0"

#Create a list of subzone ids to run in subsequent for loop.
dataframe <- read.csv("subzone codes.csv", header = F, sep = ",")
xy.list <- as.list.data.frame(t(dataframe))
xy.list[1]

#Run it once to create the csv file
query.body <- list(
  date = "2017-05-30",
  location = list(locationType = "locationHierarchyLevel", levelType = "discrete_visit_subzone", id = 'JWSZ08'),
  queryGranularity = list(type = "period", period = "PT1H"),
  aggregations = list(list(metric = "unique_agents", type = "hyperUnique",
                           describedAs = "footfall"))
)

# token variable contains a valid access token; see Getting Started.
query.response <- POST("http://api.datastreamx.com/1925/605/v1/discretevisit/v2/query",
                       add_headers('DataStreamX-Data-Key' = datastreamx.key),
                       encode = "json",
                       body = query.body,
                       verbose())

stop_for_status(query.response)
cat(content(query.response, as = "text"), "\n")


#convert query response to JSON

data <- fromJSON(rawToChar(query.response$content))
data.df <- do.call(what = "cbind", args = lapply(data, as.data.frame))
names(data.df)[1] = names(data[1])


#write csv to working directory

write.table(data.df, file = "API-output2.csv", sep = ",", col.names = T)

#use for loop to append to previously created csv files
for (i in xy.list){
  query.body <- list(
    date = "2017-05-30",
    location = list(locationType = "locationHierarchyLevel", levelType = "discrete_visit_subzone", id = i),
    queryGranularity = list(type = "period", period = "PT1H"),
    aggregations = list(list(metric = "unique_agents", type = "hyperUnique",
                             describedAs = "footfall"))
  )
  
  # token variable contains a valid access token; see Getting Started.
  query.response <- POST("http://api.datastreamx.com/1925/605/v1/discretevisit/v2/query",
                         add_headers('DataStreamX-Data-Key' = datastreamx.key),
                         encode = "json",
                         body = query.body,
                         verbose())
  
  stop_for_status(query.response)
  cat(content(query.response, as = "text"), "\n")


#convert query response to JSON

  data <- fromJSON(rawToChar(query.response$content))
  data.df <- do.call(what = "cbind", args = lapply(data, as.data.frame))
  names(data.df)[1] = names(data[1])


#write csv to working directory

  write.table(data.df, file = "API-output2.csv", append = TRUE, col.names = F, sep = ",")
}
  