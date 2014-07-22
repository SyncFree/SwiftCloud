#Import and install required packages
require("ggplot2");
require("reshape2");
require("gridExtra");



preprocess_OP <- function (data){
  #Data format is:
  #timestamp_ms,APP_OP,session_id,operation_name,duration_ms
  
  #Filter for batch updates
  data <- subset(data,data$V2=="APP_OP")
  data <- subset(data,data$V4=="read" | data$V4=="update")
  return (data)
}


toplevel_path <- "~/data_eval/50reads-50updates";
#file <- paste(path,"test_ycsb.csv",sep="/")


#toplevel_path <- "/home/zawir/workspace/swiftcloud/results/ycsb/workloada/test/"

file_list <- list.files(toplevel_path, pattern="*scout-stdout.log",recursive=TRUE)

file_list <- (paste(toplevel_path,file_list,sep="/"))


p <- list()
dc <- 1
su <- 1
sc <- 8
threads <- c(2,4,6,8)
throughput.stats <- data.frame(TH=integer(),mean=double(),median=double(),min=double(),max=double())
pruning <- 30000
notifications <- 1000
records <- "100000"
ops <- "500000"
#j <- grep(paste("DC",dc,"SU",su,"pruning",pruning,"notifications",notifications,"SC",sc,"TH",th,"records",records,"operations",ops,sep="-"),file_list)
for(th in threads) {
  j <- grep(paste("DC",dc,"SU",su,"SC",sc,"TH",th,"records",records,"operations",ops,sep="-"),file_list)
  
  d <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),operation=character(),duration=numeric())
  
  #Sort by number of threads
  for (file in file_list[j]){
    temp_dataset <- read.table(file,comment.char = "#", fill = TRUE, sep = ",", stringsAsFactors=FALSE);
    temp_dataset <- preprocess_OP(temp_dataset);
    d <- rbind(d, temp_dataset);
    rm(temp_dataset)
  }
  #Set names for columns
  names(d) <- c("timestamp","type","sessionId","operation","duration")
  
  #helper <- d
  #summary(d)
  
  #Filter out the rows where duration couldn't be parsed due to 
  d <- subset(d,!is.na(d$duration))
  
  # Common tasks for YCSB and SwiftSocial experiments:
  # TODO: aggregated throughput computation
  
  start_time <- min(d$timestamp)
  prune_start <- 10000
  duration_run <- 30000
  d <- subset(d,d$timestamp - start_time > prune_start)
  d <- subset(d,d$timestamp - start_time - prune_start < duration_run)
  
  
  #summary(d)
  
  # PLOTS
  format_ext <- ".png"
  
  #!Making scatter plots takes quite long for our data with > 1.000.000 entries!
  #Scatterplot for distribution over time
  
  # sampled_subset <- df[sample(nrow(df),n),]
  #scatter.plot <- ggplot(d, aes(timestamp,duration)) + geom_point(aes(color=operation))
  #ggsave(scatter.plot, file=paste(toplevel_path, "timeline-TH",th,format_ext,collapse=""), scale=1)
  
  # CDF Plot
  cdf.plot <- ggplot(d, aes(x=duration)) + stat_ecdf(aes(colour=operation)) + ggtitle (paste("TH",th))
  cdf.plot
  ggsave(cdf.plot, file=paste(toplevel_path, "cdf-TH",th,format_ext,collapse=""), scale=1)
  
  #Histogram
  #p <- qplot(duration, data = d,binwidth=5,color=operation,geom="freqpoly") + facet_wrap( ~ sessionId)
  
  #Throughput plot
  #Careful: It seems that the first and last bin only cover 5000 ms
  throughput.plot <- ggplot(d, aes(x=timestamp)) + geom_histogram(binwidth=1000) 
  throughput.plot
  ggsave(throughput.plot, file=paste(toplevel_path, "throughput-TH",th,format_ext,collapse=""), scale=1)
  
  
  steps <- c(seq(min(d$timestamp),max(d$timestamp),by=1000), max(d$timestamp)) 
  through <- hist(d$timestamp, breaks=steps)
  summary(through$counts)
  newd <- data.frame(TH=th,mean=mean(through$counts),min=min(through$counts),median=median(through$counts),max=max(through$counts))
  rbind(throughput.stats,newd)
  rm(d)  
}

throughput.stats


