#Import and install required packages
require("ggplot2");
require("reshape2");
require("gridExtra");

if (length(commandArgs(TRUE)) > 0) {
  path <- commandArgs(TRUE)[1]
} else {
  #path <- "/home/zawir/code/swiftcloud/results/test/Jul25-1406269793327-d8e77ae1-workloada-mode-notifications-cap-1400.tar.gz"
  stop("syntax: analyze_run.R <directory or tar.gz archive with log files for a run>")
}

select_OP <- function (data){
  #Data format is:
  #timestamp_ms,APP_OP,session_id,operation_name,duration_ms
  
  #Filter for batch updates
  data <- subset(data,data$V2=="APP_OP")
  # since d contains variety of different entries, this casting is needed
  data$V5 <- as.numeric(data$V5)
  # data <- subset(data,data$V4=="read" | data$V4=="update")
  return (data)
}

select_OP_FAILURE <- function (data) {
  data <- subset(data,data$V2=="APP_OP_FAILURE")
  return (data)
}

select_METADATA <- function (data) {
  data <- subset(data,data$V2=="METADATA")
  #report type METADATA formatted as timestamp_ms,METADATA,session_id,message_name,total_message_size,version_or_update_size,value_size,
  #                                  explicitly_computed_global_metadata,batch_size,max_vv_size,max_vv_exceptions_num
  data$V5 <- as.numeric(data$V5)
  data$V6 <- as.numeric(data$V6)
  data$V7 <- as.numeric(data$V7)
  data$V8 <- as.numeric(data$V8)
  data$V9 <- as.numeric(data$V9)
  data$V10 <- as.numeric(data$V10)
  data$V11 <- as.numeric(data$V11)
  return (data)
}

process_experiment_run_dir <- function(dir, run_id, output_prefix) {
  file_list <- list.files(dir, pattern="*scout-stdout.log",recursive=TRUE,full.names=TRUE)
  
  if (length(file_list) == 0) {
    warning(paste("No logs found in", dir))
  }
  
  dir.create(dirname(output_prefix), recursive=TRUE,showWarnings=FALSE)
  
  d <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),operation=character(),duration=numeric())
  derr <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),operation=character(),cause=character())
  dmetadata <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),message=character(),totalMessageSize=numeric(),
                          versionOrUpdateSize=numeric(),valueSize=numeric(),explicitGlobalMetadata=numeric(),batchSize=numeric(),
                          maxVVSize=numeric(),maxVVExceptionsNum=numeric())                          
  
  #Sort by number of threads
  for (file in file_list){
    temp_dataset <- read.table(file,comment.char = "#", fill = TRUE, sep = ",", stringsAsFactors=FALSE);
    op_dataset <- select_OP(temp_dataset);
    d <- rbind(d, op_dataset)
    rm(op_dataset)
    
    err_dataset <- select_OP_FAILURE(temp_dataset);
    derr <-rbind(derr, err_dataset)
    rm(err_dataset)
    
    metadata_dataset <- select_METADATA(temp_dataset);
    dmetadata <-rbind(dmetadata, metadata_dataset)
    rm(metadata_dataset)
    
    rm(temp_dataset)
  }
  print(paste("Loaded", length(file_list), "files"))
  
  #helper <- d
  #summary(d)
  
  #Filter out the rows where duration couldn't be parsed due to 
  #d <- subset(d,!is.na(d$duration))
  
  #Set names for columns
  names(d) <- c("timestamp","type","sessionId","operation","duration")
  names(derr) <- c("timestamp","type","sessionId","operation","cause")
  names(dmetadata) <- c("timestamp","type","sessionId","message","totalMessageSize",
                   "versionOrUpdateSize","valueSize","explicitGlobalMetadata","batchSize",
                   "maxVVSize","maxVVExceptionsNum")
  
  # Common tasks for YCSB and SwiftSocial experiments:
  # TODO: aggregated throughput computation
  
  start_time <- min(d$timestamp)
  prune_start <- 150000
  # That should include at least one pruning point (60s)
  duration_run <- 100000
  d <- subset(d,d$timestamp - start_time > prune_start)
  d <- subset(d,d$timestamp - start_time - prune_start < duration_run)

  dmetadata <- subset(dmetadata,dmetadata$timestamp - start_time > prune_start)
  dmetadata <- subset(dmetadata,dmetadata$timestamp - start_time - prune_start < duration_run)
  
  #summary(d)
  
  # PLOTS
  format_ext <- ".png"
  
  #!Making scatter plots takes quite long for our data with > 1.000.000 entries!
  #Scatterplot for distribution over time
  
  # sampled_subset <- df[sample(nrow(df),n),]
  scatter.plot <- ggplot(d, aes(timestamp,duration)) + geom_point(aes(color=operation))
  ggsave(scatter.plot, file=paste(output_prefix, "-operation_latencies",format_ext,collapse="", sep=""), scale=1)
  
  # Operation duration CDF Plot
  cdf.plot <- ggplot(d, aes(x=duration)) + stat_ecdf(aes(colour=operation)) # + ggtitle (paste("TH",th))
  cdf.plot
  ggsave(cdf.plot, file=paste(output_prefix, "-cdf",format_ext,collapse="", sep=""), scale=1)
  
  # TODO: record CDF information in a CSV file
  
  #Histogram
  #p <- qplot(duration, data = d,binwidth=5,color=operation,geom="freqpoly") + facet_wrap( ~ sessionId)
  
  # Throughput over time plot
  # Careful: It seems that the first and last bin only cover 5000 ms
  throughput.plot <- ggplot(d, aes(x=timestamp)) + geom_histogram(binwidth=1000) 
  throughput.plot
  ggsave(throughput.plot, file=paste(output_prefix, "-throughput",format_ext,collapse="", sep=""), scale=1)
  
  # Message occurences historgram
  for (m in unique(dmetadata$message)) {
    metadata.plot <- ggplot(subset(dmetadata, dmetadata$message==m), aes(x=timestamp)) + geom_histogram(binwidth=1000) 
    metadata.plot
    ggsave(metadata.plot, file=paste(output_prefix, "-message-occurence", m,format_ext,collapse="", sep=""), scale=1)
  }
  
  
  # Throughput descriptive statistics
  throughput.stats <- data.frame(run_id=character(),mean=double(),median=double(),min=double(),max=double())
  steps <- c(seq(min(d$timestamp),max(d$timestamp),by=1000), max(d$timestamp)) 
  through <- hist(d$timestamp, breaks=steps)
  summary(through$counts)
  newd <- data.frame(run_id=run_id,mean=mean(through$counts),min=min(through$counts),median=median(through$counts),max=max(through$counts))
  throughput.stats <- rbind(throughput.stats,newd)
  write.table(throughput.stats, paste(output_prefix, "throughput.csv", sep="-"), sep=",", row.names=FALSE)
  
  # Errors descriptive statistics
  errors.stats <- data.frame(run_id=character(),cause=character(),occurences=integer())  
  # TODO: do it in R-idiomatic wa y
  for (c in unique(derr$cause)) {
    o <- nrow(subset(derr, derr$cause==c))
    errors.stats <- rbind(errors.stats, data.frame(run_id=run_id, cause=c, occurences=o))
  }
  write.table(errors.stats, paste(output_prefix, "errors.csv", sep="-"), sep=",", row.names=FALSE)

  rm(d)
  rm(derr)
  rm(dmetadata)
}
                                                                                                                                                                                  
path <- normalizePath(path)

if (file.info(path)$isdir) {
  prefix <- sub(paste(.Platform$file.sep, "$", sep=""),  "", path)
  output_prefix <- file.path(dirname(prefix), "processed", basename(prefix))
  process_experiment_run_dir(dir=path, run_id=basename(path), output_prefix=output_prefix)
} else {
  # presume it is a tar.gz archive
  run_id <- sub(".tar.gz", "", basename(path))
  tmp_dir <- tempfile(pattern=run_id)
  untar(path, exdir=tmp_dir, compressed="gzip")
  output_prefix <- file.path(dirname(path), "processed", run_id)
  process_experiment_run_dir(dir=tmp_dir, run_id=run_id, output_prefix=output_prefix)
  unlink(tmp_dir, recursive=TRUE)
}


# The following variables and for loops are ad-hoc per experiment family processing
#modes <- c("no-caching", "notifications", "refresh-frequent", "refresh-infrequent")
#caps <- c(6000,8000,10000)
#threads <- c(4,12,20,28,36,44,52,60)
#for (mode in modes) {
  #for (cap in caps) {
    #for(th in threads) {
    #}
  #}
#}
