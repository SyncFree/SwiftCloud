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

format_ext <- ".png"

adjust_timestamps <- function (data){
  #Data format is:
  #timestamp_ms,...
  start_timestamp = min(data$V1)
  data <- transform(data, V1=(data$V1 - start_timestamp))
  return (data)
}

select_OP <- function (data){
  #Data format is:
  #timestamp_ms,APP_OP,session_id,operation_name,duration_ms
  
  #Filter for batch updates
  data <- subset(data,data$V2=="APP_OP")
  # since d contains variety of different entries, this casting is needed
  data <- transform(data, V5 = as.numeric(V5))
  # data <- subset(data,data$V4=="read" | data$V4=="update")
  return (data)
}

select_OP_FAILURE <- function (data) {
  #Data format is:
  #timestamp_ms,APP_OP,session_id,operation_name,error_cause
  data <- subset(data,data$V2=="APP_OP_FAILURE")
  return (data)
}

select_METADATA <- function (data) {
  data <- subset(data,data$V2=="METADATA")
  #report type METADATA formatted as timestamp_ms,METADATA,session_id,message_name,total_message_size,version_or_update_size,value_size,
  #                                  explicitly_computed_global_metadata,batch_size,max_vv_size,max_vv_exceptions_num
  data <- transform(data, V5=as.numeric(V5), V6=as.numeric(V6), V7=as.numeric(V7),
                          V8=as.numeric(V8), V9=as.numeric(V9), V10=as.numeric(V10),
                          V11=as.numeric(V11))
  return (data)
}

process_experiment_run_dir <- function(dir, run_id, output_prefix) {
  file_list <- list.files(dir, pattern="*scout-stdout.log",recursive=TRUE,full.names=TRUE)
  
  if (length(file_list) == 0) {
    warning(paste("No logs found in", dir))
  }
  
  dir.create(dirname(output_prefix), recursive=TRUE,showWarnings=FALSE)
  
  d <- data.frame()
  for (file in file_list){
    temp_dataset <- read.table(file,comment.char = "#", fill = TRUE, sep = ",", stringsAsFactors=FALSE);
    d <- rbind(d, temp_dataset)
    rm(temp_dataset)
  }
  print(paste("Loaded", length(file_list), "files"))
  #summary(d)
  #Filter out the rows where duration couldn't be parsed due to 
  #d <- subset(d,!is.na(d$duration))
  d <- adjust_timestamps(d)
  # That should include at least one pruning point (60s)
  prune_start <- 150000
  duration_run <- 100000
  # V1 = timestamp
  dfiltered <- subset(d,d$V1 > prune_start & d$V1 - prune_start < duration_run)
  
  # can it be done in a more compact way?
  dop <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),operation=character(),duration=numeric())
  derr <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),operation=character(),cause=character())
  dmetadata <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),message=character(),totalMessageSize=numeric(),
                          versionOrUpdateSize=numeric(),valueSize=numeric(),explicitGlobalMetadata=numeric(),batchSize=numeric(),
                          maxVVSize=numeric(),maxVVExceptionsNum=numeric())
  dop_filtered <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),operation=character(),duration=numeric())
  derr_filtered <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),operation=character(),cause=character())
  dmetadata_filtered <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),message=character(),totalMessageSize=numeric(),
                          versionOrUpdateSize=numeric(),valueSize=numeric(),explicitGlobalMetadata=numeric(),batchSize=numeric(),
                          maxVVSize=numeric(),maxVVExceptionsNum=numeric())
  dop <- select_OP(d)
  derr <- select_OP_FAILURE(d)
  dmetadata <- select_METADATA(d)
  dop_filtered <- select_OP(dfiltered)
  derr_filtered <- select_OP_FAILURE(dfiltered)
  dmetadata_filtered <- select_METADATA(dfiltered)
  #Set names for columns
  names(dop) <- c("timestamp","type","sessionId","operation","duration")
  names(dop_filtered) <- names(dop)
  names(derr) <- c("timestamp","type","sessionId","operation","cause")
  names(derr_filtered) <- names(derr)
  names(dmetadata) <- c("timestamp","type","sessionId","message","totalMessageSize",
                   "versionOrUpdateSize","valueSize","explicitGlobalMetadata","batchSize",
                   "maxVVSize","maxVVExceptionsNum")
  names(dmetadata_filtered) <- names(dmetadata)
  rm(d)
  rm(dfiltered)

  # PLOTS
  #!Making scatter plots takes quite long for our data with > 1.000.000 entries!
  #Scatterplot for distribution over time  
  scatter.plot <- ggplot(dop, aes(timestamp,duration)) + geom_point(aes(color=operation))
  ggsave(scatter.plot, file=paste(output_prefix, "-response_time",format_ext,collapse="", sep=""), scale=1)

  # Throughput over time plot
  # Careful: It seems that the first and last bin only cover 5000 ms
  throughput.plot <- ggplot(dop, aes(x=timestamp)) + geom_histogram(binwidth=1000) 
  #throughput.plot
  ggsave(throughput.plot, file=paste(output_prefix, "-throughput",format_ext,collapse="", sep=""), scale=1)
  
  # Operation duration CDF plot for the filtered period
  cdf.plot <- ggplot(dop_filtered, aes(x=duration)) + stat_ecdf(aes(colour=operation)) # + ggtitle (paste("TH",th))
  # cdf.plot
  ggsave(cdf.plot, file=paste(output_prefix, "-cdf",format_ext,collapse="", sep=""), scale=1)
  
  # TODO: record CDF information in a CSV file
  
  #Histogram
  #p <- qplot(duration, data = d,binwidth=5,color=operation,geom="freqpoly") + facet_wrap( ~ sessionId)
  
  # Message occurences over time plot(s)
  for (m in unique(dmetadata$message)) {
    metadata.plot <- ggplot(subset(dmetadata, dmetadata$message==m), aes(x=timestamp)) + geom_histogram(binwidth=1000) 
    # metadata.plot
    ggsave(metadata.plot, file=paste(output_prefix, "-message_occurence-", m,format_ext,collapse="", sep=""), scale=1)
  }
  
  # Throughput / response time descriptive statistics over filtered data
  operations <- data.frame(run_id=character(),throughput_mean=double(),throughput_median=double(),
                           throughput_quant25=double(),throughput_quant75=double(),
                           response_time_mean=double(),response_time_median=double(),
                           response_time_quant25=double(),response_time_quant75=double())
         
  steps <- c(seq(min(dop_filtered$timestamp),max(dop_filtered$timestamp),by=1000), max(dop_filtered$timestamp))
  through <- hist(dop_filtered$timestamp, breaks=steps)
  summary(through$counts)
  summary(dop_filtered$duration)
  # TODO: record all quantiles in a table
  operations <- rbind(operations, data.frame(run_id=run_id,throughput_mean=mean(through$counts),
                      throughput_median=median(through$counts), throughput_quant25=quantile(through$counts, probs=c(0.25)),
                      throughput_quant75=quantile(through$counts, probs=c(0.75)),
                      response_time_mean=mean(dop_filtered$duration),response_time_median=median(dop_filtered$duration),
                      response_time_quantt25=quantile(dop_filtered$duration, probs=c(0.25)),
                      response_time_quant75=quantile(dop_filtered$duration, probs=c(0.75))))
  write.table(operations, paste(output_prefix, "operations_stats.csv", sep="-"), sep=",", row.names=FALSE)

  # TODO: metadata descriptive stats
  
  # Errors descriptive statistics
  errors.stats <- data.frame(run_id=character(),cause=character(),occurences=integer())  
  # TODO: do it in R-idiomatic way
  for (c in unique(derr$cause)) {
    o <- nrow(subset(derr, derr$cause==c))
    errors.stats <- rbind(errors.stats, data.frame(run_id=run_id, cause=c, occurences=o))
  }
  write.table(errors.stats, paste(output_prefix, "errors.csv", sep="-"), sep=",", row.names=FALSE)

  rm(dop)
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
