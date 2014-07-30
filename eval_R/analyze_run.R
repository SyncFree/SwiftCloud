#Import and install required packages
require("ggplot2");
require("reshape2");
require("gridExtra");

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
  names(data) <- c("timestamp","type","sessionId","operation","duration")
  return (data)
}

select_OP_FAILURE <- function (data) {
  #Data format is:
  #timestamp_ms,APP_OP,session_id,operation_name,error_cause
  data <- subset(data,data$V2=="APP_OP_FAILURE")
  names(data) <- c("timestamp","type","sessionId","operation","cause")
  return (data)
}

select_METADATA <- function (data) {
  data <- subset(data,data$V2=="METADATA")
  #report type METADATA formatted as timestamp_ms,METADATA,session_id,message_name,total_message_size,version_or_update_size,value_size,
  #                                  explicitly_computed_global_metadata,batch_size,max_vv_size,max_vv_exceptions_num
  data <- transform(data, V5=as.numeric(V5), V6=as.numeric(V6), V7=as.numeric(V7),
                          V8=as.numeric(V8), V9=as.numeric(V9),
                          V10=as.numeric(V10), V11=as.numeric(V11))
  data$V9 <- sapply(data$V9, function(x) { return (max(x, 1)) })
  names(data) <- c("timestamp","type","sessionId","message","totalMessageSize",
                   "versionOrUpdateSize","valueSize","explicitGlobalMetadata","batchSize",
                   "maxVVSize","maxVVExceptionsNum")
  data$normalizedTotalMessageSize <- data$totalMessageSize / data$batchSize
  data$normalizedExplicitGlobalMetadata <- data$explicitGlobalMetadata / data$batchSize
  return (data)
}

process_experiment_run_dir <- function(dir, output_prefix, spectrogram=TRUE,summarized=TRUE) {
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
  dop <- select_OP(d)
  derr <- select_OP_FAILURE(d)
  dmetadata <- select_METADATA(d)
  dop_filtered <- select_OP(dfiltered)
  derr_filtered <- select_OP_FAILURE(dfiltered)
  dmetadata_filtered <- select_METADATA(dfiltered)
  rm(d)
  rm(dfiltered)

  # "SPECTROGRAM" MODE OUPUT
  if (spectrogram) {
    #Scatterplot for distribution over time  
    scatter.plot <- ggplot(dop, aes(timestamp,duration)) + geom_point(aes(color=operation))
    ggsave(scatter.plot, file=paste(output_prefix, "-response_time",format_ext,collapse="", sep=""), scale=1)
  
    #Histogram
    #p <- qplot(duration, data = d,binwidth=5,color=operation,geom="freqpoly") + facet_wrap( ~ sessionId)
    
    # Message occurences over time plot(s)
    for (m in unique(dmetadata$message)) {
      m_metadata <- subset(dmetadata, dmetadata$message==m) 
      msg.plot <- ggplot(m_metadata, aes(x=timestamp)) + geom_histogram(binwidth=1000) 
      # msg.plot
      ggsave(msg.plot, file=paste(output_prefix, "-msg_occur-", m, format_ext,collapse="", sep=""), scale=1)
    }
    # Message size and metadata size over time scatter plots
    msg_size.plot <- ggplot(dmetadata, aes(timestamp,totalMessageSize)) + geom_point(aes(color=message))
    ggsave(msg_size.plot, file=paste(output_prefix, "-msg_size",format_ext,collapse="", sep=""), scale=1)
  
    metadata_size.plot <- ggplot(dmetadata, aes(timestamp,explicitGlobalMetadata)) + geom_point(aes(color=message))
    ggsave(metadata_size.plot, file=paste(output_prefix, "-msg_meta",format_ext,collapse="", sep=""), scale=1)
  
    metadata_norm_size.plot <- ggplot(dmetadata, aes(timestamp, normalizedExplicitGlobalMetadata)) + geom_point(aes(color=message))
    ggsave(metadata_norm_size.plot, file=paste(output_prefix, "-msg_meta_norm",format_ext,collapse="", sep=""), scale=1)
  }

  # "SUMMARIZED" MODE OUTPUT
  if (summarized) {
    # Throughput over time plot
    # Careful: It seems that the first and last bin only cover 5000 ms
    throughput.plot <- ggplot(dop, aes(x=timestamp)) + geom_histogram(binwidth=1000) 
    #throughput.plot
    ggsave(throughput.plot, file=paste(output_prefix, "-throughput",format_ext,collapse="", sep=""), scale=1)
    
    # Operation duration CDF plot for the filtered period
    cdf.plot <- ggplot(dop_filtered, aes(x=duration)) + stat_ecdf(aes(colour=operation)) # + ggtitle (paste("TH",th))
    # cdf.plot
    ggsave(cdf.plot, file=paste(output_prefix, "-cdf",format_ext,collapse="", sep=""), scale=1)
    
    # common output format for descriptive statistics
    quantile_steps <- seq(from=0.0, to=1.0, by=0.001)
    stats <- c("mean", rep("permille", length(quantile_steps)))
    stats_params <- c(0, quantile_steps) 
  
    # Throughput / response time descriptive statistics over filtered data
    time_steps <- c(seq(min(dop_filtered$timestamp),max(dop_filtered$timestamp),by=1000), max(dop_filtered$timestamp))
    through <- hist(dop_filtered$timestamp, breaks=time_steps)
    # summary(through$counts)
    # summary(dop_filtered$duration)
    operations_stats <- data.frame(stat=stats, stat_param=stats_params,
                             throughput=c(mean(through$counts), quantile(through$counts, probs=quantile_steps)),
                             response_time=c(mean(dop_filtered$duration), quantile(dop_filtered$duration, probs=quantile_steps)))
    write.table(operations_stats, paste(output_prefix, "ops.csv", sep="-"), sep=",", row.names=FALSE)
  
    # Metadata size descriptive statistics
    message_size_stats <- data.frame(stat=stats, stat_params=stats_params)
    metadata_size_stats <- data.frame(stat=stats, stat_params=stats_params)
    for (m in unique(dmetadata_filtered$message)) {
      m_filtered <- subset(dmetadata_filtered, dmetadata_filtered$message==m)
      message_size_stats[[m]] <- c(mean(m_filtered$totalMessageSize), quantile(m_filtered$totalMessageSize, probs=quantile_steps))
      message_size_stats[[paste(m,"normalized",sep="-")]] <- c(mean(m_filtered$normalizedTotalMessageSize),
                                                              quantile(m_filtered$normalizedTotalMessageSize, probs=quantile_steps))
      metadata_size_stats[[m]] <- c(mean(m_filtered$explicitGlobalMetadata), quantile(m_filtered$explicitGlobalMetadata, probs=quantile_steps))
      metadata_size_stats[[paste(m, "normalized",sep="-")]] <- c(mean(m_filtered$normalizedExplicitGlobalMetadata),
                                                                 quantile(m_filtered$normalizedExplicitGlobalMetadata, probs=quantile_steps))
    }
    write.table(message_size_stats, paste(output_prefix, "msg_size.csv", sep="-"), sep=",", row.names=FALSE)
    write.table(metadata_size_stats, paste(output_prefix, "meta_size.csv", sep="-"), sep=",", row.names=FALSE)
  
    # Errors descriptive statistics
    errors.stats <- data.frame(cause=character(),occurences=integer())  
    # TODO: do it in R-idiomatic way
    for (c in unique(derr$cause)) {
      o <- nrow(subset(derr, derr$cause==c))
      errors.stats <- rbind(errors.stats, data.frame(cause=c, occurences=o))
    }
    write.table(errors.stats, paste(output_prefix, "errors.csv", sep="-"), sep=",", row.names=FALSE)
  }

  rm(dop)
  rm(derr)
  rm(dmetadata)
  rm(dop_filtered)
  rm(derr_filtered)
  rm(dmetadata_filtered)
}

process_experiment_run <- function(path, spectrogram, summarized) {
  if (file.info(path)$isdir) {
    prefix <- sub(paste(.Platform$file.sep, "$", sep=""),  "", path)
    output_prefix <- file.path(dirname(prefix), "processed", basename(prefix))
    process_experiment_run_dir(dir=path, output_prefix=output_prefix, spectrogram, summarized)
  } else {
    # presume it is a tar.gz archive
    run_id <- sub(".tar.gz", "", basename(path))
    tmp_dir <- tempfile(pattern=run_id)
    untar(path, exdir=tmp_dir, compressed="gzip")
    output_prefix <- file.path(dirname(path), "processed", run_id)
    process_experiment_run_dir(dir=tmp_dir,  output_prefix=output_prefix, spectrogram, summarized)
    unlink(tmp_dir, recursive=TRUE)
  }
}

fail_usage <- function() {
  stop("syntax: analyze_run.R <all|spectrogram|summarized> <directory or tar.gz archive with log files for a run>")
}

if (length(commandArgs(TRUE)) >= 2) {
  spectrogram <- TRUE
  summarized <- TRUE
  mode <- commandArgs(TRUE)[1]
  if (mode == "all") {
    # NOP
  } else if (mode == "spectrogram") {
    summarized <- FALSE
  } else if (mode == "summarized") {
    spectrogram <- FALSE
  } else {
    fail_usage()
  }
  path <- normalizePath(commandArgs(TRUE)[2])
  process_experiment_run(path, spectrogram, summarized)
} else {
  if (interactive()) {
    print("INTERACTIVE MODE: use process_experiment_run() function")
  } else {
    fail_usage()
  }
}

