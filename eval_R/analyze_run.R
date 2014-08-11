#Import and install required packages
require("ggplot2");
require("reshape2");
require("gridExtra");

format_ext <- ".png"

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
                          V10=as.numeric(V10), V11=as.numeric(V11), V12=as.numeric(V12), V13=as.numeric(V13))
  data$V9 <- sapply(data$V9, function(x) { return (max(x, 1)) })
  names(data) <- c("timestamp","type","sessionId","message","totalMessageSize",
                   "versionOrUpdateSize","valueSize","explicitGlobalMetadata","batchSizeFinestGrained",
                   "batchSizeFinerGrained", "batchSizeCoarseGrained", "maxVVSize","maxVVExceptionsNum")
  data$normalizedTotalMessageSizeByBatchSizeFinestGrained <- data$totalMessageSize / data$batchSizeFinestGrained
  data$normalizedTotalMessageSizeByBatchSizeFinerGrained <- data$totalMessageSize / data$batchSizeFinerGrained
  data$normalizedTotalMessageSizeByBatchSizeCoarseGrained <- data$totalMessageSize / data$batchSizeCoarseGrained
  data$normalizedExplicitGlobalMetadataByBatchSizeFinestGrained <- data$explicitGlobalMetadata / data$batchSizeFinestGrained
  data$normalizedExplicitGlobalMetadataByBatchSizeFinerGrained <- data$explicitGlobalMetadata / data$batchSizeFinerGrained
  data$normalizedExplicitGlobalMetadataByBatchSizeCoarseGrained <- data$explicitGlobalMetadata / data$batchSizeCoarseGrained
  return (data)
}

select_DATABASE_TABLE_SIZE <- function (data) {
  data <- subset(data,data$V2=="DATABASE_TABLE_SIZE")
  data <- transform(data, V5=as.numeric(V5))
  names(data) <- c("timestamp","type","nodeId","tableName","tableSize")
  return (data)
}

select_IDEMPOTENCE_GUARD_SIZE <- function (data) {
  data <- subset(data,data$V2=="IDEMPOTENCE_GUARD_SIZE")
  data <- transform(data, V4=as.numeric(V4))
  names(data) <- c("timestamp","type","nodeId","idempotenceGuardSize")
  return (data)
}

process_experiment_run_dir <- function(dir, output_prefix, spectrogram=TRUE,summarized=TRUE) {
  # Scout logs
  file_list <- list.files(dir, pattern="*scout-stdout.log",recursive=TRUE,full.names=TRUE)
  
  if (length(file_list) == 0) {
    warning(paste("No scout logs found in", dir))
  }

  d <- data.frame()
  for (file in file_list){
    temp_dataset <- read.table(file,comment.char = "#", fill = TRUE, sep = ",", stringsAsFactors=FALSE);
    d <- rbind(d, temp_dataset)
    rm(temp_dataset)
  }
  print(paste("Loaded", length(file_list), "scout log files"))
  #summary(d)
  #Data format is:
  #timestamp_ms,...
  min_exp_timestamp = min(d$V1)
  d <- transform(d, V1=(d$V1 - min_exp_timestamp))
  # That should include at least one pruning point (60s)
  prune_start <- 350000
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
  
  # DC logs
  dc_file_list <- list.files(dir, pattern="*sur-stdout.log",recursive=TRUE,full.names=TRUE)
  
  if (length(dc_file_list) == 0) {
    warning(paste("No DC logs found in", dir))
  }
  
  ddc <- data.frame()
  for (file in dc_file_list){
    temp_dataset <- read.table(file,comment.char = "#", fill = TRUE, sep = ",", stringsAsFactors=FALSE);
    ddc <- rbind(ddc, temp_dataset)
    rm(temp_dataset)
  }
  print(paste("Loaded", length(dc_file_list), "DC log files"))
  ddc <- transform(ddc, V1=(ddc$V1 - min_exp_timestamp))
  ddc_filtered <- subset(ddc,ddc$V1 > prune_start & ddc$V1 - prune_start < duration_run)
  
  # TODO: extrapolate IDEMPOTENCE_GUARD_SIZE entries beyond initialization phase
  
  dtablesize <- select_DATABASE_TABLE_SIZE(ddc)
  dguardsize <- select_IDEMPOTENCE_GUARD_SIZE(ddc)
  dtablesize_filtered <- select_DATABASE_TABLE_SIZE(ddc_filtered)
  dguardsize_filtered <- select_IDEMPOTENCE_GUARD_SIZE(ddc_filtered)

  # Create destination directory
  dir.create(dirname(output_prefix), recursive=TRUE, showWarnings=FALSE)
  
  # "SPECTROGRAM" MODE OUPUT
  if (spectrogram) {
    # Response time scatterplot over time  
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
  
    metadata_norm1_size.plot <- ggplot(dmetadata, aes(timestamp, normalizedExplicitGlobalMetadataByBatchSizeFinestGrained)) + geom_point(aes(color=message))
    ggsave(metadata_norm1_size.plot, file=paste(output_prefix, "-msg_meta_norm1",format_ext,collapse="", sep=""), scale=1)

    metadata_norm2_size.plot <- ggplot(dmetadata, aes(timestamp, normalizedExplicitGlobalMetadataByBatchSizeFinerGrained)) + geom_point(aes(color=message))
    ggsave(metadata_norm2_size.plot, file=paste(output_prefix, "-msg_meta_norm2",format_ext,collapse="", sep=""), scale=1)
    
    metadata_norm3_size.plot <- ggplot(dmetadata, aes(timestamp, normalizedExplicitGlobalMetadataByBatchSizeCoarseGrained)) + geom_point(aes(color=message))
    ggsave(metadata_norm3_size.plot, file=paste(output_prefix, "-msg_meta_norm3",format_ext,collapse="", sep=""), scale=1)
    
    # Storage metadata/db size, plots over time
    db_table_size.plot <- ggplot()
    guard_size.plot <- ggplot()
    for (eachDc in unique(dtablesize$nodeId)) {
      dctablesize <- subset(dtablesize, dtablesize$nodeId == eachDc)
      dctablesize$dcTable <- paste("DC", dctablesize$nodeId, ", table", dctablesize$tableName)
      for (tab in unique(dctablesize$tableName)) {
        tabdctablesize <- subset(dctablesize, dctablesize$tableName == tab)
        db_table_size.plot <- db_table_size.plot + geom_line(data=tabdctablesize, mapping=aes(timestamp,tableSize, color=dcTable))
      }
    }

    for (eachDc in unique(dguardsize$nodeId)) {
      dcguardsize <- subset(dguardsize, dguardsize$nodeId == eachDc)
      guard_size.plot <- guard_size.plot + geom_line(data=dcguardsize, mapping=aes(timestamp,idempotenceGuardSize,color=nodeId))
    }

    ggsave(db_table_size.plot, file=paste(output_prefix, "-table_size",format_ext,collapse="", sep=""), scale=1)
    ggsave(guard_size.plot, file=paste(output_prefix, "-guard_size",format_ext,collapse="", sep=""), scale=1)
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
    stats <- c("mean", "stddev", rep("permille", length(quantile_steps)))
    stats_params <- c(0, 0, quantile_steps)
    compute_stats <- function(vec) {
      return (c(mean(vec), sd(vec), quantile(vec, probs=quantile_steps)))
    }
  
    # Throughput / response time descriptive statistics over filtered data
    time_steps <- c(seq(min(dop_filtered$timestamp),max(dop_filtered$timestamp),by=1000), max(dop_filtered$timestamp))
    through <- hist(dop_filtered$timestamp, breaks=time_steps)
    # summary(through$counts)
    # summary(dop_filtered$duration)

    # TODO: add colors by session
    operations_stats <- data.frame(stat=stats, stat_param=stats_params,
                             throughput=compute_stats(through$counts),
                             response_time=compute_stats(dop_filtered$duration))
    write.table(operations_stats, paste(output_prefix, "ops.csv", sep="-"), sep=",", row.names=FALSE)
    
    # Metadata size descriptive statistics
    metadata_size_stats <- data.frame(stat=stats, stat_params=stats_params)
    for (m in unique(dmetadata_filtered$message)) {
      m_filtered <- subset(dmetadata_filtered, dmetadata_filtered$message==m)
      metadata_size_stats[[paste(m, "msg", sep="-")]] <- compute_stats(m_filtered$totalMessageSize)
      metadata_size_stats[[paste(m,"msg", "norm1",sep="-")]] <- compute_stats(m_filtered$normalizedTotalMessageSizeByBatchSizeFinestGrained)
      metadata_size_stats[[paste(m,"msg", "norm2",sep="-")]] <- compute_stats(m_filtered$normalizedTotalMessageSizeByBatchSizeFinerGrained)
      metadata_size_stats[[paste(m,"msg", "norm3",sep="-")]] <- compute_stats(m_filtered$normalizedTotalMessageSizeByBatchSizeCoarseGrained)
      metadata_size_stats[[paste(m, "meta", sep="-")]] <- compute_stats(m_filtered$explicitGlobalMetadata)
      metadata_size_stats[[paste(m, "meta", "norm1",sep="-")]] <- compute_stats(m_filtered$normalizedExplicitGlobalMetadataByBatchSizeFinestGrained)
      metadata_size_stats[[paste(m, "meta", "norm2",sep="-")]] <- compute_stats(m_filtered$normalizedExplicitGlobalMetadataByBatchSizeFinerGrained)
      metadata_size_stats[[paste(m, "meta", "norm3",sep="-")]] <- compute_stats(m_filtered$normalizedExplicitGlobalMetadataByBatchSizeCoarseGrained)
    }
    for (eachTable in unique(dtablesize_filtered$tableName)) {
      tablestats <- subset(dtablesize_filtered, dtablesize_filtered$tableName == eachTable)
      metadata_size_stats[[paste(eachTable, "table", sep="-")]] <- compute_stats(tablestats$tableSize)
    }
    metadata_size_stats$idempotenceGuard <- compute_stats(dguardsize$idempotenceGuardSize)
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

process_experiment_run <- function(path, spectrogram=TRUE, summarized=TRUE) {
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

