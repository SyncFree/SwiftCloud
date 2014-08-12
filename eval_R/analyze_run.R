#Import and install required packages
require("ggplot2");
require("reshape2");
require("gridExtra");

# That should include at least one pruning point (60s)
PRUNE_START_MS <- 350000
DURATION_RUN_MS <- 100000
FORMAT_EXT <- ".png"

process_experiment_run_dir <- function(dir, output_prefix, spectrogram=TRUE,summarized=TRUE) {
  # Scout logs
  file_list <- list.files(dir, pattern="*scout-stdout.log",recursive=TRUE,full.names=TRUE)
  
  if (length(file_list) == 0) {
    warning(paste("No scout logs found in", dir))
  }

  clientdata <- data.frame()
  for (file in file_list){
    temp_dataset <- read.table(file,comment.char = "#", fill = TRUE, sep = ",", stringsAsFactors=FALSE);
    clientdata <- rbind(clientdata, temp_dataset)
    rm(temp_dataset)
  }
  print(paste("Loaded", length(file_list), "scout log files"))
  #summary(data)

  #Data format is:
  #timestamp_ms,...
  # compute relative timestamps
  min_exp_timestamp = min(clientdata$V1)
  clientdata <- transform(clientdata, V1=(clientdata$V1 - min_exp_timestamp))

  dop <- subset(clientdata,clientdata$V2=="APP_OP")
  dop <- dop[, c("V1", "V2", "V3", "V4", "V5")]
  # since d contains variety of different entries, this casting is needed
  dop <- transform(dop, V5 = as.numeric(V5))
  names(dop) <- c("timestamp","type","sessionId","operation","duration")

  derr <- subset(clientdata,clientdata$V2=="APP_OP_FAILURE")
  derr <- derr[, c("V1", "V2", "V3", "V4", "V5")]
  names(derr) <- c("timestamp","type","sessionId","operation","cause")
  
  
  dmetadata <- subset(clientdata,clientdata$V2=="METADATA")
  dmetadata <- dmetadata[, c("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13")]
  dmetadata <- transform(dmetadata, V5=as.numeric(V5), V6=as.numeric(V6), V7=as.numeric(V7),
                    V8=as.numeric(V8), V9=as.numeric(V9),
                    V10=as.numeric(V10), V11=as.numeric(V11), V12=as.numeric(V12), V13=as.numeric(V13))
  dmetadata$V9 <- sapply(dmetadata$V9, function(x) { return (max(x, 1)) })
  names(dmetadata) <- c("timestamp","type","sessionId","message","totalMessageSize",
                   "versionOrUpdateSize","valueSize","explicitGlobalMetadata","batchSizeFinestGrained",
                   "batchSizeFinerGrained", "batchSizeCoarseGrained", "maxVVSize","maxVVExceptionsNum")
  dmetadata$normalizedTotalMessageSizeByBatchSizeFinestGrained <- dmetadata$totalMessageSize / dmetadata$batchSizeFinestGrained
  dmetadata$normalizedTotalMessageSizeByBatchSizeFinerGrained <- dmetadata$totalMessageSize / dmetadata$batchSizeFinerGrained
  dmetadata$normalizedTotalMessageSizeByBatchSizeCoarseGrained <- dmetadata$totalMessageSize / dmetadata$batchSizeCoarseGrained
  dmetadata$normalizedExplicitGlobalMetadataByBatchSizeFinestGrained <- dmetadata$explicitGlobalMetadata / dmetadata$batchSizeFinestGrained
  dmetadata$normalizedExplicitGlobalMetadataByBatchSizeFinerGrained <- dmetadata$explicitGlobalMetadata / dmetadata$batchSizeFinerGrained
  dmetadata$normalizedExplicitGlobalMetadataByBatchSizeCoarseGrained <- dmetadata$explicitGlobalMetadata / dmetadata$batchSizeCoarseGrained

  dop_filtered <- subset(dop, dop$timestamp > PRUNE_START_MS & dop$timestamp - PRUNE_START_MS < DURATION_RUN_MS)
  # derr_filtered <- 
  dmetadata_filtered <- subset(dmetadata, dmetadata$timestamp > PRUNE_START_MS & dmetadata$timestamp - PRUNE_START_MS < DURATION_RUN_MS)
  rm(clientdata)
  
  # DC logs
  dc_file_list <- list.files(dir, pattern="*sur-stdout.log",recursive=TRUE,full.names=TRUE)
  
  if (length(dc_file_list) == 0) {
    warning(paste("No DC logs found in", dir))
  }
  
  dcdata <- data.frame()
  for (file in dc_file_list){
    temp_dataset <- read.table(file,comment.char = "#", fill = TRUE, sep = ",", stringsAsFactors=FALSE);
    dcdata <- rbind(dcdata, temp_dataset)
    rm(temp_dataset)
  }
  print(paste("Loaded", length(dc_file_list), "DC log files"))
  dcdata <- transform(dcdata, V1=(dcdata$V1 - min_exp_timestamp))
  
  dtablesize <- subset(dcdata,dcdata$V2=="DATABASE_TABLE_SIZE")
  dtablesize <- dtablesize[, c("V1", "V2", "V3", "V4", "V5")]
  dtablesize <- transform(dtablesize, V5=as.numeric(V5))
  names(dtablesize) <- c("timestamp","type","nodeId","tableName","tableSize")
  # remove dummy table from the output
  dtablesize <- subset(dtablesize, dtablesize$tableName != "e")
  
  max_timestamp <- max(dcdata$V1)
  dguardsize <- subset(dcdata,dcdata$V2=="IDEMPOTENCE_GUARD_SIZE")
  dguardsize <- transform(dguardsize, V4=as.numeric(V4))
  dguardsize <- dguardsize[, c("V1", "V2", "V3", "V4")]
  names(dguardsize) <- c("timestamp","type","nodeId","idempotenceGuardSize")
  for (dc in unique(dguardsize$nodeId)) {
    dc_last_guard <- tail(subset(dguardsize, dguardsize$nodeId == dc), 1)
    MIN_SAMPLING_PERIOD <- 1000
    if (dc_last_guard$timestamp + MIN_SAMPLING_PERIOD < max_timestamp) {
      missing_timestamps <- seq(dc_last_guard$timestamp + MIN_SAMPLING_PERIOD, max_timestamp, by=MIN_SAMPLING_PERIOD)
      missing_entries <- data.frame(timestamp=missing_timestamps,
                                       type=as.character(rep("IDEMPOTENCE_GUARD_SIZE", length(missing_timestamps))),
                                       nodeId=as.character(rep(dc, length(missing_timestamps))),
                                       idempotenceGuardSize=rep(dc_last_guard$idempotenceGuardSize, length(missing_timestamps)))
      dguardsize <- rbind(dguardsize, missing_entries)
    }
  }

  dtablesize_filtered <- subset(dtablesize, dtablesize$timestamp > PRUNE_START_MS & dtablesize$timestamp - PRUNE_START_MS < DURATION_RUN_MS)
  dguardsize_filtered <- subset(dguardsize, dguardsize$timestamp > PRUNE_START_MS & dguardsize$timestamp - PRUNE_START_MS < DURATION_RUN_MS)
  
  rm(dcdata)

  # Create destination directory
  dir.create(dirname(output_prefix), recursive=TRUE, showWarnings=FALSE)
  
  # "SPECTROGRAM" MODE OUPUT
  if (spectrogram) {
    # Response time scatterplot over time  
    scatter.plot <- ggplot(dop, aes(timestamp,duration)) + geom_point(aes(color=operation))
    ggsave(scatter.plot, file=paste(output_prefix, "-response_time",FORMAT_EXT,collapse="", sep=""), scale=1)
    
    # Throughput over time plot
    throughput.plot <- ggplot(dop, aes(x=timestamp)) + geom_histogram(binwidth=1000) 
    #throughput.plot
    ggsave(throughput.plot, file=paste(output_prefix, "-throughput_full",FORMAT_EXT,collapse="", sep=""), scale=1)
  
    #Histogram
    #p <- qplot(duration, data = d,binwidth=5,color=operation,geom="freqpoly") + facet_wrap( ~ sessionId)
    
    # Message occurences over time plot(s)
    for (m in unique(dmetadata$message)) {
      m_metadata <- subset(dmetadata, dmetadata$message==m) 
      msg.plot <- ggplot(m_metadata, aes(x=timestamp)) + geom_histogram(binwidth=1000) 
      # msg.plot
      ggsave(msg.plot, file=paste(output_prefix, "-msg_occur-", m, FORMAT_EXT,collapse="", sep=""), scale=1)
    }
    # Message size and metadata size over time scatter plots
    msg_size.plot <- ggplot(dmetadata, aes(timestamp,totalMessageSize)) + geom_point(aes(color=message))
    ggsave(msg_size.plot, file=paste(output_prefix, "-msg_size",FORMAT_EXT,collapse="", sep=""), scale=1)
  
    metadata_size.plot <- ggplot(dmetadata, aes(timestamp,explicitGlobalMetadata)) + geom_point(aes(color=message))
    ggsave(metadata_size.plot, file=paste(output_prefix, "-msg_meta",FORMAT_EXT,collapse="", sep=""), scale=1)
  
    metadata_norm1_size.plot <- ggplot(dmetadata, aes(timestamp, normalizedExplicitGlobalMetadataByBatchSizeFinestGrained)) + geom_point(aes(color=message))
    ggsave(metadata_norm1_size.plot, file=paste(output_prefix, "-msg_meta_norm1",FORMAT_EXT,collapse="", sep=""), scale=1)

    metadata_norm2_size.plot <- ggplot(dmetadata, aes(timestamp, normalizedExplicitGlobalMetadataByBatchSizeFinerGrained)) + geom_point(aes(color=message))
    ggsave(metadata_norm2_size.plot, file=paste(output_prefix, "-msg_meta_norm2",FORMAT_EXT,collapse="", sep=""), scale=1)
    
    metadata_norm3_size.plot <- ggplot(dmetadata, aes(timestamp, normalizedExplicitGlobalMetadataByBatchSizeCoarseGrained)) + geom_point(aes(color=message))
    ggsave(metadata_norm3_size.plot, file=paste(output_prefix, "-msg_meta_norm3",FORMAT_EXT,collapse="", sep=""), scale=1)
    
    # Storage metadata/db size, plots over time
    db_table_size.plot <- ggplot()
    for (eachDc in unique(dtablesize$nodeId)) {
      dctablesize <- subset(dtablesize, dtablesize$nodeId == eachDc)
      dctablesize$dcTable <- paste("DC", dctablesize$nodeId, ", table", dctablesize$tableName)
      for (tab in unique(dctablesize$tableName)) {
        tabdctablesize <- subset(dctablesize, dctablesize$tableName == tab)
        db_table_size.plot <- db_table_size.plot + geom_line(data=tabdctablesize, mapping=aes(timestamp,tableSize, color=dcTable))
      }
    }

    guard_size.plot <- ggplot()
    for (eachDc in unique(dguardsize$nodeId)) {
      dcguardsize <- subset(dguardsize, dguardsize$nodeId == eachDc)
      guard_size.plot <- guard_size.plot + geom_line(data=dcguardsize, mapping=aes(timestamp,idempotenceGuardSize,color=nodeId))
    }

    ggsave(db_table_size.plot, file=paste(output_prefix, "-table_size",FORMAT_EXT,collapse="", sep=""), scale=1)
    ggsave(guard_size.plot, file=paste(output_prefix, "-guard_size",FORMAT_EXT,collapse="", sep=""), scale=1)
  }
  rm(dop)
  rm(dmetadata)
  gc()
  
  # "SUMMARIZED" MODE OUTPUT
  if (summarized) {
    # Throughput over time plot
    # Careful: It seems that the first and last bin only cover 5000 ms
    throughput.plot <- ggplot(dop_filtered, aes(x=timestamp)) + geom_histogram(binwidth=1000) 
    #throughput.plot
    ggsave(throughput.plot, file=paste(output_prefix, "-throughput",FORMAT_EXT,collapse="", sep=""), scale=1)

    # Operation duration CDF plot for the filtered period
    cdf.plot <- ggplot(dop_filtered, aes(x=duration)) + stat_ecdf(aes(colour=operation)) # + ggtitle (paste("TH",th))
    # cdf.plot
    ggsave(cdf.plot, file=paste(output_prefix, "-cdf",FORMAT_EXT,collapse="", sep=""), scale=1)

    # common output format for descriptive statistics
    quantile_steps <- seq(from=0.0, to=1.0, by=0.001)
    stats <- c("mean", "stddev", rep("permille", length(quantile_steps)))
    stats_params <- c(0, 0, quantile_steps)
    compute_stats <- function(vec) {
      return (c(mean(vec), sd(vec), quantile(vec, probs=quantile_steps)))
    }
    # A more efficient in place version.
    compute_stats_dmetadata_filtered <- function(col) {
      return (c(mean(dmetadata_filtered[[col]]), sd(dmetadata_filtered[[col]]), quantile(dmetadata_filtered[[col]], probs=quantile_steps)))
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
      metadata_size_stats[[paste(m, "msg", sep="-")]] <- compute_stats_dmetadata_filtered("totalMessageSize")
      metadata_size_stats[[paste(m,"msg", "norm1",sep="-")]] <- compute_stats_dmetadata_filtered("normalizedTotalMessageSizeByBatchSizeFinestGrained")
      metadata_size_stats[[paste(m,"msg", "norm2",sep="-")]] <- compute_stats_dmetadata_filtered("normalizedTotalMessageSizeByBatchSizeFinerGrained")
      metadata_size_stats[[paste(m,"msg", "norm3",sep="-")]] <- compute_stats_dmetadata_filtered("normalizedTotalMessageSizeByBatchSizeCoarseGrained")
      metadata_size_stats[[paste(m, "meta", sep="-")]] <- compute_stats_dmetadata_filtered("explicitGlobalMetadata")
      metadata_size_stats[[paste(m, "meta", "norm1",sep="-")]] <- compute_stats_dmetadata_filtered("normalizedExplicitGlobalMetadataByBatchSizeFinestGrained")
      metadata_size_stats[[paste(m, "meta", "norm2",sep="-")]] <- compute_stats_dmetadata_filtered("normalizedExplicitGlobalMetadataByBatchSizeFinerGrained")
      metadata_size_stats[[paste(m, "meta", "norm3",sep="-")]] <- compute_stats_dmetadata_filtered("normalizedExplicitGlobalMetadataByBatchSizeCoarseGrained")
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

  rm(derr)
  rm(dop_filtered)
  # rm(derr_filtered)
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

