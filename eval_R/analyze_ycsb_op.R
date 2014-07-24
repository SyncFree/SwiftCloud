

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

rmdir <- function(dir) {
  unlink(dir, recursive=TRUE)
}

mkdir <- function(dir) {
  dir.create(dir, recursive=TRUE)
}

untargz <- function(archive,output_dir) {
  untar(archive, exdir=output_dir, compressed="gzip")
}


#toplevel_path <- "~/data_eval/50reads-50updates";
#file <- paste(path,"test_ycsb.csv",sep="/")

# Directory that contains a set of .targ.gz archives, one for each run.
toplevel_path <- "/home/zawir/code/swiftcloud/results/ycsb/workloadb-uniform-var-threads-cap/"
tmp_dir <- paste(toplevel_path, "tmp", sep="/")

tar_list <- list.files(toplevel_path, pattern="*.tar.gz",recursive=TRUE)
tar_list <- (paste(toplevel_path,tar_list,sep="/"))

rmdir(tmp_dir)
mkdir(tmp_dir)

# The following variables and for loops are ad-hoc per experiment family
# TODO: extract functions to process logs and produce aggregated information, graphs etc.
modes <- c("no-caching", "notifications", "refresh-frequent", "refresh-infrequent")
caps <- c(6000,8000,10000)
threads <- c(4,12,20,28,36,44,52,60)
throughput.stats <- data.frame(mode=character(),cap=integer(),TH=integer(),mean=double(),median=double(),min=double(),max=double())

for (mode in modes) {
  for (cap in caps) {
    for(th in threads) {
      base_pattern <- paste(mode,"cap",cap,"TH",th,sep="-")
      pattern <- paste(base_pattern,".tar.gz", sep="")
      t <- grep(pattern, tar_list)
      if (length(t) <= 0) {
        warning(paste("could not find archive matching pattern:", pattern))
        next
      }
      if (length(t) > 1) {
        warning("ignoring multiple archives matching a single configuration:")
        for (duplicate in tar_list[t]) {
          warning(duplicate)
        }
        next
      }
      tar <- tar_list[t]
      print(paste("Processing archive", tar))
      tmp_subdir <- paste(tmp_dir, base_pattern, sep="/")
      rmdir(tmp_subdir)
      mkdir(tmp_subdir)
      untargz(tar, tmp_subdir)
      
      file_list <- list.files(tmp_subdir, pattern="*scout-stdout.log",recursive=TRUE)
      file_list <- (paste(tmp_subdir,file_list,sep="/"))
    
      d <- data.frame(timestamp=numeric(),type=character(),sessionId=character(),operation=character(),duration=numeric())
      
      #Sort by number of threads
      for (file in file_list){
        temp_dataset <- read.table(file,comment.char = "#", fill = TRUE, sep = ",", stringsAsFactors=FALSE);
        temp_dataset <- preprocess_OP(temp_dataset);
        d <- rbind(d, temp_dataset)
        rm(temp_dataset)
      }
      print(paste("Loaded", length(file_list), "files"))

      #Set names for columns
      names(d) <- c("timestamp","type","sessionId","operation","duration")
      
      #helper <- d
      #summary(d)
      
      #Filter out the rows where duration couldn't be parsed due to 
      d <- subset(d,!is.na(d$duration))
      
      # Common tasks for YCSB and SwiftSocial experiments:
      # TODO: aggregated throughput computation
      
      start_time <- min(d$timestamp)
      prune_start <- 150000
      duration_run <- 150000
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
      ggsave(cdf.plot, file=paste(toplevel_path, "cdf-",mode, "-cap-",cap,"-TH-",th,format_ext,collapse="", sep=""), scale=1)
      
      #Histogram
      #p <- qplot(duration, data = d,binwidth=5,color=operation,geom="freqpoly") + facet_wrap( ~ sessionId)
      
      #Throughput plot
      #Careful: It seems that the first and last bin only cover 5000 ms
      throughput.plot <- ggplot(d, aes(x=timestamp)) + geom_histogram(binwidth=1000) 
      throughput.plot
      ggsave(throughput.plot, file=paste(toplevel_path, "throughput-",mode, "-cap-",cap,"-TH-",th,format_ext,collapse="", sep=""), scale=1)
      
      steps <- c(seq(min(d$timestamp),max(d$timestamp),by=1000), max(d$timestamp)) 
      through <- hist(d$timestamp, breaks=steps)
      summary(through$counts)
      newd <- data.frame(mode=mode,cap=cap,TH=th,mean=mean(through$counts),min=min(through$counts),median=median(through$counts),max=max(through$counts))
      rbind(throughput.stats,newd)
      rm(d)
      rmdir(tmp_subdir)
    }
  }
}

throughput.stats

rmdir(tmp_dir)