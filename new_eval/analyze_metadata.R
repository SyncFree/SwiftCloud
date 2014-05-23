require("ggplot2");
require("reshape2");

preprocess <- function (data){
  
  #Filter for batch updates
  data <- subset(data,data$V3=="METADATA_BatchUpdatesNotification")
  #Throw away first 30 sec and take duration of the next 70 secs????
  data <- transform(data,V2=as.numeric(as.character(V2)),V7=as.numeric(as.character(V7)),V8=as.numeric(as.character(V8)))
  data <- subset(data,data$V2 - data[1,2] > 30000)
  data <- subset(data,data$V2 - data[1,2] < 100000)
  
  #Drop unused columns
  data <- subset(data,select=-c(V1,V2,V3,V4,V5,V6,V9,V10));
  
  data <- droplevels(data);
#  data <- transform(data,V2=as.numeric(as.character(V2)),V4=as.numeric(as.character(V4)),V5=as.numeric(as.character(V5)),V6=as.numeric(as.character(V6)),V7=as.numeric(as.character(V7)),V8=as.numeric(as.character(V8)),V9=as.numeric(as.character(V9)),V10=as.numeric(as.character(V10)));
  data <- subset(data,data$V8 > 0)
  
  
  return (data)
}

collect_stats <- function(path,type) {
  file_list <- list.files(path, pattern="*scout-stdout.log",recursive=TRUE)
  file_list <- (paste(path,file_list,sep="/"))
  
  #data frame to store numbers for plot
  nthreads <- c(2,4,6)
  df <- matrix(ncol=6, nrow=length(nthreads))
  iter <- 0
  for(i in nthreads){
    iter <-iter +1
    j <- grep(paste("TH-",i,sep=""),file_list)
    
    #Sort by number of threads
    for (file in file_list[j]){
      # if the merged dataset doesn't exist, create it
      if (!exists("d")){
        d <- read.table(file,comment.char = ";", fill = TRUE, sep = ",",row.names=NULL);
        d <- preprocess(d)
      }
      # if the merged dataset does exist, append to it
      if (exists("d")){
        temp_dataset <- read.table(file,comment.char = ";", fill = TRUE, sep = ",",row.names=NULL);
        temp_dataset <- preprocess(temp_dataset);
        d <- rbind(d, temp_dataset);
        rm(temp_dataset)
      } 
    }
    
    #Rename columns
#    names(d)[1] <- "SessionID"
#    names(d)[2] <- "Time"
#    names(d)[4] <- "TotalMsgSize"
#    names(d)[5] <- "Vs/UpSize"
#    names(d)[6] <- "ValueSize"
#    names(d)[7] <- "ExplMetaSize"
#    names(d)[8] <- "BatchSize"
#    names(d)[9] <- "MaxVVSize"
#    names(d)[10] <- "MaxVVExcepSize"

    names(d)[1] <- "ExplMetaSize"
    names(d)[2] <- "BatchSize"


    d <- cbind(d,"MsgSizeNormalized" = d$ExplMetaSize / d$BatchSize)
    df[iter,] <- c(type,i,min(d$MsgSizeNormalized),max(d$MsgSizeNormalized),mean(d$MsgSizeNormalized),median(d$MsgSizeNormalized))

    rm(d)
  
  }
  df <- data.frame(df)
  names(df)[1] <- "Type"
  names(df)[2] <- "Threads"
  names(df)[3] <- "Min"
  names(df)[4] <- "Max"
  names(df)[5] <- "Mean"
  names(df)[6] <- "Median"
  
  return (df)
}

path_p <- "~/git/data_eval/swiftsocial/metadata-practi";
path_s <- "~/git/data_eval/swiftsocial/metadata-swiftcloud";

stat_swift <- collect_stats(path_s,"swift")
stat_practi <- collect_stats(path_p,"practi")

stat <- rbind(stat_swift,stat_practi)
stat <- transform(stat,Min=as.numeric(as.character(Min)),Max=as.numeric(as.character(Max)),Mean=as.numeric(as.character(Mean)),Median=as.numeric(as.character(Median)));

p <- ggplot(stat, aes(colour=Type, y=Median, x=Threads))
p <- p + geom_point() + geom_errorbar(aes(ymax = Max, ymin=Min), width=0.2) + facet_grid(. ~ Type)