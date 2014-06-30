#Import and install required packages
require("ggplot2");
require("reshape2");
require("gridExtra");

preprocess_OP <- function (data){
  #Data format is:
  #timestamp_ms,APP_OP,session_id,operation_name,duration_ms
  
  #Filter for batch updates
  data <- subset(data,data$V2=="APP_OP")
  return (data)
}


#path <- "~/data_eval/50reads-50updates/Jun29-1404068293230-c09289b5-DC-1-SU-1-SC-8-TH-4-records-100000-operations-500000/ec2-54-72-34-188.eu-west-1.compute.amazonaws.com/";
#file <- paste(path,"test_ycsb.csv",sep="/")


toplevel_path <- "~/data_eval/50reads-50updates";

#file <- "~/data_eval/50reads-50updates/Jun29-1404068293230-c09289b5-DC-1-SU-1-SC-8-TH-4-records-100000-operations-500000/ec2-54-72-34-188.eu-west-1.compute.amazonaws.com/scout-stdout.log"

file_list <- list.files(toplevel_path, pattern="*scout-stdout.log",recursive=TRUE)
file_list <- (paste(toplevel_path,file_list,sep="/"))

p <- list()
d <- data.frame()
i <- 1
j <- grep(paste("TH-",i,sep=""),file_list)

#Sort by number of threads
for (file in file_list[j]){
    temp_dataset <- read.table(file,comment.char = "#", fill = TRUE, sep = ",");
    temp_dataset <- preprocess_OP(temp_dataset);
    d <- rbind(d, temp_dataset);
    rm(temp_dataset)
}
#Set names for columns
names(d) <- c("timestamp","type","sessionId","operation","duration")


summary(d)
#Filter out the rows where duration couldn't be parsed due to 
d <- subset(d,!is.na(d$duration))
summary(d)


#!Making scatter plots takes quite long for our data with > 1.000.000 entries!
#Scatterplot for distribution over time
#scatter.plot <- ggplot(d, aes(timestamp,duration)) + + geom_point(aes(color=operation))
#ggsave(scatter.plot, file=paste("~/data_eval/fig/timeline-TH",i,".pdf",collapse=""), scale=1)

#CDF Plot
cdf.plot <- ggplot(d, aes(x=duration)) + stat_ecdf(aes(colour=operation)) + ggtitle (paste("TH",i))
cdf.plot
#Histogram
#p <- qplot(duration, data = d,binwidth=5,color=operation,geom="freqpoly") + facet_wrap( ~ sessionId)

rm(d)  


#p <- do.call(grid.arrange,p)

ggsave(cdf.plot, file=paste("~/data_eval/fig/cdf-TH",i,".pdf",collapse=""), scale=1)
