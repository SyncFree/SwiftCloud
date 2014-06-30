#Import and install required packages
require("ggplot2");
require("reshape2");

path <- "~/git/data_eval/ycsb";
file <- paste(path,"test_ycsb.csv",sep="/")

d <- read.table(file,comment.char = "#", fill = TRUE, sep = ",");

preprocess_OP <- function (data){
  #Data format is:
  #timestamp_ms,APP_OP,session_id,operation_name,duration_ms

  #Filter for batch updates
  data <- subset(data,data$V2=="APP_OP")
  names(data) <- c("timestamp","type","sessionId","operation","duration")
  return (data)
}


d <- preprocess_OP(d)
summary(d)

p <- qplot(duration, data = d,binwidth=5,color=sessionId,geom="freqpoly") + facet_wrap(~operation)
p
ggsave(p, file="~/git/data_eval/ycsb/test.pdf", scale=1)

