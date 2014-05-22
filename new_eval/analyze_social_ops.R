#First install ggplot2: import("ggplot2")
#Before rerunning the script, you need to clean the environment first!

library("ggplot2");

assembleFilename <- function(s){
  path <- "~/git/data_eval/swiftsocial/metadata-swiftcloud";
  dir <- "May20-1400619623414-16cc04bc-DC-3-SU-1-SC-15-TH-2-USERS-6000";
  fname <- "scout-stdout.log";
  return (paste(path,dir,s,fname,sep="/"))
}

preprocess <- function (data){
#Distribution of ops per session
  data <- subset(data,data$V2=="READ" | data$V2=="POST" | data$V2=="SEE_FRIENDS" | data$V2=="STATUS" | data$V2=="FRIEND");
#Drop unused columns
  data <- subset(data,select=-c(V5,V6,V7,V8,V9,V10));
  data <- droplevels(data);
  data <- transform(data,V3=as.numeric(as.character(V3)),V4=as.numeric(as.character(V4)));
#Throw away first 30 sec and take duration of the next 70 secs????
  data <- subset(data,data$V4 - data[1,4] > 30000)
  data <- subset(data,data$V4 - data[1,4] < 100000)
  return (data)
}

site <- c("ec2-54-187-223-234.us-west-2.compute.amazonaws.com",
          "ec2-54-187-230-35.us-west-2.compute.amazonaws.com",
          "ec2-54-187-230-38.us-west-2.compute.amazonaws.com",
          "ec2-54-200-29-51.us-west-2.compute.amazonaws.com",
          "ec2-54-200-29-62.us-west-2.compute.amazonaws.com",
          "ec2-54-72-217-106.eu-west-1.compute.amazonaws.com",
          "ec2-54-72-227-177.eu-west-1.compute.amazonaws.com",
          "ec2-54-72-52-235.eu-west-1.compute.amazonaws.com",
          "ec2-54-76-46-25.eu-west-1.compute.amazonaws.com",
          "ec2-54-76-46-49.eu-west-1.compute.amazonaws.com",
          "ec2-54-86-124-17.compute-1.amazonaws.com",
          "ec2-54-86-197-100.compute-1.amazonaws.com",
          "ec2-54-86-223-100.compute-1.amazonaws.com",
          "ec2-54-86-252-161.compute-1.amazonaws.com",
          "ec2-54-86-252-209.compute-1.amazonaws.com");
file_list <- assembleFilename(site);

for (file in file_list){
  # if the merged dataset doesn't exist, create it
  if (!exists("d")){
    d <- read.table(file,comment.char = ";", fill = TRUE, sep = ",");
    d <- preprocess(d)
  }
  # if the merged dataset does exist, append to it
  if (exists("d")){
    temp_dataset <- read.table(file,comment.char = ";", fill = TRUE, sep = ",");
    temp_dataset <- preprocess(temp_dataset);
    d <- rbind(d, temp_dataset);
    rm(temp_dataset)
  } 
}

#Rename columns
names(d)[1] <- "SessionID"
names(d)[2] <- "Operation"
names(d)[3] <- "RespTime"
names(d)[4] <- "Time"



p <- ggplot(d, aes(x=RespTime,fill=Operation)) + geom_bar() + facet_grid(. ~ SessionID)
ggsave(p, file="~/git/data_eval/swiftsocial/op_hist_TH_2.pdf", scale=4)
