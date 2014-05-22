library("ggplot2");

assembleFilename <- function(s){
  path <- "~/git/data_eval/swiftsocial/metadata-swiftcloud";
  dir <- "May20-1400619623414-16cc04bc-DC-3-SU-1-SC-15-TH-2-USERS-6000";
  fname <- "scout-stdout.log";
  return (paste(path,dir,s,fname,sep="/"))
}

assembleFilename_practi <- function(s){
  path <- "~/git/data_eval/swiftsocial/metadata-practi";
#  dir <- "May21-1400624015559-968d0e13-DC-3-SU-1-SC-15-TH-2-USERS-6000";
  dir <- "May21-1400630839658-968d0e13-DC-3-SU-1-SC-15-TH-12-USERS-36000";
  fname <- "scout-stdout.log";
  return (paste(path,dir,s,fname,sep="/"))
}


preprocess <- function (data){
  #Filter for batch updates
  data <- subset(data,data$V3=="METADATA_BatchUpdatesNotification")
  data <- droplevels(data);
  data <- transform(data,V2=as.numeric(as.character(V2)),V4=as.numeric(as.character(V4)),V5=as.numeric(as.character(V5)),V6=as.numeric(as.character(V6)),V7=as.numeric(as.character(V7)),V8=as.numeric(as.character(V8)),V9=as.numeric(as.character(V9)),V10=as.numeric(as.character(V10)));
  data <- subset(data,data$V8 > 0)
  
  #Throw away first 30 sec and take duration of the next 70 secs????
  data <- subset(data,data$V2 - data[1,2] > 30000)
  data <- subset(data,data$V2 - data[1,2] < 100000)
  
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
site_practi = c("ec2-54-186-191-161.us-west-2.compute.amazonaws.com",
                "ec2-54-187-128-209.us-west-2.compute.amazonaws.com",
                "ec2-54-187-159-246.us-west-2.compute.amazonaws.com",
                "ec2-54-187-176-81.us-west-2.compute.amazonaws.com",
                "ec2-54-187-202-88.us-west-2.compute.amazonaws.com",
                "ec2-54-208-146-127.compute-1.amazonaws.com",
                "ec2-54-208-159-108.compute-1.amazonaws.com",
                "ec2-54-208-180-105.compute-1.amazonaws.com",
                "ec2-54-76-12-95.eu-west-1.compute.amazonaws.com",
                "ec2-54-76-18-132.eu-west-1.compute.amazonaws.com",
                "ec2-54-76-24-160.eu-west-1.compute.amazonaws.com",
                "ec2-54-76-30-106.eu-west-1.compute.amazonaws.com",
                "ec2-54-76-34-20.eu-west-1.compute.amazonaws.com",
                "ec2-54-86-132-125.compute-1.amazonaws.com",
                "ec2-54-86-248-243.compute-1.amazonaws.com");
file_list <- assembleFilename_practi(site_practi);

#file_list <- assembleFilename(site);
rm(d)

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
names(d)[2] <- "Time"
names(d)[4] <- "TotalMsgSize"
names(d)[5] <- "Vs/UpSize"
names(d)[6] <- "ValueSize"
names(d)[7] <- "ExplMetaSize"
names(d)[8] <- "BatchSize"
names(d)[9] <- "MaxVVSize"
names(d)[10] <- "MaxVVExcepSize"

d <- cbind(d,"MsgSizeNormalized" = d$ExplMetaSize / d$BatchSize)
#p <- ggplot(d, aes(x=RespTime,fill=Operation)) + geom_bar() + facet_grid(. ~ SessionID)
#ggsave(p, file="~/git/data_eval/swiftsocial/op_hist_TH_2.pdf", scale=4)
#srm(d)