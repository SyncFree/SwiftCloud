path <- "/Users/annettebieniusa/Documents/workspace/SwiftCloud/results/social-responsiveness-config-RR-";
fname <- "result-social-";
f <- paste(path,fname,sep="");

outname <- paste(f,"stats.txt",sep="");
out <- file(outname,"a+");
#output header, will be sorted by alphabet
head <- c("TOTAL","TOTOAL_dev","FRIEND","LOGIN","LOGOUT","POST","READ","SEE_FRIEND","STATUS","type","client","mean_aggr");
cat(head,file=out,sep=" ");
flush(out);
cat("\n",file=out);

#CHECK if those are consistent with order in client and iteration!!!
clients <- c("\"ASIA1","\"ASIA2","\"US1","\"US2");
settings <- c("RR-SMR\"","RR-SMR Async\"","RR-C\"","RR-C Async\"");		
settings_f <- c("1/result-social-REPEATABLE_READS-STRICTLY_MOST_RECENT-false-120000-false-1000","2/result-social-REPEATABLE_READS-STRICTLY_MOST_RECENT-false-120000-true-1000","3/result-social-REPEATABLE_READS-CACHED-true-120000-false-1000","4/result-social-REPEATABLE_READS-CACHED-true-120000-true-1000");
client_f <-c(".ec2-122-248-226-204.ap-southeast-1.compute.amazonaws.com",".ec2-46-137-229-245.ap-southeast-1.compute.amazonaws.com",".ec2-184-169-233-51.us-west-1.compute.amazonaws.com",".ec2-50-18-133-148.us-west-1.compute.amazonaws.com");
for (client in c(1,3)) { 

for (i in c(1,2,3,4)) {

	
	infile <- paste(path,settings_f[i],".log",client_f[client],sep="");
	data <- read.table(infile,header = TRUE, sep = ",");
    
	#sort1.data <- data[order(command,command_exec_time), ];
	#table(sort1.data$command);
	
	
	a <- aggregate(data$command_exec_time, list(OPERATION=data$command), median);
	#FIXME it is actually necessary to test that each commands are in the frame!
	mean_FRIEND <- a[1,2];
	mean_LOGIN <- a[2,2];
	mean_LOGOUT <- a[3,2]
	mean_POST <- a[4,2];
	mean_READ <- a[5,2];
	mean_SEEFRIEND <- a[6,2];
	mean_STATUS <- a[7,2];
	mean_TOTAL  <- a[8,2];

	b <- aggregate(data$command_exec_time, list(OPERATION=data$command), sd);
    dev_TOTAL <- b[7,2]
    
    #Sum of all operations leaving out the TOTAL and LOGIN
    data_filter <- subset(data,data$command!="TOTAL" & data$command!="LOGIN");
    sum_per_session <-aggregate(data_filter$command_exec_time, list(ID=data_filter$session_id), sum);
	mean_aggr<-median(sum_per_session$x);
	dev_aggr <-sd(sum_per_session$x);

	line <- c(mean_TOTAL,dev_TOTAL,mean_FRIEND,mean_LOGIN,mean_LOGOUT,mean_POST,mean_READ,mean_SEEFRIEND,mean_STATUS,clients[client],settings[i],mean_aggr);
	cat(line,file=out,sep=" ");
	flush(out);
	cat("\n",file=out);
	}

}
close(out);