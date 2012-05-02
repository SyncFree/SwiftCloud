path <- "/Users/annettebieniusa/Documents/workspace/SwiftCloud/results/";
fname <- "result-social-";
f <- paste(path,fname,sep="");

outname <- paste(f,"stats.txt",sep="");
out <- file(outname,"a+");
#output header, will be sorted by alphabet
head <- c("session_mean","stddev","FRIEND","LOGIN","POST","READ","SEE_FRIEND","STATUS","type","client");
cat(head,file=out,sep=" ");
flush(out);
cat("\n",file=out);
		

for (client in  c(".ec2-176-34-221-41.eu-west-1.compute.amazonaws.com",".ec2-184-72-10-67.us-west-1.compute.amazonaws.com")) { 

for (type in c("REPEATABLE_READS-STRICTLY_MOST_RECENT-false-60000-false-1000")) {
	
	infile <- paste(f,type,".log",client,sep="");
	data <- read.table(infile,header = TRUE, sep = ",");
    
	sort1.data <- data[order(command,command_exec_time), ];
	table(sort1.data$command);
	
	
	mean_total<-mean(aggregate(data$command_exec_time, list(ID=data$session_id), sum)$x);
	mean_dev  <-sd(aggregate(data$command_exec_time, list(ID=data$session_id), sum)$x);
	
	a <- aggregate(data$command_exec_time, list(OPERATION=data$command), mean);
	#FIXME it is actually necessary to test that each commands are in the frame!
	mean_FRIEND <- a[1,2];
	mean_LOGIN <- a[2,2];
	mean_POST <- a[3,2];
	mean_READ <- a[4,2];
	mean_SEEFRIEND <- a[5,2];
	mean_STATUS <- a[6,2];

	line <- c(mean_total,mean_dev,mean_FRIEND,mean_LOGIN,mean_POST,mean_READ,mean_SEEFRIEND,mean_STATUS,type,client);
	cat(line,file=out,sep=" ");
	flush(out);
	cat("\n",file=out);
	}
}
close(out);