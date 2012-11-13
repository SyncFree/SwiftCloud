path <- "/Users/annettebieniusa/Documents/workspace/SwiftCloud/results/";
fname <- "result-social-";
f <- paste(path,fname,sep="");

outname <- paste(f,"cummulative.txt",sep="");
out <- file(outname,"a+");
#output header, will be sorted by alphabet
intervals <- c(-1,50,100,150,200,250,300,350,400,450,500,550,600,650);
head <- c("interval","STATUS","POST","FRIEND","READ","SEE_FRIENDS");
cat(head,file=out,sep=" ");
flush(out);
cat("\n",file=out);

files <- c("/Users/annettebieniusa/Documents/workspace/SwiftCloud/results/social-responsiveness-config-RR-1/result-social-REPEATABLE_READS-STRICTLY_MOST_RECENT-false-120000-false-1000.log.ec2-122-248-226-204.ap-southeast-1.compute.amazonaws.com");
#,"/Users/annettebieniusa/Documents/workspace/SwiftCloud/results/social-responsiveness-config-RR-4/result-social-REPEATABLE_READS-CACHED-true-120000-true-1000.log.ec2-122-248-226-204.ap-southeast-1.compute.amazonaws.com");

for (set in files) {
data <- read.table(set,header = TRUE, sep = ",");
session <- subset(data,data$session_id==5);

int_name <- c("[0,50]",  "(50,100]", "(100,150]", "(150,200]", "(200,250]", "(250,300]", "(300,350]", "(350,400]", "(400,450]", "(450,500]", "(500,550]", "(550,600]", "(600,650]"); 

for(iv in c(1,2,3,4,5,6,7,8,9,10,11,12,13)) {

    p_status <- cbind(table(cut(subset(session,session$command == "STATUS")$command_exec_time, breaks=intervals)))[iv]/max(nrow(subset(session,session$command == "STATUS")),1);
    p_post <-cbind(table(cut(subset(session,session$command == "POST")$command_exec_time, breaks=intervals)))[iv]/max(nrow(subset(session,session$command == "POST")),1);

    p_friend <-cbind(table(cut(subset(session,session$command == "FRIEND")$command_exec_time, breaks=intervals)))[iv]/max(nrow(subset(session,session$command == "FRIEND")),1);

    p_read <-cbind(table(cut(subset(session,session$command == "READ")$command_exec_time, breaks=intervals)))[iv]/max(nrow(subset(session,session$command == "READ")),1);

    p_see <-cbind(table(cut(subset(session,session$command == "SEE_FRIENDS")$command_exec_time, breaks=intervals)))[iv]/max(nrow(subset(session,session$command == "SEE_FRIENDS")),1);

    
    line <- c(int_name[iv],p_status,p_post,p_friend,p_read,p_see);
	cat(line,file=out,sep=" ");
	flush(out);
	cat("\n",file=out);

}
}
close(out);
