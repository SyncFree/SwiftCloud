path <- "/Users/annettebieniusa/Documents/workspace/SwiftCloud/results/";
fname <- "result-ping-";
f <- paste(path,fname,sep="");

for (type in c("REPEATABLE_READS-CACHED-true")) {
	outname <- paste(f,"stats.txt",sep="");
	out <- file(outname,"a+");

	infile <- paste(f,type,".log",sep="");
	data <- read.table(infile);
	line <- c(type,median(data$V1),mean(data$V1),sd(data$V1),median(data$V1)-sd(data$V1),median(data$V1)+sd(data$V1));
	cat(line,file=out,sep=" ");
	flush(out);
	cat("\n",file=out);
		
	close(out);
}