path <- "/Users/annettebieniusa/Documents/workspace/SwiftCloud/";
fname <- "result-runping-";
f <- paste(path,fname,sep="");

for (type in c("CACHED")) {
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