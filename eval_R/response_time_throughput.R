require("ggplot2")
require("reshape2")

if (length(commandArgs(TRUE)) > 0) {
  path <- commandArgs(TRUE)[1]
} else {
  #path <- "/home/zawir/code/swiftcloud/results/XXX/processed/"
  stop("syntax: response_time_throughput.R <directory with *-operations_stats.csv files>")
}

if (!file.info(path)$isdir) {
  stop(paste(path, "is not a directory"))
}

format_ext <- ".png"

file_list <- list.files(path, pattern="*operations_stats.csv",recursive=FALSE, full.names=TRUE)
if (length(file_list) == 0) {
  stop(paste("no input files found in", dir))
}

d <- data.frame()
for (file in file_list) {
  d <- rbind(d, read.table(file,sep = ",",row.names=NULL, header=TRUE))
}
# TODO: extract information from runid (ad-hoc?) and split data into
# (1) different figures according to the workload
# (2) different lines according to the configuration

#d$workload <- regmatches(d$run_id, regexpr(d$run_id, pattern="workload[ab](-uniform)?"))
#d$mode <- regmatches(d$run_id, regexpr(d$run_id, pattern="(notifications-frequent)|(notifications-infrequent)|(no-caching)|(refresh-frequent)|(refresh-infrequent)"))
#d$rest <- sub(d$run_id, pattern=paste(".+", d$mode, "-", sep=""), replacement="")

# TODO: use whisker box, and import qunatiles rather than quartiles
# TODO: format
p <- ggplot(d, aes(y=response_time_median, x=throughput_median))
p <- p + geom_line() + geom_errorbar(aes(ymax = response_time_quant75, ymin=response_time_quantt25), width=0.2) # + facet_grid(. ~ Type)
p <- p + theme(axis.line = element_line(color = 'black'))
p <- p + theme_bw() + theme(plot.background = element_blank(),panel.border = element_blank())
p <- p + labs(x= "throughput [txn/s]",y = "operation response time [ms]")
p <- p + theme(legend.position="none")

ggsave(p, file=paste(paste(file.path(path, "response_time_throughput"), format_ext, sep="")), scale=1)
