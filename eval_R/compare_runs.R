require("ggplot2")
require("reshape2")
require("stringr")
require("grid")

format_ext <- ".png"

# TODO: use factors for workloads & modes?
WORKLOAD_LEVELS = c("workloada-uniform", "workloada", "workloadb-uniform", "workloadb")
WORKLOAD_LABELS = c("workload A (uniform)", "workload A (zipf)", "workload B (uniform)", "workload B (zipf)")
MODE_LEVELS = c("notifications-frequent", "notifications-infrequent", "no-caching", "refresh-frequent", "refresh-infrequent")
MODE_LABELS = c("notifications (every 1s)", "notifications (every 10s)", "no caching", "cache refresh (every 1s)", "cache refresh (every 10s)")

decode_filename <- function(file, var_name, suffix) {
  pattern_alternatives <- function(patterns) {
    pattern <- paste("(", patterns[1], ")", sep="")
    for (s in patterns[2:length(patterns)]) {
      pattern <- paste(pattern, "|(", s, ")", sep="")
    }
    return (pattern)
  }
  REGEX <- paste("(",pattern_alternatives(WORKLOAD_LEVELS),")-mode-(", pattern_alternatives(MODE_LEVELS), ")-", var_name, "-([0-9.]+)-", suffix, sep="")
  WORKLOAD_IDX <- 2
  MODE_IDX <- WORKLOAD_IDX + length(WORKLOAD_LEVELS) + 1
  VAR_IDX <- MODE_IDX + length(MODE_LEVELS) + 1
  match <- str_match(file, REGEX)
  
  if (length(match)<= 1 || is.element(c(match[WORKLOAD_IDX], match[MODE_IDX], match[VAR_IDX]), NA)) {
      stop(paste("cannot match filename", str, "with the expected pattern", REGEX))
  }
  workload <- factor(match[WORKLOAD_IDX], WORKLOAD_LEVELS, WORKLOAD_LABELS)
  cache_mode <- factor(match[MODE_IDX], MODE_LEVELS, MODE_LABELS)
  var_value <- as.numeric(match[VAR_IDX])
  return (list(workload=workload, mode=cache_mode, var=var_value))
}

read_runs <- function(dir, var_name, suffix) {
  if (!file.info(dir)$isdir) {
    stop(paste(dir, "is not a directory"))
  }

  file_list <- list.files(dir, pattern=paste("*", suffix, sep=""),recursive=FALSE, full.names=TRUE)
  if (length(file_list) == 0) {
    stop(paste("no input files found in", dir))
  }

  stats <- data.frame()
  for (file in file_list) {
     res <- decode_filename(file, var_name, suffix)
     s <- read.table(file,sep = ",",row.names=NULL, header=TRUE)
     s$workload <- rep(res$workload, nrow(s))
     s$mode <- rep(res$mode, nrow(s))
     s$var <- rep(res$var, nrow(s))
     stats <- rbind(stats, s)
     rm(s)
  }
  return (stats)
}

response_time_throughput_plot <- function(dir) {
  stats <- read_runs(dir, "opslimit", "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    mean_stats <- subset(workload_stats, workload_stats$stat == "mean")
    p <- ggplot() 
    for (m in unique(mean_stats$mode)) {
      mean_mode_stats <- subset(mean_stats, mean_stats$mode == m)
      mean_mode_stats <- mean_mode_stats[order(mean_mode_stats$var), ]
      print(mean_mode_stats)
      p <- p + geom_path(data=mean_mode_stats,
                         mapping=aes(y=response_time, x=throughput, colour=mode),
                         arrow = arrow(length = unit(0.1,"cm")))
    }
    ggsave(p, file=paste(paste(file.path(dir, w), "-", "response_time_throughput", format_ext, sep="")), scale=1)
    #p <- p + theme(legend.position="none")
    #p <- p + theme(axis.line = element_line(color = 'black'))
    #p <- p + theme_bw() + theme(plot.background = element_blank(),panel.border = element_blank())
    #p <- p + scale_x_continuous("throughput [txn/s]") + scale_y_continuous("response time [ms]") 
  }
  # TODO: use whisker box, and import qunatiles rather than quartiles
  # TODO: format
  #   p <- ggplot(d, aes(y=response_time_median, x=throughput_median))
  #   p <- p + geom_line() + geom_errorbar(aes(ymax = response_time_quant75, ymin=response_time_quantt25), width=0.2) # + facet_grid(. ~ Type)
  #   p <- p + theme(axis.line = element_line(color = 'black'))
  #   p <- p + theme_bw() + theme(plot.background = element_blank(),panel.border = element_blank())
  #   p <- p + labs(x= "throughput [txn/s]",y = "operation response time [ms]")
  #   p <- p + theme(legend.position="none")

  # ggsave(p, file=paste(paste(file.path(path, "response_time_throughput"), format_ext, sep="")), scale=1)
}

multi_cdf_plot <- function(dir, var_name, var_label) {
  stats <- read_runs(dir, var_name, "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    permilles <- subset(workload_stats, workload_stats$stat == "permille")
    for (m in unique(permilles$mode)) {
      mode_permilles <- subset(permilles, permilles$mode == m)
      mode_permilles <- mode_permilles[order(mode_permilles$var), ]
      p <- ggplot() 
      p <- p + labs(x="operation response time [ms]",y = "CDF [%]")
      RESPONSE_TIME_CUTOFF <- 1000
      p <- p + coord_cartesian(xlim = c(0, RESPONSE_TIME_CUTOFF), ylim = c(0, 1.05))
      p <- p + theme_bw()
      p <- p + ggtitle(label = paste("YCSB: ", w, " SwiftCloud with ", m, sep=""))
      for (v in unique(permilles$var)) {
        var_permilles <- subset(mode_permilles, mode_permilles$var == v)
        max_response_time <- max(var_permilles$response_time)
        if (max_response_time < RESPONSE_TIME_CUTOFF) {
          # Fill in the rest of the CDF.
          missing_entries <- RESPONSE_TIME_CUTOFF - max_response_time
          response_times <- (max_response_time+1):RESPONSE_TIME_CUTOFF
          var_permilles <- rbind(var_permilles, data.frame(
            stat=rep("permille", missing_entries), stat_param=rep(1, missing_entries),
            response_time=response_times,
            throughput=rep(NA, missing_entries), workload=rep(w, missing_entries),
            mode=rep(m, missing_entries), var=rep(v, missing_entries)))
        }
        var_permilles$var_label <- paste(var_permilles$var * 100, "% of local accesses", sep="")
        # print(summary(var_permilles))
        p <- p + geom_line(data=var_permilles,
                           mapping=aes(y=stat_param, x=response_time, colour=var_label))
      }
      ggsave(p, file=paste(paste(file.path(dir, w), "-", m, "-multi_cdf", format_ext, sep="")), scale=1)
    }
  }
}

if (length(commandArgs(TRUE)) > 0) {
  path <- commandArgs(TRUE)[1]
  response_time_throughput_plot(path)
} else {
  if (!interactive()) {
    stop("syntax: response_time_throughput.R <directory with *-operations_stats.csv files>")
  }
}

