require("ggplot2")
require("reshape2")
require("stringr")
require("grid")
require("plyr")

format_ext <- ".png"

# TODO: use factors for workloads & modes?
WORKLOAD_LEVELS <- c("workloada-uniform", "workloada", "workloadb-uniform", "workloadb")
WORKLOAD_LABELS <- c("workload A (uniform)", "workload A (zipf)", "workload B (uniform)", "workload B (zipf)")
MODE_LEVELS <- c("notifications-frequent", "notifications-infrequent", "no-caching", "refresh-frequent", "refresh-infrequent")
MODE_LABELS <- c("notifications (every 1s)", "notifications (every 10s)", "no caching", "cache refresh (every 1s)", "cache refresh (every 10s)")

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
      error(paste("cannot match filename", file, "with the expected pattern", REGEX))
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
     stats <- rbind.fill(stats, s)
     rm(s)
  }
  return (stats)
}

throughput_response_time_plot <- function(dir) {
  stats <- read_runs(dir, "opslimit", "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    mean_stats <- subset(workload_stats, workload_stats$stat == "mean")
    p <- ggplot() 
    p <- p + ggtitle(label = paste("YCSB", w))
    p <- p + labs(x="throughput [txn/s]",y = "mean response time [ms]")
    for (m in unique(mean_stats$mode)) {
      mean_mode_stats <- subset(mean_stats, mean_stats$mode == m)
      mean_mode_stats <- mean_mode_stats[order(mean_mode_stats$var), ]
      print(mean_mode_stats)
      p <- p + geom_path(data=mean_mode_stats,
                         mapping=aes(y=response_time, x=throughput, colour=mode),
                         arrow = arrow(length = unit(0.1,"cm")))
    }
    p <- p + scale_colour_discrete(breaks = unique(mean_mode_stats$var))
    ggsave(p, file=paste(paste(file.path(dir, w), "-", "throughput_response_time", format_ext, sep="")), scale=1)
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

# If var_label == NA, then load (throughput op/s) is used as a variable
multi_cdf_plot <- function(dir, var_name, var_label) {
  stats <- read_runs(dir, var_name, "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      p <- ggplot()
      p <- p + labs(x="operation response time [ms]",y = "CDF [%]")
      RESPONSE_TIME_CUTOFF <- 1000
      p <- p + coord_cartesian(xlim = c(0, RESPONSE_TIME_CUTOFF), ylim = c(0, 1.05))
      p <- p + theme_bw()
      p <- p + ggtitle(label = paste("YCSB ", w, ", ", m, sep=""))
      labels <- c()
      for (v in unique(mode_stats$var)) {
        mode_var_stats <- subset(mode_stats, mode_stats$var == v)
        permilles <- subset(mode_var_stats, mode_var_stats$stat == "permille")
        max_response_time <- max(mode_var_stats$response_time)
        if (max_response_time < RESPONSE_TIME_CUTOFF) {
          # Fill in the rest of the CDF.
          missing_entries <- RESPONSE_TIME_CUTOFF - max_response_time
          response_times <- (max_response_time+1):RESPONSE_TIME_CUTOFF
          permilles <- rbind(permilles, data.frame(
            stat=rep("permille", missing_entries), stat_param=rep(1, missing_entries),
            response_time=response_times,
            throughput=rep(NA, missing_entries), workload=rep(w, missing_entries),
            mode=rep(m, missing_entries), var=rep(v, missing_entries)))
        }
        
        if (is.na(var_label)) {
          mean_throughput <- subset(mode_var_stats, mode_var_stats$stat == "mean")$throughput
          permilles$var_label <- rep(paste(mean_throughput, "txn/s load"), nrow(permilles))
        } else {
          permilles$var_label <- paste(permilles$var, var_label)
        }
        labels <- c(labels, unique(permilles$var_label))
        # print(summary(var_permilles))
        p <- p + geom_line(data=permilles,
                           mapping=aes(y=stat_param, x=response_time, colour=var_label))
      }
      p <- p + scale_colour_discrete(breaks = labels)
      ggsave(p, file=paste(paste(file.path(dir, w), "-", m, "-multi_cdf", format_ext, sep="")), scale=1)
    }
  }
}

var_throughput_plot <- function(dir, var_name, var_label) {
  stats <- read_runs(dir, var_name, "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() + ggtitle(label = paste("YCSB ", w))
    p <- p + labs(x=var_label,y = "throughput [txn/s]")
    # TODO: add error_bars
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      means <- subset(mode_stats, mode_stats$stat == "mean")
      means <- means[order(means$var), ]
      p <- p + geom_path(data=means,
                         mapping=aes(y=throughput, x=var, colour=mode))
    }
    ggsave(p, file=paste(paste(file.path(dir, w), "-", var_name, "-throughput", format_ext, sep="")), scale=1)
  }
}

# assumption: #clients = f(var)
var_throughput_per_client_plot <- function(dir, var_name, var_label, clients_number = function(var) (var/50)) {
  stats <- read_runs(dir, var_name, "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() + ggtitle(label = paste("YCSB ", w))
    p <- p + labs(x=var_label,y = "throughput per client [txn/s]")
    # TODO: add error_bars
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      means <- subset(mode_stats, mode_stats$stat == "mean")
      means <- means[order(means$var), ]
      means$throughput_per_client <- means$throughput / clients_number(means$throughput)
      p <- p + geom_path(data=means,
                         mapping=aes(y=throughput_per_client, x=var, colour=mode))
    }
    ggsave(p, file=paste(paste(file.path(dir, w), "-", var_name, "-throughput_per_client", format_ext, sep="")), scale=1)
  }
}

# clients_number is a function of variable specified by var_name (e.g. db size)
var_throughput_per_client_plot <- function(dir, var_name, var_label, clients_number = function(var) (var/50)) {
  stats <- read_runs(dir, var_name, "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() + ggtitle(label = paste("YCSB ", w))
    p <- p + labs(x=var_label,y = "throughput per client [txn/s]")
    # TODO: add error_bars
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      means <- subset(mode_stats, mode_stats$stat == "mean")
      means <- means[order(means$var), ]
      means$throughput_per_client <- means$throughput / clients_number(means$throughput)
      p <- p + geom_path(data=means,
                         mapping=aes(y=throughput_per_client, x=var, colour=mode))
    }
    ggsave(p, file=paste(paste(file.path(dir, w), "-", var_name, "-throughput_per_client", format_ext, sep="")), scale=1)
  }
}

var_clock_size_in_fetch_plot <- function(dir, var_name, var_label) {
  stats <- read_runs(dir, var_name, "meta_size.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() + ggtitle(label = paste("YCSB ", w))
    p <- p + labs(x=var_label,y = "clock size [bytes]")
    # TODO: add error_bars
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      means <- subset(mode_stats, mode_stats$stat == "mean")
      means <- means[order(means$var), ]
      p <- p + geom_path(data=means,
                         mapping=aes(y=BatchFetchObjectVersionReply, x=var, colour=mode))
    }
    ggsave(p, file=paste(paste(file.path(dir, w), "-", var_name, "-BatchFetchObjectVersionReply_clock", format_ext, sep="")), scale=1)
  }
}

var_clock_size_in_commit_plot <- function(dir, var_name, var_label) {
  stats <- read_runs(dir, var_name, "meta_size.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() + ggtitle(label = paste("YCSB ", w))
    p <- p + labs(x=var_label,y = "metadata size per transaction [bytes]")
    # TODO: add error_bars
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      means <- subset(mode_stats, mode_stats$stat == "mean")
      means <- means[order(means$var), ]
      p <- p + geom_path(data=means,
                         mapping=aes(y=BatchCommitUpdatesRequest.normalized, x=var, colour=mode))
    }
    ggsave(p, file=paste(paste(file.path(dir, w), "-", var_name, "-BatchCommitUpdatesRequest_clock_per_txn", format_ext, sep="")), scale=1)
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

