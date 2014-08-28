require("ggplot2")
require("reshape2")
require("stringr")
require("grid")
require("plyr")

format_ext <- ".png"

WORKLOAD_LEVELS <- c("workloada-uniform", "workloada", "workloadb-uniform", "workloadb", "workload-social", "workload-social-views-counter")
WORKLOAD_LABELS <- c("YCSB A (uniform)", "YCSB A (zipf)", "YCSB B (uniform)", "YCSB B (zipf)", "SwiftSocial", "SwiftSocial (page view counters)")
MODE_LEVELS <- c("notifications-frequent", "notifications-frequent-no-pruning",  "notifications-frequent-practi", "notifications-infrequent", "notifications-infrequent-practi", "no-caching", "refresh-frequent", "refresh-frequent-no-pruning", "refresh-infrequent", "refresh-infrequent-bloated-counters", "refresh-infrequent-no-pruning", "refresh-infrequent-no-pruning-bloated-counters")
MODE_LABELS <- rep(MODE_LEVELS, 1)
#MODE_LABELS <- c("mutable cache with 1 notification per sec", "mutable cache with 1 notification per sec" + no pruning", "mutable cache + 1 notification/s + PRACTI metadata", "mutable cache + 1 notification/10s", "mutable cache + 1 notification/10s + PRACTI metadata", "no cache replica", "mutable cache + 1 refresh/s", "mutable cache + 1 refresh/s + no pruning", "mutable cache + 1 refresh/10s")

THEME <- theme_bw() + theme(plot.background = element_blank(),panel.border = element_blank(),
                            legend.position='bottom', legend.direction='vertical', legend.box='horizontal', legend.key=element_blank())
TOP_DIR <- "~/Dropbox/INRIA/results/"
experiment_dir <- function(experiment) {
  return (file.path(TOP_DIR, experiment, "processed"))  
}

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
      stop(paste("cannot match filename", file, "with the expected pattern", REGEX))
  }
  workload <- factor(match[WORKLOAD_IDX], WORKLOAD_LEVELS, WORKLOAD_LABELS)
  cache_mode <- factor(match[MODE_IDX], MODE_LEVELS, MODE_LABELS)
  var_value <- as.numeric(match[VAR_IDX])
  return (list(workload=workload, mode=cache_mode, var=var_value))
}

read_runs_full <- function(dir, var_name, suffix) {
  full_processor <- function (type, s) {
    s$workload <- rep(type$workload, nrow(s))
    s$mode <- rep(type$mode, nrow(s))
    s$var <- rep(type$var, nrow(s))
    s$stat_param
    return (s)
  }
  return (read_runs_impl(dir, var_name, suffix, full_processor))
}

read_runs_params <- function(dir, var_name, suffix, params = c()) {
  params_processor <- function(type, s) {
    row <- data.frame(type)
    if (length(params) == 0) {
      params <- setdiff(names(s), c("stat", "stat_param"))
    }
    for (param in params) {
      row[[paste(param, "mean", sep=".")]] <- subset(s, stat == "mean")[[param]]
      row[[paste(param, "stddev", sep=".")]] <- subset(s, stat == "stddev")[[param]]
      for (quantile in seq(0, 100, by = 5)) {
        row[[paste(param, paste("q", quantile, sep=""), sep=".")]] <- subset(s, stat == "permille" & stat_param == quantile/100 )[[param]]
      }
    }
    return (row)
  }
  return (read_runs_impl(dir, var_name, suffix, params_processor))
}

read_runs_errors <- function(dir, var_name, suffix) {
  errors_processor <- function (type, s) {
    row <- data.frame(type)
    for (eachCause in s$cause) {
      row[[paste("errors", eachCause, sep=".")]] <- subset(s, cause == eachCause)$occurences
    }
    return (row)
  }
  return (read_runs_impl(dir, var_name, suffix, errors_processor))
}

read_runs_impl <- function(dir, var_name, suffix, processor) {
  if (!file.info(dir)$isdir) {
    stop(paste(dir, "is not a directory"))
  }

  file_list <- list.files(dir, pattern=paste("*", suffix, sep=""),recursive=FALSE, full.names=TRUE)
  if (length(file_list) == 0) {
    stop(paste("no input files found in", dir))
  }

  stats <- data.frame()
  for (file in file_list) {
     decoded <- decode_filename(file, var_name, suffix)
     s <- read.table(file,sep = ",",row.names=NULL, header=TRUE)
     if ("stat_params" %in% names(s)) {
       # HOTFIX for old files generated with a typo
       s$stat_param <- s$stat_params
       s <- subset(s, select=-stat_params)
     }
     stats <- rbind.fill(stats, processor(decoded, s))
     rm(decoded)
     rm(s)
  }
  return (stats)
}

# If var_label == NA, then load (throughput op/s) is used as a variable
var_response_time_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "ops.csv")
  xvar <- "var"
  if (is.na(var_label)) {
    var_label <- "throughput [txn/s]"
    xvar <- "throughput.mean"
  }
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() 
    p <- p + ggtitle(label =  w)
    p <- p + labs(x=var_label,y = "response time [ms]")
    p <- p + THEME
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      melted <- melt(mode_stats, id.vars=c("workload", "mode", xvar))
      # Is this a canonical way to do this?
      melted <- subset(melted, variable %in% c("response_time.q75", "response_time.q95"))
      p <- p + geom_path(data=melted,
                         mapping=aes_string(y="value", x=xvar, group="variable", color="mode", linetype="variable"))
      p <- p + geom_point(data=melted,
                         mapping=aes_string(y="value", x=xvar, group="variable", color="mode", shape="mode"))
    }
    p <- p + scale_colour_discrete(breaks = unique(workload_stats$mode))
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-response_time", format_ext, sep="")), scale=1)
  }
}

scalabilitythroughput_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA)
}

scalabilityclients_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilityclients"), var_name="clients", var_label="#clients")
}

scalabilityclientssmalldb_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilityclients-smalldb"), var_name="clients", var_label="#clients")
}

# If var_label == NA, then load (throughput op/s) is used as a variable
multi_cdf_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_full(dir, var_name, "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    cols <-  names(workload_stats)
    for (resp_time_type in cols[grepl("response_time", cols)]) {
      if (length(setdiff(unique(workload_stats[[resp_time_type]]), c(NA))) == 0) {
        next
      }
      for (m in unique(workload_stats$mode)) {
        mode_stats <- subset(workload_stats, workload_stats$mode == m)
        mode_stats <- mode_stats[order(mode_stats$var), ]
        p <- ggplot()
        p <- p + labs(x="operation response time [ms]",y = "CDF [%]")
        RESPONSE_TIME_CUTOFF <- 1500
        p <- p + coord_cartesian(xlim = c(0, RESPONSE_TIME_CUTOFF), ylim = c(0, 1.05))
        p <- p + THEME
        p <- p + ggtitle(label = paste(w, ", ", m, sep=""))
        labels <- c()
        for (v in unique(mode_stats$var)) {
          mode_var_stats <- subset(mode_stats, mode_stats$var == v)
          permilles <- subset(mode_var_stats, mode_var_stats$stat == "permille")
          max_response_time <- max(mode_var_stats[[resp_time_type]])
          if (max_response_time < RESPONSE_TIME_CUTOFF) {
            # Fill in the rest of the CDF.
            missing_entries <- (max_response_time+1):RESPONSE_TIME_CUTOFF
            filling <- data.frame(
              stat=rep("permille", length(missing_entries)), stat_param=rep(1, length(missing_entries)),
              throughput=rep(NA, length(missing_entries)), workload=rep(w, length(missing_entries)),
              mode=rep(m, length(missing_entries)), var=rep(v, length(missing_entries)))
            filling[[resp_time_type]] <- missing_entries
            permilles <- rbind.fill(permilles, filling)
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
                             mapping=aes_string(y="stat_param", x=resp_time_type, colour="var_label"))
        }
        p <- p + scale_colour_discrete(breaks = labels)
        dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
        ggsave(p, file=paste(paste(file.path(output_dir, w), "-", m, "-multi_cdf-", resp_time_type, format_ext, sep="")), scale=1)
      }
    }
  }
}

scalabilitythroughput_multi_cdf_plot <- function() {
  multi_cdf_plot(experiment_dir("scalabilitythroughput"), var_name = "opslimit", var_label = NA)
}

scalabilityclients_multi_cdf_plot <- function() {
  multi_cdf_plot(experiment_dir("scalabilityclients"), var_name = "clients", var_label = "clients")
}

scalabilitydbsize_multi_cdf_plot <- function() {
  multi_cdf_plot(experiment_dir("scalabilityclients"), var_name = "dbsize", var_label = "objects (~ #clients)")
}

var_errors_plot <- function(dir, var_name, var_axis_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_errors(dir, var_name, "errors.csv")
  p <- ggplot()
  p <- p + labs(x=var_axis_label,y = "operation failures/run")
  p <- p + THEME
  p <- p + ggtitle(label = "Errors")
  for (m in unique(stats$mode)) {
    mode_stats <- subset(stats, mode == m)
    mode_stats <- mode_stats[order(mode_stats$var), ]
    melted <- melt(mode_stats, id.vars=c("workload", "mode", "var"), na.rm=TRUE) 
    p <- p + geom_point(data=melted, mapping=aes_string(x="var", y="value", group="variable", color="mode", shape="variable"))
  }
  p<- p + facet_grid(workload ~ ., scale = "free")
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  ggsave(p, file=paste(paste(file.path(output_dir, var_name), "-errors", format_ext, sep="")), scale=1)
}

scalabilitythroughput_errors_plot <- function() {
  var_errors_plot(experiment_dir("scalabilitythroughput"), var_name = "opslimit", var_axis_label= "load limit [txn/s]")
}

scalabilityclients_errors_plot <- function() {
  var_errors_plot(experiment_dir("scalabilityclients"), var_name = "clients", var_axis_label= "#clients")
}

scalabilityclientssmalldb_errors_plot <- function() {
  var_errors_plot(experiment_dir("scalabilityclients-smalldb"), var_name = "clients", var_axis_label= "#clients")
}

scalabilitydbsize_errors_plot <- function() {
  var_errors_plot(experiment_dir("scalabilitydbsize"), var_name = "dbsize", var_axis_label= "#objects (~ #clients)")
}

clientfailures_errors_plot <- function() {
  var_errors_plot(experiment_dir("clientfailures"), var_name = "failures", var_axis_label= "#unavilable client replicas")
}

responsetimelocality_errors_plot <- function() {
  var_errors_plot(experiment_dir("responsetimelocality"), var_name = "locality", var_label= "fraction of local requests")
}

var_throughput_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison"), error_bars = FALSE) {
  stats <- read_runs_params(dir, var_name, "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() + ggtitle(label =  w)
    p <- p + labs(x=var_label,y = "throughput [txn/s]")
    p <- p + THEME
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      p <- p + geom_path(data=mode_stats,
                         mapping=aes(y=throughput.mean, x=var, colour=mode))
      if (error_bars) {
        p <- p + geom_errorbar(data=mode_stats,
                         mapping=aes(ymax=throughput.mean+throughput.stddev, ymin=throughput.mean-throughput.stddev, x=var, colour=mode),
                         width=0.2)
      }
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-throughput", format_ext, sep="")), scale=1)
  }
}

scalabilityclients_throughput_plot <- function() {
  var_throughput_plot(experiment_dir("scalabilityclients"), "clients", "#clients")
}

scalabilityclientssmalldb_throughput_plot <- function() {
  var_throughput_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#clients")
}

scalabilitydbsize_throughput_plot <- function() {
  var_throughput_plot(experiment_dir("scalabilitydbsize"), "dbsize", "#objects (~ #clients)")
}

# #clients = f(var)
var_throughput_per_client_plot <- function(dir, var_name, var_label, clients_number, output_dir=file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() + ggtitle(label =  w)
    p <- p + labs(x=var_label,y = "throughput per client [txn/s]")
    p <- p + THEME
    # TODO: add error_bars
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      mode_stats$throughput.mean_per_client <- mode_stats$throughput.mean / clients_number(mode_stats$var)
      p <- p + geom_path(data=mode_stats,
                         mapping=aes(y=throughput.mean_per_client, x=var, colour=mode))
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-throughput_per_client", format_ext, sep="")), scale=1)
  }
}

scalabilityclients_throughput_per_client_plot <- function() {
  var_throughput_per_client_plot(experiment_dir("scalabilityclients"), "clients", "#clients", clients_number = identity)
}

scalabilitydbsize_throughput_per_client_plot <- function() {
  var_throughput_per_client_plot(experiment_dir("scalabilitydbsize"), "dbsize", "#objects (~ #clients)", clients_number = function(objects) (objects / 20))
}

var_clock_size_in_fetch_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_full(dir, var_name, "meta_size.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() + ggtitle(label = w)
    p <- p + labs(x=var_label,y = "fetch clock size [bytes]")
    p <- p + THEME
    # TODO: add error_bars
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      means <- subset(mode_stats, mode_stats$stat == "mean")
      means <- means[order(means$var), ]
      p <- p + geom_path(data=means,
                         mapping=aes(y=BatchFetchObjectVersionReply.meta, x=var, colour=mode))
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-BatchFetchObjectVersionReply_clock", format_ext, sep="")), scale=1)
  }
}

normalize_batch <- function(stats, message, batch_size, norm="norm3") {
  indep_mean_col <- paste(message, "indep.mean", sep=".")
  indep_stddev_col <- paste(message, "indep.stddev", sep=".")
  dep_mean_col <- paste(message, "dep", norm, "mean", sep=".")
  dep_stddev_col <- paste(message, "dep", norm, "stddev", sep=".")
  scaled_mean_col <- paste(message, "mean.scaled", sep=".")
  scaled_stddev_col <- paste(message, "stddev.scaled", sep=".")
  scaled_min_col <- paste(message, "min.scaled", sep=".")
  scaled_max_col <- paste(message, "max.scaled", sep=".")
  
  stats[[scaled_mean_col]] <- stats[[indep_mean_col]] + stats[[dep_mean_col]] * batch_size
  # TODO: scale stddev more precisely
  stats[[scaled_stddev_col]] <- stats[[indep_stddev_col]] + stats[[dep_stddev_col]] * batch_size
  stats[[scaled_min_col]] <- stats[[scaled_mean_col]] - stats[[scaled_stddev_col]]
  stats[[scaled_min_col]][stats[[scaled_min_col]] < 0] <- 0
  stats[[scaled_max_col]] <- stats[[scaled_mean_col]] + stats[[scaled_stddev_col]]
  return (stats)
}

NOTIFICATIONS_EXAMPLE_BATCH_SIZE <- 10
var_notifications_metadata_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", c("BatchUpdatesNotification.meta.indep", "BatchUpdatesNotification.meta.dep.norm3"))
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, workload == w)
    p <- ggplot() + ggtitle(label = paste(w, "normalized notifications message metadata"))
    p <- p + labs(x=var_label,y = "notification message metadata for 10 updates [bytes]")
    p <- p + THEME
    p <- p + scale_y_continuous(limits=c(1, 20000)) #, breaks=c(1, 10, 100, 1000, 10000))
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      mode_stats <- normalize_batch(mode_stats, "BatchUpdatesNotification.meta", NOTIFICATIONS_EXAMPLE_BATCH_SIZE)
      p <- p + geom_line(data=mode_stats,
                         mapping=aes(y=BatchUpdatesNotification.meta.mean.scaled,
                                     x=var, color=mode))
      p <- p + geom_errorbar(data=mode_stats,
                             mapping=aes(ymax=BatchUpdatesNotification.meta.max.scaled,
                                         ymin=BatchUpdatesNotification.meta.min.scaled,
                                         x=var), color="black", width=15)
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-BatchUpdatesNotification-metadata", format_ext, sep="")), scale=1)
  }
}

scalabilityclientssmalldb_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#clients")
}

scalabilitydbsize_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("scalabilitydbsize"), "dbsize", "#objects (~ #clients)")
}

clientfailures_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("clientfailures"), "failures", "#unavailable clients")
}

COMMIT_EXAMPLE_BATCH_SIZE <- 10
var_commit_metadata_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", c("BatchCommitUpdatesRequest.meta.indep", "BatchCommitUpdatesRequest.meta.dep.norm3"))
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, workload == w)
    p <- ggplot() + ggtitle(label = paste(w, "normalized commit message metadata"))
    p <- p + labs(x=var_label,y = "commit message metadata for 10 updates [bytes]")
    p <- p + THEME
    p <- p + scale_y_continuous(limits=c(1, 100)) #, breaks=c(1, 10, 100, 1000, 10000))
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      mode_stats <- normalize_batch(mode_stats, "BatchCommitUpdatesRequest.meta", COMMIT_EXAMPLE_BATCH_SIZE)
      p <- p + geom_line(data=mode_stats,
                         mapping=aes(y=BatchCommitUpdatesRequest.meta.mean.scaled,
                                     x=var, color=mode))
      p <- p + geom_errorbar(data=mode_stats,
                             mapping=aes(ymax=BatchCommitUpdatesRequest.meta.max.scaled,
                                         ymin=BatchCommitUpdatesRequest.meta.min.scaled,
                                         x=var), color="black", width=15)
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-BatchCommitUpdatesRequest-metadata", format_ext, sep="")), scale=1)
  }
}

scalabilityclients_commit_metadata_plot <- function() {
  var_commit_metadata_plot(experiment_dir("scalabilityclients"), "clients", "#clients")
}

scalabilityclientssmalldb_commit_metadata_plot <- function() {
  var_commit_metadata_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#clients")
}

scalabilitydbsize_commit_metadata_plot <- function() {
  var_commit_metadata_plot(experiment_dir("scalabilitydbsize"), "dbsize", "#objects (~ #clients)")
}

clientfailures_commit_metadata_plot <- function() {
  var_commit_metadata_plot(experiment_dir("clientfailures"), "failures", "#unavailable clients")
}

CACHE_REFRESH_BATCH_SIZE <- 10
var_cacherefresh_metadata_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", c("CacheRefreshReply.meta.indep", "CacheRefreshReply.meta.dep.norm2"))
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, workload == w)
    p <- ggplot() + ggtitle(label = paste(w, "normalized cache refresh message metadata"))
    p <- p + labs(x=var_label,y = "cache refresh message metadata for 10 updated objects [bytes]")
    p <- p + THEME
    p <- p + scale_y_continuous(limits=c(1, 100)) #, breaks=c(1, 10, 100, 1000, 10000))
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      mode_stats <- normalize_batch(mode_stats, "CacheRefreshReply.meta", CACHE_REFRESH_BATCH_SIZE, norm="norm2")
      p <- p + geom_line(data=mode_stats,
                         mapping=aes(y=CacheRefreshReply.meta.mean.scaled,
                                     x=var, color=mode))
      p <- p + geom_errorbar(data=mode_stats,
                             mapping=aes(ymax=CacheRefreshReply.meta.max.scaled,
                                         ymin=CacheRefreshReply.meta.min.scaled,
                                         x=var), color="black", width=15)
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-CacheRefreshReply-metadata", format_ext, sep="")), scale=1)
  }
}

scalabilityclients_cacherefresh_metadata_plot <- function() {
  var_cacherefresh_metadata_plot(experiment_dir("scalabilityclients"), "clients", "#clients")
}

scalabilityclientssmalldb_cacherefresh_metadata_plot <- function() {
  var_cacherefresh_metadata_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#clients")
}

scalabilitydbsize_cacherefresh_metadata_plot <- function() {
  var_cacherefresh_metadata_plot(experiment_dir("scalabilitydbsize"), "dbsize", "#objects (~ #clients)")
}

clientfailures_cacherefresh_metadata_plot <- function() {
  var_cacherefresh_metadata_plot(experiment_dir("clientfailures"), "failures", "#unavailable client replicas")
}

clientfailures_cacherefresh_metadata_plot <- function() {
  var_cacherefresh_metadata_plot(experiment_dir("clientfailures"), "failures", "#unavailable clients")
}


IDEMPOTENCE_GUARD_ENTRY_BYTES <- 6
select_table <- function (workload) {
  if (grepl("social", workload, ignore.case=TRUE)) {
    return ("views.table")
  }
  return ("usertable.table")
}
TABLES <- c(select_table("swiftsocial"), select_table("YCSB"))
dc_table <- function(table) {
  return (paste(table, "dc", sep="."))
}
var_storage_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", c("idempotenceGuard", dc_table(TABLES)))
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, workload == w)
    p <- ggplot() + ggtitle(label = w)
    p <- p + labs(x=var_label,y = "DC replica storage utilization [bytes]")
    p <- p + THEME
    p <- p + scale_y_log10()
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      mode_stats$idempotenceGuard.scaled.mean <- mode_stats$idempotenceGuard.mean*IDEMPOTENCE_GUARD_ENTRY_BYTES
      mode_stats$total <- mode_stats$idempotenceGuard.scaled.mean + mode_stats[[paste(dc_table(select_table(w)), "mean", sep=".")]]
      melted <- melt(mode_stats, id.vars=c("workload", "mode", "var"))
      melted <- subset(melted, variable %in% c("idempotenceGuard.scaled.mean", paste(dc_table(select_table(w)), "mean", sep="."), "total"))
      p <- p + geom_path(data=melted, mapping=aes_string(y="value", x="var", color="mode", linetype="variable"))
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-storage_dc", format_ext, sep="")), scale=1)
  }
}

clientfailures_storage_plot <- function() {
  var_storage_plot(experiment_dir("clientfailures"), "failures", "#unavailable clients")
}

scalabilityclients_storage_plot <- function() {
  var_storage_plot(experiment_dir("scalabilityclients"), "clients", "#clients")
}

scalabilityclientssmalldb_storage_plot <- function() {
  var_storage_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#clients")
}

client_table <- function(table) {
  return (paste(table, "client", sep="."))
}
var_client_storage_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", client_table(TABLES))
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, workload == w)
    p <- ggplot() + ggtitle(label = w)
    p <- p + labs(x=var_label,y = "client replica storage utilization [bytes]")
    p <- p + THEME
    p <- p + scale_y_log10()
    # TODO: add error_bars
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      p <- p + geom_path(data=mode_stats, mapping=aes_string(y=paste(client_table(select_table(w)), "mean", sep="."), x="var", color="mode"))
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-storage_client", format_ext, sep="")), scale=1)
  }
}

clientfailures_client_storage_plot <- function() {
  var_client_storage_plot(experiment_dir("clientfailures"), "failures", "#unavailable clients")
}

var_clock_size_in_commit_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_full(dir, var_name, "meta_size.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() + ggtitle(label = w)
    p <- p + labs(x=var_label,y = "commit clock size per transaction [bytes]")
    p <- p + THEME
    # TODO: add error_bars
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      means <- subset(mode_stats, mode_stats$stat == "mean")
      means <- means[order(means$var), ]
      p <- p + geom_path(data=means,
                         mapping=aes(y=BatchCommitUpdatesRequest.meta.norm1, x=var, colour=mode))
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-BatchCommitUpdatesRequest_clock_per_txn", format_ext, sep="")), scale=1)
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

