require("ggplot2")
require("reshape2")
require("stringr")
require("grid")
require("plyr")

preview <- TRUE
format_ext <- ifelse(preview, ".png", ".pdf")
add_title <- function(plot, title) {
  if (preview) {
    plot <- plot + ggtitle(label=title)
  }
  return (plot)
}

WORKLOAD_LEVELS <- c("workloada-uniform", "workloada", "workloadb-uniform", "workloadb", "workload-social", "workload-social-views-counter", "workloada-uniform-lowlocality", "workloada-lowlocality", "workloadb-uniform-lowlocality", "workloadb-lowlocality", "workload-social-lowlocality", "workload-social-views-counter-lowlocality")
WORKLOAD_LABELS <- c("YCSB A (uniform)", "YCSB A (zipf)", "YCSB B (uniform)", "YCSB B (zipf)", "SwiftSocial", "SwiftSocial (page view counters)", "YCSB A (uniform, low locality)", "YCSB A (zipf, low locality)", "YCSB B (uniform, low locality)", "YCSB B (zipf, low locality)", "SwiftSocial (low locality)", "SwiftSocial (page view counters, low locality)")
PURE_MODE_LEVELS <- c("notifications-frequent", "notifications-frequent-no-pruning",  "notifications-frequent-practi", "notifications-frequent-bloated-counters", "notifications-infrequent", "notifications-infrequent-practi", "notifications-infrequent-bloated-counters", "no-caching", "refresh-frequent", "refresh-frequent-no-pruning", "refresh-infrequent", "refresh-infrequent-bloated-counters", "refresh-infrequent-no-pruning", "refresh-infrequent-no-pruning-bloated-counters")
MODE_LEVELS <- PURE_MODE_LEVELS
# TODO: use cartesian product
for (cc in paste("clients", seq(500, 2500, by=500), sep="-")) {
  MODE_LEVELS <- union(MODE_LEVELS, paste(PURE_MODE_LEVELS, cc, sep="-"))
}
DCED_MODE_LEVELS <- rep(MODE_LEVELS, 1)
for (dd in paste("dcs", seq(3, 9, by=3), sep="-")) {
  MODE_LEVELS <- union(MODE_LEVELS, paste(DCED_MODE_LEVELS, dd, sep="-"))
}
MODE_LABELS <- MODE_LEVELS #TODO
#MODE_LABELS <- c("mutable cache with 1 notification per sec", "mutable cache with 1 notification per sec" + no pruning", "mutable cache + 1 notification/s + PRACTI metadata", "mutable cache + 1 notification/10s", "mutable cache + 1 notification/10s + PRACTI metadata", "no cache replica", "mutable cache + 1 refresh/s", "mutable cache + 1 refresh/s + no pruning", "mutable cache + 1 refresh/10s")

THEME <- theme_bw() + theme(plot.background = element_blank(),panel.border = element_blank(),
                            legend.position='bottom', legend.direction='vertical', legend.box='horizontal', legend.key=element_blank())

# computer-scientish log. scale
cs_log_scale <- function(min_value=10**0, max_value=10**6) {
  # TODO: we should use log2, but it's a bit more difficult with the newest version of ggplot2
  breaks <- 10**(floor(log10(max(min(min_value, max_value), 1))):ceiling(log10(max(max_value, min_value, 1))))
  labeler <- function(value) {
    if (value < 1000) {
      return (value)
    }
    if (value < 1000000) {
      return (paste(value/1000, "K", sep=""))
    }
    return (paste(value/1000000, "M", sep=""))
  }
  labels <- lapply(breaks, labeler)
  return (scale_y_log10(breaks = breaks, labels=labels, limits=c(min(breaks), max(breaks))))
}

TOP_DIR <- "~/Dropbox/INRIA/results/"
experiment_dir <- function(experiment) {
  return (file.path(TOP_DIR, experiment, "processed"))  
}

pattern_alternatives <- function(alternatives, exact_match = FALSE) {
  first <- TRUE
  exact_match_open <- ifelse(exact_match, "^", "")
  exact_match_end <- ifelse(exact_match, "$", "")
  pattern <- ""
  for (s in alternatives) {
    sep = ifelse(first, "", "|")
    pattern <- paste(pattern, paste("(", exact_match_open, s, exact_match_end, ")", sep=""), sep=sep)
    first <- FALSE
  }
  return (pattern)
}

decode_filename <- function(file, var_name, suffix) {
  REGEX <- paste("(",pattern_alternatives(WORKLOAD_LEVELS),")-mode-(", pattern_alternatives(MODE_LEVELS), ")-", var_name, "-([0-9.]+)-", suffix, sep="")
  WORKLOAD_IDX <- 2
  MODE_IDX <- WORKLOAD_IDX + length(WORKLOAD_LEVELS) + 1
  VAR_IDX <- MODE_IDX + length(MODE_LEVELS) + 1
  match <- str_match(file, REGEX)
  
  if (length(match)<= 1 || is.element(c(match[WORKLOAD_IDX], match[MODE_IDX], match[VAR_IDX]), NA)) {
      stop(paste("cannot match filename", file, "with the expected pattern", REGEX))
  }
  workload <- match[WORKLOAD_IDX]
  cache_mode <- match[MODE_IDX]
  var_value <- as.numeric(match[VAR_IDX])
  return (list(workload=workload, mode=cache_mode, var=var_value))
}

read_runs_full <- function(dir, var_name, suffix, workload_pattern=".+", mode_pattern=".+") {
  full_processor <- function (type, s) {
    s$workload <- rep(type$workload, nrow(s))
    s$mode <- rep(type$mode, nrow(s))
    s$var <- rep(type$var, nrow(s))
    s$stat_param
    return (s)
  }
  return (read_runs_impl(dir, var_name, suffix, full_processor, workload_pattern, mode_pattern))
}

read_runs_params <- function(dir, var_name, suffix, params = c(), workload_pattern=".+", mode_pattern=".+") {
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
  return (read_runs_impl(dir, var_name, suffix, params_processor, workload_pattern, mode_pattern))
}

read_runs_errors <- function(dir, var_name, suffix) {
  errors_processor <- function (type, s) {
    row <- data.frame(type)
    for (eachCause in s$cause) {
      row[[paste("errors", eachCause, sep=".")]] <- subset(s, cause == eachCause)$occurences
    }
    return (row)
  }
  return (read_runs_impl(dir, var_name, suffix, errors_processor, ".+", ".+"))
}

read_runs_impl <- function(dir, var_name, suffix, processor, workload_pattern, mode_pattern) {
  if (!file.info(dir)$isdir) {
    stop(paste(dir, "is not a directory"))
  }

  file_list <- list.files(dir, pattern=paste("*", suffix, sep=""),recursive=FALSE, full.names=TRUE)
  if (length(file_list) == 0) {
    stop(paste("no input files found in", dir))
  }

  stats <- data.frame()
  for (file in file_list) {
    hide_file <- paste(sub(paste(suffix, "$", sep=""), "", file), "hide")
    if (file.exists(hide_file)) {
      next
    }
     decoded <- decode_filename(file, var_name, suffix)
     if (!grepl(workload_pattern, decoded$workload) | !grepl(mode_pattern, decoded$mode)) {
       next
     }
     s <- read.table(file,sep = ",",row.names=NULL, header=TRUE)
     if ("stat_params" %in% names(s)) {
       # HOTFIX for old files generated with a typo
       s$stat_param <- s$stat_params
       s <- subset(s, select=-stat_params)
     }
     decoded$workload <- factor(decoded$workload, WORKLOAD_LEVELS, WORKLOAD_LABELS)
     decoded$mode <- factor(decoded$mode, MODE_LEVELS, MODE_LABELS)
     stats <- rbind.fill(stats, processor(decoded, s))
     rm(decoded)
     rm(s)
  }
  return (stats)
}

# If var_label == NA, then load (throughput op/s) is used as a variable
var_response_time_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison"),
                                   workload_pattern=".+", mode_pattern=".+", modes=c(), modes_labels=c(),
                                   lower_quantile=70, file_suffix = "") {
  if (length(modes) > 0) {
    mode_pattern <- pattern_alternatives(modes, TRUE)
  }
  stats <- read_runs_params(dir, var_name, "ops.csv", workload_pattern=workload_pattern, mode_pattern=mode_pattern)
  response_time_lower_quantile <- paste("response_time.q", lower_quantile, sep="")
  xvar <- "var"
  if (is.na(var_label)) {
    var_label <- "throughput [txn/s]"
    xvar <- "throughput.mean"
  }
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() 
    p <- add_title(p, w)
    p <- p + labs(x=var_label,y = "response time [ms]")
    p <- p + scale_y_continuous(limits=c(0, 1000))
    p <- p + THEME
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      melted <- melt(mode_stats, id.vars=c("workload", "mode", xvar))
      # Is this a canonical way to do this?
      melted <- subset(melted, variable %in% c(response_time_lower_quantile, "response_time.q95"))
      p <- p + geom_path(data=melted,
                         mapping=aes_string(y="value", x=xvar, group="variable", color="mode", linetype="variable"))
      p <- p + geom_point(data=melted,
                         mapping=aes_string(y="value", x=xvar, group="variable", color="mode", shape="mode"))
    }
    if (length(modes) > 0) {
      p <- p + scale_color_discrete(name="System configuration", breaks=modes, labels=modes_labels)
      p <- p + scale_shape_discrete(name="System configuration", breaks=modes, labels=modes_labels)
    } else {
      p <- p + scale_color_discrete(breaks = unique(workload_stats$mode))
    }
    p <- p + scale_linetype_discrete(name = "Response time w.r.t. access locality",
                                     breaks = c(response_time_lower_quantile, "response_time.q95"),
                                     labels = c(paste(lower_quantile, "th percentile (expected local request)", sep=""), "95th percentile (remote request)"))
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), file_suffix, "-", var_name, "-response_time", format_ext, sep="")), scale=1)
  }
}

scalabilitythroughput_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA)
}

scalabilitythroughputlowlocality_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA, workload_pattern="workloada.+lowlocality", mode_pattern=pattern_alternatives(c("no-caching", "notifications-(in)?frequent-clients-500")), lower_quantile=20)
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA, workload_pattern="(workloadb|social).+lowlocality", mode_pattern=pattern_alternatives(c("no-caching", "notifications-(in)?frequent-clients-1000")), lower_quantile=20)
}

scalabilitythroughputclients_response_time_plot <- function() {
  clients <- seq(500, 2000, by=500)
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA,
                         modes=c("no-caching-clients-1000", paste("notifications-frequent-clients-", clients, sep="")),
                         modes_labels=c("no client replicas", paste(clients, "client replicas")),
                         workload_pattern=pattern_alternatives(c("workloada-uniform", "workloadb-uniform", "workload-social"), TRUE),
                         file_suffix="-clients")
}

scalabilitythroughput9dcs_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilitythroughput-9dcs"), var_name="opslimit", var_label=NA)
}

scalabilityclients_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilityclients"), var_name="clients", var_label="#client replicas")
}

scalabilityclientssmalldb_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilityclients-smalldb"), var_name="clients", var_label="#client replicas")
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
        p <- add_title(p, paste(w, ", ", m, sep=""))
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

scalabilitythroughputlowlocality_multi_cdf_plot <- function() {
  multi_cdf_plot(experiment_dir("scalabilitythroughput"), var_name = "opslimit", var_label = NA, workload_pattern="lowlocality")
}

scalabilityclients_multi_cdf_plot <- function() {
  multi_cdf_plot(experiment_dir("scalabilityclients"), var_name = "clients", var_label = "client replicas")
}

scalabilityclientssmalldb_multi_cdf_plot <- function() {
  multi_cdf_plot(experiment_dir("scalabilityclients-smalldb"), var_name = "clients", var_label = "client replicas")
}

scalabilitydbsize_multi_cdf_plot <- function() {
  multi_cdf_plot(experiment_dir("scalabilityclients"), var_name = "dbsize", var_label = "objects (~ #client replicas)")
}

var_errors_plot <- function(dir, var_name, var_axis_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_errors(dir, var_name, "errors.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, workload == w)
    p <- ggplot()
    p <- p + labs(x=var_axis_label,y = "operation failures/run")
    p <- p + THEME
    p <- add_title(p, paste(w, "errors"))
    for (m in unique(stats$mode)) {
      mode_stats <- subset(workload_stats, mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      melted <- melt(mode_stats, id.vars=c("workload", "mode", "var"), na.rm=TRUE) 
      p <- p + geom_point(data=melted, mapping=aes_string(x="var", y="value", group="variable", color="mode", shape="variable"),
                          position=position_jitter(w=30, h=0))
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-errors", format_ext, sep="")), scale=1)
  }
}

scalabilitythroughput_errors_plot <- function() {
  var_errors_plot(experiment_dir("scalabilitythroughput"), var_name = "opslimit", var_axis_label= "load limit [txn/s]")
}

scalabilityclients_errors_plot <- function() {
  var_errors_plot(experiment_dir("scalabilityclients"), var_name = "clients", var_axis_label= "#client replicas")
}

scalabilityclientssmalldb_errors_plot <- function() {
  var_errors_plot(experiment_dir("scalabilityclients-smalldb"), var_name = "clients", var_axis_label= "#client replicas")
}

scalabilitydbsize_errors_plot <- function() {
  var_errors_plot(experiment_dir("scalabilitydbsize"), var_name = "dbsize", var_axis_label= "#objects (~ #client replicas)")
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
    p <- ggplot()
    p <- add_title(p, w)
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
  var_throughput_plot(experiment_dir("scalabilityclients"), "clients", "#client replicas")
}

scalabilityclientssmalldb_throughput_plot <- function() {
  var_throughput_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#client replicas")
}

scalabilitydbsize_throughput_plot <- function() {
  var_throughput_plot(experiment_dir("scalabilitydbsize"), "dbsize", "#objects (~ #client replicas)")
}

# #client replicas = f(var)
var_throughput_per_client_plot <- function(dir, var_name, var_label, clients_number, output_dir=file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "ops.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot() 
    p <- add_title(p, w)
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
  var_throughput_per_client_plot(experiment_dir("scalabilityclients"), "clients", "#client replicas", clients_number = identity)
}

scalabilitydbsize_throughput_per_client_plot <- function() {
  var_throughput_per_client_plot(experiment_dir("scalabilitydbsize"), "dbsize", "#objects (~ #client replicas)", clients_number = function(objects) (objects / 20))
}

var_clock_size_in_fetch_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_full(dir, var_name, "meta_size.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot()
    p <- add_title(p, w)
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
var_notifications_metadata_plot <- function(dir, var_name, var_label_axis, output_dir = file.path(dir, "comparison"), workload_pattern=".+",
                                            modes=c(), modes_labels=c()) {
  if (length(modes) == 0) {
    mode_pattern = ".+"
  } else{
    mode_pattern = pattern_alternatives(modes, exact_match=TRUE)
  }
  stats <- read_runs_params(dir, var_name, "meta_size.csv", c("BatchUpdatesNotification.meta.indep", "BatchUpdatesNotification.meta.dep.norm3"), workload_pattern=workload_pattern, mode_pattern=mode_pattern)
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, workload == w)
    p <- ggplot()
    p <- add_title(p, paste(w, "normalized notifications message metadata"))
    p <- p + labs(x=var_label_axis, y = "metadata in notification with 10 updates [bytes]")
    p <- p + THEME
    min_y <- 10^9
    max_y <- 1
    for (m in unique(workload_stats$mode)) {
      mode_stats <- subset(workload_stats, workload_stats$mode == m)
      mode_stats <- mode_stats[order(mode_stats$var), ]
      mode_stats <- normalize_batch(mode_stats, "BatchUpdatesNotification.meta", NOTIFICATIONS_EXAMPLE_BATCH_SIZE)
      p <- p + geom_line(data=mode_stats,
                         mapping=aes(y=BatchUpdatesNotification.meta.mean.scaled,
                                     x=var, color=mode, linetype=mode))
      p <- p + geom_errorbar(data=mode_stats, position=position_dodge(width = 30),
                             mapping=aes(ymax=BatchUpdatesNotification.meta.max.scaled,
                                         ymin=BatchUpdatesNotification.meta.min.scaled,
                                         x=var), color=mode, width=15)
      min_y <- min(min_y, na.omit(mode_stats$BatchUpdatesNotification.meta.min.scaled))
      max_y <- max(max_y, na.omit(mode_stats$BatchUpdatesNotification.meta.max.scaled))
    }
    #p <- p + scale_y_continuous(limits=c(0, ceiling(max_y/1000)*1000)) #, breaks=c(1, 10, 100, 1000, 10000))
    p <- p + cs_log_scale(min_y, max_y)
    if (length(modes) > 0) {
      p <- p + scale_color_discrete(name="System configuration", breaks=modes, labels=modes_labels)
      p <- p + scale_linetype_discrete(name="System configuration", breaks=modes, labels=modes_labels)
    }
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), "-", var_name, "-BatchUpdatesNotification-metadata", format_ext, sep="")), scale=1)
  }
}

scalabilitythroughput_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("scalabilitythroughput"), "opslimit", "load")
}

MODES_SCALABILITY_CLIENTS <- c("notifications-infrequent", "notifications-frequent-practi", "notifications-infrequent-dcs-9", "notifications-frequent-practi-dcs-9") # "notifications-infrequent", 
MODES_LABELS_SCALABILITY_CLIENTS <- c("SwiftCloud (3 DCs)", "Client-assigned metadata à la PRACTI/Depot (3 DCs)", "SwiftCloud (9 DCs)", "Client-assigned metadata à la PRACTI/Depot (9 DCs)")
scalabilityclients_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("scalabilityclients"), "clients", "#active client replicas",
                                  modes=MODES_SCALABILITY_CLIENTS,
                                  modes_labels=MODES_LABELS_SCALABILITY_CLIENTS)
}

scalabilityclientssmalldb_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#active client replicas",
                                  modes=MODES_SCALABILITY_CLIENTS,
                                  modes_labels=MODES_LABELS_SCALABILITY_CLIENTS)
}

scalabilitydbsize_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("scalabilitydbsize"), "dbsize", "#objects (~ #active client replicas)")
}

clientfailures_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("clientfailures"), "failures", "#unavailable client replicas",
                                  modes=c("notifications-frequent", "notifications-frequent-practi"),
                                  modes_labels=c("SwiftCloud", "Client-assigned metadata à la PRACTI/Depot"))
}

COMMIT_EXAMPLE_BATCH_SIZE <- 10
var_commit_metadata_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", c("BatchCommitUpdatesRequest.meta.indep", "BatchCommitUpdatesRequest.meta.dep.norm3"))
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, workload == w)
    p <- ggplot()
    p <- add_title(p, paste(w, "normalized commit message metadata"))
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
  var_commit_metadata_plot(experiment_dir("scalabilityclients"), "clients", "#client rpelicas")
}

scalabilityclientssmalldb_commit_metadata_plot <- function() {
  var_commit_metadata_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#client replicas")
}

scalabilitydbsize_commit_metadata_plot <- function() {
  var_commit_metadata_plot(experiment_dir("scalabilitydbsize"), "dbsize", "#objects (~ #client replicas)")
}

clientfailures_commit_metadata_plot <- function() {
  var_commit_metadata_plot(experiment_dir("clientfailures"), "failures", "#unavailable client replicas")
}

CACHE_REFRESH_BATCH_SIZE <- 10
var_cacherefresh_metadata_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", c("CacheRefreshReply.meta.indep", "CacheRefreshReply.meta.dep.norm2"))
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, workload == w)
    p <- ggplot()
    p <- add_title(p, paste(w, "normalized cache refresh message metadata"))
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
  var_cacherefresh_metadata_plot(experiment_dir("scalabilityclients"), "clients", "#client replicas")
}

scalabilityclientssmalldb_cacherefresh_metadata_plot <- function() {
  var_cacherefresh_metadata_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#client replicas")
}

scalabilitydbsize_cacherefresh_metadata_plot <- function() {
  var_cacherefresh_metadata_plot(experiment_dir("scalabilitydbsize"), "dbsize", "#objects (~ #client replicas)")
}

clientfailures_cacherefresh_metadata_plot <- function() {
  var_cacherefresh_metadata_plot(experiment_dir("clientfailures"), "failures", "#unavailable client replicas")
}

clientfailures_cacherefresh_metadata_plot <- function() {
  var_cacherefresh_metadata_plot(experiment_dir("clientfailures"), "failures", "#unavailable client replicas")
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
    p <- ggplot()
    p <- add_title(p, w)
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
  var_storage_plot(experiment_dir("clientfailures"), "failures", "#unavailable client replicas")
}

scalabilityclients_storage_plot <- function() {
  var_storage_plot(experiment_dir("scalabilityclients"), "clients", "#client replicas")
}

scalabilityclientssmalldb_storage_plot <- function() {
  var_storage_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#client replicas")
}

DB_SIZE_CHECKPOINT_EXP <- 10000
var_checkpoint_size_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", dc_table("views.table"))
  workload_stats <- subset(stats, workload == "SwiftSocial (page view counters)")
  p <- ggplot()
  p <- add_title(p, "Counter object checkpoint size (including metadata)")
  p <- p + labs(x=var_label,y = "counter object checkpoint size [bytes]")
  p <- p + THEME
  for (m in unique(workload_stats$mode)) {
    mode_stats <- subset(workload_stats, workload_stats$mode == m)
    mode_stats <- mode_stats[order(mode_stats$var), ]
    mode_stats$counter.checkpoint.mean <- mode_stats$views.table.dc.mean / DB_SIZE_CHECKPOINT_EXP
    melted <- melt(mode_stats, id.vars=c("workload", "mode", "var"))
    melted <- subset(melted, variable %in% c("counter.checkpoint.mean",  "views.table.dc.mean"))
    p <- p + geom_path(data=melted, mapping=aes_string(y="value", x="var", color="mode", linetype="variable"))
  }
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  ggsave(p, file=paste(paste(file.path(output_dir, var_name), "-counter-checkpoint-dc", format_ext, sep="")), scale=1)
}

scalabilityclientssmalldb_checkpoint_size_plot <- function() {
  var_checkpoint_size_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#client replicas")
}

clientfailures_checkpoint_size_plot <- function() {
  var_checkpoint_size_plot(experiment_dir("clientfailures"), "failures", "#unavailable client replicas")
}

client_table <- function(table) {
  return (paste(table, "client", sep="."))
}
var_client_storage_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", client_table(TABLES))
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, workload == w)
    p <- ggplot()
    p <- add_title(p, w)
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
  var_client_storage_plot(experiment_dir("clientfailures"), "failures", "#unavailable client replicas")
}

var_clock_size_in_commit_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison")) {
  stats <- read_runs_full(dir, var_name, "meta_size.csv")
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    p <- ggplot()
    p <- add_title(p, w)
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
