require("ggplot2")
require("reshape2")
require("stringr")
require("grid")
require("plyr")
library(scales)

preview <- FALSE
format_ext <- ifelse(preview, ".png", ".pdf")
add_title <- function(plot, title) {
  if (preview) {
    plot <- plot + ggtitle(label=title)
  }
  return (plot)
}

WORKLOAD_LEVELS <- c("workloada-uniform", "workloada", "workloadb-uniform", "workloadb", "workload-social", "workload-social-views-counter", "workloada-uniform-lowlocality", "workloada-lowlocality", "workloadb-uniform-lowlocality", "workloadb-lowlocality", "workload-social-lowlocality", "workload-social-views-counter-lowlocality")
WORKLOAD_LABELS <- c("YCSB A, uniform", "YCSB A", "YCSB B, uniform", "YCSB B", "SwiftSocial", "SwiftSocial w/stats", "YCSB A, uniform, low locality", "YCSB A, low locality", "YCSB B, uniform, low locality", "YCSB B, low locality", "SwiftSocial, low locality", "SwiftSocial w/view counters, low locality")
PURE_MODE_LEVELS <- c("notifications-frequent", "notifications-veryfrequent",
                      "notifications-frequent-no-pruning",  "notifications-frequent-practi",
                      "notifications-frequent-practi-no-deltas", "notifications-frequent-bloated-counters",
                      "notifications-infrequent", "notifications-infrequent-practi",
                      "notifications-infrequent-practi-no-deltas", "notifications-infrequent-bloated-counters",
                      "no-caching", "refresh-frequent", "refresh-frequent-no-pruning", "refresh-infrequent",
                      "refresh-infrequent-bloated-counters", "refresh-infrequent-no-pruning",
                      "refresh-infrequent-no-pruning-bloated-counters", "no-caching-no-k-stability",
                      "notifications-frequent-no-k-stability")
MODE_LEVELS <- PURE_MODE_LEVELS
BASIC_MODES <- c("no-caching", "notifications-veryfrequent", "notifications-frequent", "notifications-infrequent")
BASIC_MODES_COLORS <- c("no-caching" = "black", "notifications-veryfrequent" = "#E69F00",
                  "notifications-frequent" = "#0072B2", "notifications-infrequent" = "#009E73")
MODES_COLORS <- c(BASIC_MODES_COLORS, "notifications-frequent-practi"="#D55E00", "notifications-infrequent-practi"="#D55E00",
                  "notifications-infrequent-practi-no-deltas"="#CC79A7", "notifications-frequent-no-pruning"="#CC79A7",
                  "notifications-infrequent-no-pruning"="#CC79A7", "refresh-infrequent-bloated-counters" = "black",
                  "notifications-frequent-idempotence-guard"="#56B4E9")
BASIC_MODES_FILLS <- c("no-caching" = "white", "notifications-veryfrequent" = "#E69F00",
                        "notifications-frequent" = "#0072B2", "notifications-infrequent" = "#009E73")
# more color-blindness-friendly colors:  "#56B4E9", "#F0E442", , "#D55E00", "#CC79A7"


DCS_LEVELS <- c(1,3,6,9)
CLIENTS_LEVELS <-c(500,1000,1500,2000,2500)
# TODO: use cartesian product
#for (cc in paste("clients", seq(500, 2500, by=500), sep="-")) {
#  MODE_LEVELS <- union(MODE_LEVELS, paste(PURE_MODE_LEVELS, cc, sep="-"))
#}
#DCED_MODE_LEVELS <- rep(MODE_LEVELS, 1)
#for (dd in paste("dcs", c(1, 3, 6, 9), sep="-")) {
#  MODE_LEVELS <- union(MODE_LEVELS, paste(DCED_MODE_LEVELS, dd, sep="-"))
#}
MODE_LABELS <- MODE_LEVELS #TODO
#MODE_LABELS <- c("mutable cache with 1 notification per sec", "mutable cache with 1 notification per sec" + no pruning", "mutable cache + 1 notification/s + PRACTI metadata", "mutable cache + 1 notification/10s", "mutable cache + 1 notification/10s + PRACTI metadata", "no cache replica", "mutable cache + 1 refresh/s", "mutable cache + 1 refresh/s + no pruning", "mutable cache + 1 refresh/10s")

THEME <- theme_bw(base_size=9, base_family="Helvetica")
THEME <- THEME + theme(panel.border = element_blank(), plot.background = element_blank(),
                        #panel.grid.major = element_line(size = .3, color = "grey"),
                        axis.line = element_line(size=.4, color = "black"),
                        legend.position='bottom', legend.direction='vertical',
                        legend.box='horizontal', legend.key=element_blank(),
                        strip.background= element_rect(fill = 'white', colour = 'black'))

# computer-scientish log. scale
add_cs_log_scale <- function(p, min_value=10**0, max_value=10**6) {
  # TODO: we should use log2, but it's a bit more difficult with the newest version of ggplot2
  breaks <- 10**(floor(log10(max(min(min_value, max_value), 1))):ceiling(log10(max(max_value, min_value, 1))))
  labeler <- function(value) {
    if (value < 1000) {
      return (value)
    }
    if (value < 1000000) {
      return (paste(value/1000, "K", sep=""))
    }
    if (value < 10**9) {
      return (paste(value/10**6, "M", sep=""))
    }
    return (paste(value/10**9, "G", sep=""))
  }
  labels <- lapply(breaks, labeler)
  return (p + coord_cartesian(ylim=c(min(breaks), max(breaks))) + scale_y_log10(breaks = breaks, labels=labels))
}

TOP_DIR <- "~/Dropbox/INRIA/results/"
experiment_dir <- function(experiment) {
  return (file.path(TOP_DIR, experiment, "processed"))  
}

pattern_alternatives <- function(alternatives, exact_match = FALSE) {
  first <- TRUE
  exact_match_open <- ifelse(exact_match, "^", "")
  exact_match_end <- ifelse(exact_match, "$", "")
  pattern <- "("
  for (s in alternatives) {
    sep = ifelse(first, "", "|")
    pattern <- paste(pattern, paste("(", exact_match_open, s, exact_match_end, ")", sep=""), sep=sep)
    first <- FALSE
  }
  return (paste(pattern, ")", sep=""))
}

DEFAULT_DCS <- 3
DEFAULT_CLIENTS <- 1000
decode_filename <- function(file, var_name, suffix) {
  REGEX <- paste("(",pattern_alternatives(WORKLOAD_LEVELS),")-mode-(", pattern_alternatives(MODE_LEVELS), ")-", "(clients-(", pattern_alternatives(CLIENTS_LEVELS),")-)?(dcs-(", pattern_alternatives(DCS_LEVELS),")-)?", var_name, "-([0-9.]+)-", suffix, sep="")
  WORKLOAD_IDX <- 2
  MODE_IDX <- WORKLOAD_IDX + length(WORKLOAD_LEVELS) + 2
  CLIENTS_IDX <- MODE_IDX + length(MODE_LEVELS) + 3
  DCS_IDX <- CLIENTS_IDX + length(CLIENTS_LEVELS) + 3
  VAR_IDX <- DCS_IDX + length(DCS_LEVELS) + 2
  match <- str_match(file, REGEX)
  
  if (length(match)<= 1 || is.element(c(match[WORKLOAD_IDX], match[MODE_IDX], match[VAR_IDX]), NA)) {
      stop(paste("cannot match filename", file, "with the expected pattern", REGEX))
  }
  workload <- match[WORKLOAD_IDX]
  cache_mode <- match[MODE_IDX]
  if (nchar(match[CLIENTS_IDX]) > 0) {
    clients <- as.numeric(match[CLIENTS_IDX])
  } else {
    clients <- ifelse(grepl("no-caching", cache_mode), 0, DEFAULT_CLIENTS)
  }
  if (nchar(match[DCS_IDX]) > 0) {
    dcs <- as.numeric(match[DCS_IDX])
  } else {
    dcs <- DEFAULT_DCS
  }
  var_value <- as.numeric(match[VAR_IDX])
  return (list(workload=workload, mode=cache_mode, clients=clients, dcs=dcs, var=var_value))
}

read_runs_full <- function(dir, var_name, suffix,  workload_pattern=".+", mode_pattern=".+", clients_pattern=".+", dcs_pattern=".+", files=c()) {
  full_processor <- function (type, s) {
    s$workload <- rep(type$workload, nrow(s))
    s$mode <- rep(type$mode, nrow(s))
    s$var <- rep(type$var, nrow(s))
    s$clients <- rep(type$clients, nrow(s))
    s$dcs <- rep(type$dcs, nrow(s))
    return (s)
  }
  return (read_runs_impl(dir, var_name, suffix, full_processor, workload_pattern, mode_pattern, clients_pattern, dcs_pattern, files))
}

read_runs_params <- function(dir, var_name, suffix, params = c(), workload_pattern=".+", mode_pattern=".+", clients_pattern=".+", dcs_pattern=".+", files=c()) {
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
  return (read_runs_impl(dir, var_name, suffix, params_processor, workload_pattern, mode_pattern, clients_pattern, dcs_pattern, files))
}

read_runs_errors <- function(dir, var_name, suffix, files=c()) {
  errors_processor <- function (type, s) {
    row <- data.frame(type)
    accOccurrences <- 0
    for (eachCause in s$cause) {
      occ <- subset(s, cause == eachCause)$occurences
      row[[paste("errors", eachCause, sep=".")]] <- occ
      accOccurrences <- accOccurrences + occ
    }
    row$errors.total <- accOccurrences
    return (row)
  }
  return (read_runs_impl(dir, var_name, suffix, errors_processor, ".+", ".+", ".+", ".+", files))
}

read_runs_impl <- function(dir, var_name, suffix, processor, workload_pattern, mode_pattern, clients_pattern, dcs_pattern, files) {
  if (!file.info(dir)$isdir) {
    stop(paste(dir, "is not a directory"))
  }

  if (length(files) == 0) {
    files <- ".*"
  }
  file_list <- list.files(dir, pattern=pattern_alternatives(paste(files, suffix, sep="-")), recursive=FALSE, full.names=TRUE)
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
     if (!grepl(workload_pattern, decoded$workload) | !grepl(mode_pattern, decoded$mode)
         | !grepl(clients_pattern, decoded$clients) | !grepl(dcs_pattern, decoded$dcs)) {
       next
     }
     s <- read.table(file,sep = ",",row.names=NULL, header=TRUE)
     if (length(names(s)) == 0) {
       warning(paste("ignoring empty file"), file)
       next
     }
     if ("stat_params" %in% names(s)) {
       # HOTFIX for old files generated with a typo
       s$stat_param <- s$stat_params
       s <- subset(s, select=-stat_params)
     }
     decoded$workload <- factor(decoded$workload, WORKLOAD_LEVELS, WORKLOAD_LABELS)
     decoded$mode <- factor(decoded$mode, MODE_LEVELS, MODE_LABELS)
     decoded$dcs <- factor(decoded$dcs, DCS_LEVELS, ordered=TRUE)
     decoded$clients <- factor(decoded$clients, CLIENTS_LEVELS, ordered=TRUE)
     stats <- rbind.fill(stats, processor(decoded, s))
     rm(decoded)
     rm(s)
  }
  return (stats)
}

# If var_label == NA, then load (throughput op/s) is used as a variable
var_response_time_plot <- function(dir, var_name, var_label, output_dir = file.path(dir, "comparison"),
                                   workload_pattern=".+", mode_pattern=".+", modes=c(), modes_labels=c(),
                                   clients_pattern=".+", dcs_pattern=".+",
                                   lower_quantile=70, file_suffix = "") {
  if (length(modes) > 0) {
    mode_pattern <- pattern_alternatives(modes, TRUE)
  }
  response_time_lower_quantile <- paste("response_time.q", lower_quantile, sep="")
  stats <- read_runs_params(dir, var_name, "ops.csv", workload_pattern=workload_pattern, mode_pattern=mode_pattern, clients_pattern=clients_pattern, dcs_pattern=dcs_pattern,
                            params=c("throughput", "response_time"))
  stats$response_time.q95 <- sapply(stats$response_time.q95, function(v) { (max(v, 1))})
  stats[[response_time_lower_quantile]] <- sapply(stats[[response_time_lower_quantile]], function(v) { (max(v, 1))})
  
  xvar <- "var"
  if (is.na(var_label)) {
    var_label <- "throughput [txn/s]"
    xvar <- "throughput.mean"
  }
  
  for (w in unique(stats$workload)) {
    workload_stats <- subset(stats, stats$workload == w)
    if (nrow(workload_stats) == 0) {
      next
    }
    
    p <- ggplot() 
    p <- add_title(p, w)
    p <- p + labs(x=var_label,y = "response time [ms]")
    p <- p + scale_y_log10(breaks=c(1, 10, 100, 1000), limits=c(1,10000))
    #p <- p + coord_cartesian(ylim=c(-10, 2500))
    p <- p + THEME
    workload_stats <- workload_stats[order(workload_stats$var), ]
    #m_match <- match(m, modes)
    #  if (is.na(m_match)) {
    #    m_label <- m
    #      } else {
    #        m_label <- modes_labels[m_match]
    #      }
    #      mode_stats$modeAll <- rep(paste(m_label, paste(dd, "dcs"), paste(cc, "clients"), sep=" / "), nrow(mode_stats))
    
    p <- p + geom_path(data=workload_stats,
                       mapping=aes_string(y=response_time_lower_quantile,
                                          x=xvar, group="interaction(mode, dcs, clients)",
                                          color="interaction(mode, dcs, clients)"), linetype="solid")
    p <- p + geom_point(data=workload_stats,
                        mapping=aes_string(y=response_time_lower_quantile,
                                           x=xvar, group="interaction(mode, dcs, clients)",
                                           color="interaction(mode, dcs, clients)",
                                           shape="interaction(mode, dcs, clients)"))
    
    p <- p + geom_path(data=workload_stats,
                       mapping=aes_string(y="response_time.q95",
                                          x=xvar, group="interaction(mode, dcs, clients)",
                                          color="interaction(mode, dcs, clients)"), linetype="dashed")
    p <- p + geom_point(data=workload_stats,
                        mapping=aes_string(y="response_time.q95",
                                           x=xvar, group="interaction(mode, dcs, clients)",
                                           color="interaction(mode, dcs, clients)",
                                           shape="interaction(mode, dcs, clients)"))
    p <- p + scale_color_discrete(name="System configuration")
    p <- p + scale_shape_discrete(name="System configuration")
    p <- p + guides(linetype = "legend")
    p <- p + scale_linetype_manual(name = "Response time w.r.t. access locality",
                                   values = c("solid", "dashed"),  guide="legend",
                                      #breaks = c(response_time_lower_quantile, "response_time.q95"),
                                     labels = c(paste(lower_quantile, "th percentile (expected local request)", sep=""), "95th percentile (remote request)"))
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    ggsave(p, file=paste(paste(file.path(output_dir, w), file_suffix, "-", var_name, "-response_time", format_ext, sep="")), scale=1)
  }
}

# If var_label == NA, then load (throughput op/s) is used as a variable
workloads_throughput_response_time_plot <- function(dir, files, output_dir = file.path(dir, "comparison"), modes, modes_labels, modes_colors=c(),
                                   lower_quantile=70, file_suffix = "") {
  response_time_lower_quantile <- paste("response_time.q", lower_quantile, sep="")
  stats <- read_runs_params(dir, "opslimit", "ops.csv", files=files, params=c("throughput", "response_time"))
  error_stats <- read_runs_errors(dir, "opslimit", "errors.csv", files=files)
  stats <- merge(stats, error_stats, by=c("workload", "mode", "dcs", "clients", "var"))
  #stats <- subset(stats, errors.total < 200)
  stats$response_time.q95 <- sapply(stats$response_time.q95, function(v) { (max(v, 1))})
  stats[[response_time_lower_quantile]] <- sapply(stats[[response_time_lower_quantile]], function(v) { (max(v, 1))})
  stats <- stats[order(stats$var), ]
  stats$workload_name <- factor(sapply(stats$workload, function(w) {ifelse(grepl("YCSB A", w), "A", "B")}), levels=c("A", "B"), labels=c("YCSB A (50% updates)", "YCSB B (5% updates)"))
  stats$workload_distribution <- factor(sapply(stats$workload, function(w) {ifelse(grepl("uniform", w), "uniform", "zipfian")}), levels=c("zipfian", "uniform"), labels=c("zipfian distribution", "uniform distribution"))
  # anti reshape2-hack (reshape2 is somewhat at odds with throughput.mean)
  QUANTILES_LEVELS <- c("low","high")
  stats$modeLow <- paste(stats$mode, "low", sep=".")
  stats$modeHigh <- paste(stats$mode, "high", sep=".")
  
  p <- ggplot(stats) + THEME + theme(panel.margin= unit(0.29, 'lines'), legend.key.height=unit(0.7,"line"), legend.key.width=unit(1,"line"), legend.margin=unit(-0.6,"cm"))
  p <- p + labs(x="throughput [txn/s]", y = "response time [ms]")
  #p <- p + coord_cartesian(ylim=c(0, 2500))
  #p <- p + coord_cartesian(xlim=c(0, 25000))
  #p <- p + expand_limits(x=185)
  # HACK to force X acis to start with 0 on workload A facet
  fake_stats <- data.frame(workload_name=factor("A", levels=c("A", "B"), labels=c("YCSB A (50% updates)", "YCSB B (5% updates)")), min_x = c(1300,4900))
  p <- p + geom_vline(data=fake_stats, aes(xintercept=min_x), alpha=0)
  #p <- p + expand_limits(x=1500)
  #p <- p + expand_limits(x=5000)
  p <- p + scale_x_log10(breaks=c(1250, 2500, 5000, 10000, 20000, 40000))
  p <- p + coord_cartesian(ylim=c(0.5, 3165)) + scale_y_log10(limits=c(1,3165), breaks=c(1, 10, 100, 1000))
  
  # A bunch of HACKS to obtain the legend as desired!
  p <- p + geom_path(mapping=aes_string(y=response_time_lower_quantile,
                                        x="throughput.mean", group="interaction(workload, mode, dcs, clients)",
                                        color="modeLow", linetype="modeLow"))
  p <- p + geom_point(mapping=aes_string(y=response_time_lower_quantile,
                                         x="throughput.mean", group="interaction(workload, mode, dcs, clients)",
                                         color="modeLow", shape="modeLow", size="modeLow"))
  p <- p + geom_path(mapping=aes_string(y="response_time.q95",
                                        x="throughput.mean", group="interaction(workload, mode, dcs, clients)",
                                        color="modeHigh", linetype="modeHigh"), size = 0.2)
  p <- p + geom_point(mapping=aes_string(y="response_time.q95",
                                         x="throughput.mean", group="interaction(workload, mode, dcs, clients)",
                                         color="modeHigh", shape="modeHigh", size="modeHigh"))
  
  # Shared legend
  name <- "Configuration × response time percentile"
  breaks <- c(paste(modes, "low", sep="."), paste(modes, "high", sep="."))
  labels <- c(paste(modes_labels, "70th percentile (exp. local)", sep=", "), paste(modes_labels, "95th percentile (remote)", sep=", "))
  attributes(modes_colors) <- NULL

  # Problem: clearly, scale_*_manual's values= are not given in the order of breaks= or labels=.
  # MAGIC HACK: permutation. I am too tired to do it in R-idiomatic way, i.e., Google for several hours or read R code.
  MAGIC_PERMUTATION <- c(4, 1, 6, 3, 5, 2)
  
  colors <- rep(modes_colors, 2)[MAGIC_PERMUTATION]
  shapes <- c(15, 16, 17, 0, 1, 2)[MAGIC_PERMUTATION]
  sizes <- c(2, 2, 2, 1, 1, 1)[MAGIC_PERMUTATION]
  linetypes <- c(1, 1, 1, 2, 2, 2)[MAGIC_PERMUTATION]
  p <- p + scale_color_manual(name=name, breaks=breaks, labels=labels, values=colors)
  p <- p + scale_shape_manual(name=name, breaks=breaks, labels=labels, values=shapes)
  p <- p + scale_linetype_manual(name=name, breaks=breaks, labels=labels, values=linetypes)
  p <- p + scale_size_manual(name=name, breaks=breaks, labels=labels, values=sizes)
  p <- p + facet_grid(workload_distribution ~ workload_name, scales="free_x", space="free_x")
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  ggsave(p, file=paste(paste(file.path(output_dir, file_suffix),"-workload_throughput_response_time", format_ext, sep="")),
         width=3.6, height=3.6)
}

ycsb_workloads_throughput_response_time_plot <-function() {
  workloads_throughput_response_time_plot("~/Dropbox/INRIA/results/scalabilitythroughput/processed/",
                                          # MANUALLY FILTERED: w.r.t.  *-errors.csv and to reduce overlapping points
                                          files = c(#"workloada-mode-no-caching-clients-1000-opslimit.*",
                                                    paste("workloada-mode-no-caching-clients-1000-opslimit",
                                                          c(1400, 1800, 2200, 2600, 4000, 4500),sep="-"),
                                                    #"workloada-mode-notifications-veryfrequent-clients-500-opslimit.*",
                                                    paste("workloada-mode-notifications-veryfrequent-clients-500-opslimit",
                                                          c(1400,1800, 2200, 2600, 3400, 4000),sep="-"),
                                                    #"workloada-mode-notifications-frequent-clients-500-opslimit.*",
                                                    paste("workloada-mode-notifications-frequent-clients-500-opslimit",
                                                          c(1400,1800, 2200, 2600, 3000, 3400),sep="-"),
                                                    #"workloada-uniform-mode-no-caching-clients-1000-opslimit.*",
                                                    paste("workloada-uniform-mode-no-caching-clients-1000-opslimit",
                                                          c(2000, 2500, 3000, 3500, 4000, 4500),sep="-"),
                                                    #"workloada-uniform-mode-notifications-frequent-clients-500-opslimit.*",
                                                    paste("workloada-uniform-mode-notifications-frequent-clients-500-opslimit",
                                                          c(2000, 2500, 3000, 3500, 4000, 4500, 5000),sep="-"),
                                                    #"workloada-uniform-mode-notifications-veryfrequent-clients-500-opslimit.*",
                                                    paste("workloada-uniform-mode-notifications-veryfrequent-clients-500-opslimit",
                                                          c(2000, 2500, 3000, 3500, 4500, 5000),sep="-"),
                                                    #"workloadb-mode-no-caching-clients-1000-opslimit.*",
                                                    paste("workloadb-mode-no-caching-clients-1000-opslimit",
                                                          c(4000, 6000, 8000, 12000),sep="-"),
                                                    #"workloadb-mode-notifications-veryfrequent-clients-1000-opslimit.*",
                                                    paste("workloadb-mode-notifications-veryfrequent-clients-1000-opslimit",
                                                          c(4000, 6000, 8000, 10000, 12000, 14000, 16000),sep="-"),
                                                    #"workloadb-mode-notifications-frequent-clients-1000-opslimit.*",
                                                    paste("workloadb-mode-notifications-frequent-clients-1000-opslimit",
                                                          c(4000, 6000, 8000, 10000, 12000, 14000, 16000),sep="-"),
                                                    #"workloadb-uniform-mode-no-caching-clients-1000-opslimit.*",
                                                    paste("workloadb-uniform-mode-no-caching-clients-1000-opslimit",
                                                          c(4000, 6000, 8000, 10000, 12000),sep="-"),
                                                    #"workloadb-uniform-mode-notifications-veryfrequent-clients-1000-opslimit.*",
                                                    paste("workloadb-uniform-mode-notifications-veryfrequent-clients-1000-opslimit",
                                                          c(# MISSING EXECUTIONS? 4000, 6000, 8000, 10000,
                                                            12000, 16000,20000, 24000, 28000),sep="-"),
                                                    #"workloadb-uniform-mode-notifications-frequent-clients-1000-opslimit.*",
                                                    paste("workloadb-uniform-mode-notifications-frequent-clients-1000-opslimit",
                                                          c(4000, 6000, 8000, 12000, 16000, 20000, 24000, 28000),sep="-")),
                                          file_suffix = "YCSB",
                                          modes=c("no-caching", "notifications-veryfrequent", "notifications-frequent"),
                                          modes_colors=c(MODES_COLORS["no-caching"], MODES_COLORS["notifications-veryfrequent"], MODES_COLORS["notifications-frequent"]),
                                          modes_labels=c("server replicas only", "client replicas - high refresh rate", 
                                                         "client replicas - medium refresh rate"))
}

workloads_modes_max_throughput_plot <- function(dir, var_name, files, output_dir = file.path(dir, "comparison"),
                                                lower_quantile_threshold = 10, high_quantile_threshold=5000,
                                                modes=c(), modes_labels=c(), modes_colors=c(), modes_fills=c()) {
  stats <- read_runs_params(dir, var_name, "ops.csv", files=files)
  stats$locality <- factor(sapply(stats$workload, function(w) {ifelse(grepl("locality", w), "low", "high")}), levels=c("high", "low"), labels=c("High locality workload", "Low locality workload"))
  stats <- subset(stats, response_time.q95 <= high_quantile_threshold)
  stats <- subset(stats, grepl("no-caching", mode) | response_time.q20 < lower_quantile_threshold)
  stats <- subset(stats, grepl("no-caching", mode) | grepl("locality", workload) | response_time.q70 < lower_quantile_threshold)
  # TODO threshold on errors?
  melted_stats <- melt(stats, id.vars=c("workload", "locality", "mode", "var"), measure.vars=c("throughput.mean"))
  mode_max_stats <- dcast(melted_stats, workload + locality + mode ~ variable, max, fill=0)
  deltas_stats <- dcast(melt(mode_max_stats, id.vars=c("workload", "locality", "mode")), workload + locality ~ variable,
                  function(v) { ((as.numeric(v[1])/as.numeric(v[2]) - 1) * 100)} , fill=0)
  max_stats <- dcast(melted_stats, workload + locality ~ variable, max, fill=0)
  mode_delta_stats <- merge(max_stats, deltas_stats, by=c("workload", "locality"), suffixes=c(".max", ".delta"))
  mode_delta_stats$delta_text <- sapply(mode_delta_stats$throughput.mean.delta, function(delta) {
    paste(ifelse(delta > 0, "+", ""), round(delta), "%", sep="")
  })

  mode_max_stats$workload_stripped <- sapply(mode_max_stats$workload, function(w) {(sub(", low locality", "", w))})
  mode_delta_stats$workload_stripped <- sapply(mode_delta_stats$workload, function(w) {(sub(", low locality", "", w))})

  p <- ggplot() + THEME + theme(axis.text.x = element_text(angle = 45, hjust = 1, vjust=1.11), axis.ticks.x =element_blank(),
                                panel.grid.major.x = element_blank(), legend.key.height=unit(0.7,"line"),
                                legend.key.width=unit(0.7, "line"))
  p <- p + coord_cartesian(ylim=c(0, ceiling(max(mode_max_stats$throughput.mean)/5000)*5000))
  p <- p + scale_y_continuous(limits=c(0, ceiling(max(mode_max_stats$throughput.mean)/5000)*5000))
  #p <- p + scale_x_discrete(breaks=CLIENTS_LEVELS, limits=CLIENTS_LEVELS)
  p <- p + labs(x="workload",y = "max. throughput [txn/s]")
  p <- p + geom_bar(data=mode_max_stats,
                     mapping=aes(y=throughput.mean, x=workload_stripped, group=mode, fill=mode),
                    color="black", position="dodge", stat="identity", width=0.8)
  p <- p + geom_text(data=mode_delta_stats, aes(label=delta_text, x = workload_stripped, y=throughput.mean.max + 1200),
                     size = 2.5, color=BASIC_MODES_FILLS["notifications-frequent"], hjust=0.54)
  #p <- p + coord_flip()
  p <- p + facet_wrap(~ locality, scales="free_x")

  if (length(modes_colors) > 0) {
    p <- p + scale_color_manual(values=modes_colors, breaks=modes, labels=modes_labels)
  } else {
    p <- p + scale_color_discrete(breaks=modes, labels=modes_labels)
  }
  if (length(modes_fills) > 0) {
    p <- p + scale_fill_manual(values=modes_fills, breaks=modes, labels=modes_labels)
  } else {
    p <- p + scale_fill_discrete(breaks=modes, labels=modes_labels)
  }
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  p <- p + theme(legend.position = c(0.755, 0.808), legend.background=element_blank(), #element_rect(fill="white",colour="black"),
                 legend.title=element_blank(), axis.title.x = element_blank()) 
  ggsave(p, file=paste(paste(file.path(output_dir, "workloads_modes_max_throughput"), format_ext, sep="")), width=3.6, height=2.4)
}

scalabilitythroughput_3dcs_workloads_modes_max_throughput_plot <- function() {
  workloads_modes_max_throughput_plot("~/Dropbox/INRIA/results/scalabilitythroughput/processed/",
                                      "opslimit", files=c(
                                        "workloada-mode-no-caching-clients-1000-opslimit.*",
                                        paste("workloada-mode-no-caching-clients-1000-opslimit",
                                              c(1400, 1800, 2200, 2600, 3400, 4000, 4500),sep="-"),
                                        #"workloada-mode-notifications-infrequent-clients-500-opslimit.*",
                                        #"workloada-mode-notifications-veryfrequent-clients-500-opslimit.*",
                                        #paste("workloada-mode-notifications-veryfrequent-clients-500-opslimit",
                                        #      c(1400, 1800, 2200, 2600, 3400, 4000),sep="-"),
                                        #"workloada-mode-notifications-frequent-clients-500-opslimit.*",
                                        paste("workloada-mode-notifications-frequent-clients-500-opslimit",
                                              c(1400, 1800, 2200, 2600, 3000, 3400),sep="-"),
                                        #"workloada-uniform-mode-no-caching-clients-1000-opslimit.*",
                                        paste("workloada-uniform-mode-no-caching-clients-1000-opslimit",
                                              c(2000, 2500, 3000, 3500, 4000, 4500),sep="-"),
                                        #"workloada-uniform-mode-notifications-frequent-clients-500-opslimit.*",
                                        paste("workloada-uniform-mode-notifications-frequent-clients-500-opslimit",
                                              c(2000, 2500, 3000, 3500, 4000, 4500, 5000),sep="-"),
                                        #"workloada-uniform-mode-notifications-infrequent-clients-500-opslimit.*",
                                        #"workloada-uniform-mode-notifications-veryfrequent-clients-500-opslimit.*",
                                        #paste("workloada-uniform-mode-notifications-veryfrequent-clients-500-opslimit",
                                        #      c(2000, 2500, 3000, 3500, 4500, 5000),sep="-"),
                                        #"workloadb-mode-no-caching-clients-1000-opslimit.*",
                                        paste("workloadb-mode-no-caching-clients-1000-opslimit",
                                              c(4000, 6000, 8000, 12000),sep="-"),
                                        #"workloadb-mode-notifications-infrequent-clients-1000-opslimit.*",
                                        #"workloadb-mode-notifications-veryfrequent-clients-1000-opslimit.*",
                                        #paste("workloadb-mode-notifications-veryfrequent-clients-1000-opslimit",
                                        #      c(4000, 6000, 8000, 10000, 12000, 14000, 16000),sep="-"),
                                        #"workloadb-mode-notifications-frequent-clients-1000-opslimit.*",
                                        paste("workloadb-mode-notifications-frequent-clients-1000-opslimit",
                                              c(4000, 6000, 8000, 10000, 12000, 14000, 16000),sep="-"),
                                        #"workloadb-uniform-mode-no-caching-clients-1000-opslimit.*",
                                        paste("workloadb-uniform-mode-no-caching-clients-1000-opslimit",
                                              c(4000, 6000, 8000, 10000, 12000),sep="-"),
                                        #"workloadb-uniform-mode-notifications-infrequent-clients-1000-opslimit.*",
                                        #"workloadb-uniform-mode-notifications-veryfrequent-clients-1000-opslimit.*",
                                        #paste("workloadb-uniform-mode-notifications-veryfrequent-clients-1000-opslimit",
                                        #      c(# MISSING EXECUTIONS? 4000, 6000, 8000, 10000,
                                        #        12000, 16000,20000, 24000, 28000),sep="-"),
                                        #"workloadb-uniform-mode-notifications-frequent-clients-1000-opslimit.*",
                                        paste("workloadb-uniform-mode-notifications-frequent-clients-1000-opslimit",
                                              c(4000, 6000, 8000, 12000, 16000, 20000, 24000, 28000),sep="-"),
                                        "workload-social-mode-no-caching-clients-1000-opslimit.*",
                                        "workload-social-mode-notifications-frequent-clients-1000-opslimit.*",
                                        
                                        "workloada-lowlocality-mode-no-caching-clients-1000-opslimit.*",
                                        "workloada-lowlocality-mode-notifications-frequent-clients-500-opslimit.*",
                                        "workloada-uniform-lowlocality-mode-no-caching-clients-1000-opslimit.*",
                                        "workloada-uniform-lowlocality-mode-notifications-frequent-clients-500-opslimit.*",
                                        "workloadb-lowlocality-mode-no-caching-clients-1000-opslimit.*",
                                        "workloadb-lowlocality-mode-notifications-frequent-clients-1000-opslimit.*",
                                        "workloadb-uniform-lowlocality-mode-no-caching-clients-1000-opslimit.*",
                                        "workloadb-uniform-lowlocality-mode-notifications-frequent-clients-1000-opslimit.*",
                                        "workload-social-lowlocality-mode-no-caching-clients-1000-opslimit.*",
                                        "workload-social-lowlocality-mode-notifications-frequent-clients-1000-opslimit.*"
                                        ),
                                        modes=c("notifications-frequent", "no-caching"),
                                        modes_fills=BASIC_MODES_FILLS,
                                        modes_labels=c("SwiftCloud w/client replicas", "server replicas only")
                                      )
}

clients_max_throughput_plot <- function(dir, var_name, files, output_dir = file.path(dir, "comparison"),
                                        modes, modes_labels, modes_colors=c(),
                                        lower_quantile_threshold = 10, high_quantile_threshold=5000,
                                        errors_threshold = 10000, show_server_replicas=F) {
  stats <- read_runs_params(dir, var_name, "ops.csv", files=files)
  error_stats <- read_runs_errors(dir, "opslimit", "errors.csv", files=files)
  stats <- merge(stats, error_stats, by=c("workload", "mode", "dcs", "clients", "var"))
  stats <- subset(stats, errors.total < errors_threshold)
  stats <- subset(stats, response_time.q95 <= high_quantile_threshold)
  stats <- subset(stats, grepl("no-caching", mode) | response_time.q20 < lower_quantile_threshold)
  stats <- subset(stats, grepl("no-caching", mode) | grepl("locality", workload) | response_time.q70 < lower_quantile_threshold)

  melted_stats <- melt(stats, id.vars=c("workload", "mode", "dcs", "clients", "var"), measure.vars=c("throughput.mean"))
  max_stats <- dcast(melted_stats, workload + mode + dcs + clients ~ variable, max, fill=0, subset=.(mode != "no-caching"))
  max_stats <- transform(max_stats, clients=as.numeric(levels(max_stats$clients)[max_stats$clients]))

  max_no_caching_stats <- dcast(melted_stats, workload + mode + dcs ~ variable, max, fill=0, subset=.(mode == "no-caching"))

  # TODO: there is probably a simpler way to do compute limited_clients_throughput_stats.
  max_any_mode_stats <- dcast(melted_stats, workload + dcs + clients ~ variable, max, fill=0, subset=.(mode != "no-caching"))
  max_any_mode_stats <- transform(max_any_mode_stats, clients=as.numeric(levels(max_any_mode_stats$clients)[max_any_mode_stats$clients]))
  max_clients_throughput_stats <- dcast(melt(max_any_mode_stats, id.vars=c("workload", "dcs")), workload + dcs ~ variable, max, fill=500)
  limited_clients_throughput_stats <- subset(max_clients_throughput_stats, clients != max(max_clients_throughput_stats$clients), select=c(workload,dcs,clients))
  limited_clients_throughput_stats <- merge(limited_clients_throughput_stats, max_any_mode_stats, by=c("workload", "dcs", "clients"))
  limited_clients_throughput_stats$label <- rep("/ limit", nrow(limited_clients_throughput_stats))
  
  if (nrow(max_stats) > 0) {
    #     throughput_stats <- transform(throughput_stats, clients=as.numeric(levels(clients))[clients],
    #                                   dcs=as.numeric(levels(dcs))[dcs])
    #     throughput_stats <- throughput_stats[order(throughput_stats$clients), ]
    #     throughput_stats$line_thickness <- rep(0.0001, nrow(throughput_stats))
    p <- ggplot() + THEME + theme(legend.title = element_blank(), legend.box="vertical",
                                  legend.text = element_text(size=6.5), legend.key.height=unit(0.7,"line"),
                                  legend.key.width=unit(1,"line"),  panel.margin=unit(0.5, "line"))
                                  #axis.text.x = element_text(angle = 45, hjust = 1, vjust=1.11))
    p <- p + coord_cartesian(ylim=c(0, ceiling(max(max_stats$throughput.mean)/5000)*5000),
                             xlim=c(350, 2650))
    p <- p + scale_x_continuous(breaks=c(500, 1500, 2500))
    #p <- p + scale_x_discrete(breaks=CLIENTS_LEVELS, limits=CLIENTS_LEVELS)
    p <- p + labs(x="#client replicas",y = "max. throughput [txn/s]")
    if (show_server_replicas) {
      p <- p + geom_hline(data=max_no_caching_stats, mapping=aes(yintercept=throughput.mean, color=mode,
                                                                size=dcs, shape=mode, linetype=mode))
    }
    p <- p + geom_path(data=max_stats,
                       mapping=aes(y=throughput.mean, x=clients, group=interaction(workload,mode,dcs),
                                   color=mode, size=dcs))
    p <- p + geom_point(data=subset(max_stats, dcs==1),
                       mapping=aes(y=throughput.mean, x=clients, color=mode, shape=mode), size=1)
    p <- p + geom_point(data=subset(max_stats, dcs==3),
                        mapping=aes(y=throughput.mean, x=clients, color=mode, shape=mode), size=1.7)
    #p <- p + geom_segment(data=limited_clients_throughput_stats,
    #                      mapping=aes(x=clients+80, xend=clients + 80,
    #                                  y=throughput.mean-1000, yend=throughput.mean+1000), color="black",
    #                      linestyle=2)
     p <- p + geom_text(data=limited_clients_throughput_stats,
                        mapping=aes(x=clients+120, y=throughput.mean, label=label),
                        size = 2.2, hjust=0)
    p <- p + facet_wrap(~workload)
    if (length(modes_colors) > 0) {
      p <- p + scale_color_manual(values=modes_colors, breaks=modes, labels=modes_labels)
      if (show_server_replicas) {
        p <- p + scale_linetype_manual(values=c(2), breaks=modes, labels=modes_labels)
      }
    } else {
      p <- p + scale_color_discrete(breaks=modes, labels=modes_labels)
    }
    p <- p + scale_shape_discrete(breaks=modes, labels=modes_labels)
    dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
    p <- p + theme(legend.position = "bottom", legend.box="horizontal", legend.background=element_blank(), #legend.background=element_rect(fill="white",colour="black"),
          legend.title=element_blank(),  legend.margin =unit(-2.1, "lines"))
    p <- p + scale_size_manual(values=c(0.3, 0.75), breaks=c(1, 3), labels=c("1 DC replica", "3 DC replicas"))
    ggsave(p, file=paste(paste(file.path(output_dir, "clients-max_throughput"), format_ext, sep="")),
           width=3.6, height=2.4)
  }
}

scalabilitythroughputclients_max_throughput_plot <- function() {
  clients_max_throughput_plot("~/Dropbox/INRIA/results/scalabilitythroughput/processed", "opslimit",
                              files=c("workloada-uniform-mode-no-caching-clients-.*",
                                      "workloada-uniform-mode-notifications-infrequent-clients-.*",
                                      "workloada-uniform-mode-notifications-frequent-clients-.*",
                                      "workloadb-uniform-mode-no-caching-clients-.*",
                                      "workloadb-uniform-mode-notifications-infrequent-clients-.*",
                                      "workloadb-uniform-mode-notifications-frequent-clients-.*",
                                      "workload-social-mode-no-caching-clients-.*",
                                      "workload-social-mode-notifications-infrequent-clients-.*",
                                      "workload-social-mode-notifications-frequent-clients-.*"),
                              modes=BASIC_MODES,
                              modes_labels=c("reference: server replicas only", "high refresh rate of client replicas",
                                             "medium refresh rate of client replicas", "low refresh rate of client replicas"),
                              modes_colors=BASIC_MODES_COLORS)
}

scalabilitythroughput_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA)
}

scalabilitythroughputbest_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilitythroughput"),
                         var_name="opslimit", var_label=NA,
                         workload_pattern=pattern_alternatives(c("workloada", "workloada-uniform")),
                         mode_pattern=pattern_alternatives(c("no-caching", "notifications-(in|very)?frequent")),
                         clients_pattern="500", dcs_pattern="3", lower_quantile=70)
  var_response_time_plot(experiment_dir("scalabilitythroughput"),
                         var_name="opslimit", var_label=NA,
                         workload_pattern=pattern_alternatives(c("workloadb", "workloadb-uniform", "workload-social")),
                         mode_pattern=pattern_alternatives(c("no-caching", "notifications-(in|very)?frequent")),
                         clients_pattern="1000", dcs_pattern="3", lower_quantile=70)
}

scalabilitythroughputclients1dc_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA, workload_pattern=pattern_alternatives(c("workloada", "workloada-uniform")), mode_pattern=pattern_alternatives(c("no-caching", "notifications-(in|very)?frequent")), dcs_pattern="1", lower_quantile=70, file_suffix="-dcs-1")
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA, workload_pattern=pattern_alternatives(c("workloadb", "workloadb-uniform", "workload-social")), mode_pattern=pattern_alternatives(c("no-caching", "notifications-(in|very)?frequent")), dcs_pattern="1", lower_quantile=70, file_suffix="-dcs-1")
}

scalabilitythroughputclients3dcs_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA, workload_pattern=pattern_alternatives(c("workloada", "workloada-uniform")), mode_pattern=pattern_alternatives(c("no-caching", "notifications-(in|very)?frequent")), dcs_pattern="3", lower_quantile=70, file_suffix="-dcs-3")
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA, workload_pattern=pattern_alternatives(c("workloadb", "workloadb-uniform", "workload-social")), mode_pattern=pattern_alternatives(c("no-caching", "notifications-(in|very)?frequent")), dcs_pattern="3", lower_quantile=70, file_suffix="-dcs-3")
}


scalabilitythroughputlowlocality_response_time_plot <- function() {
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA, workload_pattern="workloada.+lowlocality", mode_pattern=pattern_alternatives(c("no-caching", "notifications-(in)?frequent-clients-(500|1000|1500)")), lower_quantile=20)
  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA, workload_pattern="(workloadb|social).+lowlocality", mode_pattern=pattern_alternatives(c("no-caching", "notifications-(in)?frequent-clients-(500|1000|1500)")), lower_quantile=20)
}

#scalabilitythroughputclients_response_time_plot <- function() {
#  clients <- seq(500, 2000, by=500)
#  var_response_time_plot(experiment_dir("scalabilitythroughput"), var_name="opslimit", var_label=NA,
#                         modes=c("no-caching-clients-1000", paste("notifications-infrequent-clients-", clients, sep=""), paste("notifications-frequent-clients-", clients, sep="")),
#                         modes_labels=c("no client replicas", paste(clients, "client replicas (1 notification / 10s)"), paste(clients, "client replicas (1 notification / 1s)")),
#                         workload_pattern=pattern_alternatives(c("workloada-uniform", "workloadb-uniform", "workload-social"), TRUE),
#                         file_suffix="-clients")
#}

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

OPERATION_TYPES <- c("read", "update", "READ", "FRIEND", "SEE_FRIENDS", "POST", "STATUS")
RESPONSE_TIME_OPERATION_TYPES <- paste("response_time", OPERATION_TYPES, sep="_")
OPERATION_TYPES_LABELS <- c("read", "update", "read wall", "add friend", "see friends", "post message", "post status")

CDFS_RESPONSE_TIME_CUTOFF <- 270
RTT <- 60

cdfs_locality_plot <- function(dir, var_name, files,
                               output_dir = file.path(dir, "comparison"), output_suffix) {
  stats <- read_runs_full(dir, var_name, "ops.csv", files=files)
  
  p <- ggplot()
  p <- p + labs(x="operation response time [ms]",y = "CDF for all sessions")
  p <- p + coord_cartesian(xlim = c(-1, CDFS_RESPONSE_TIME_CUTOFF), ylim = c(0, 1.01))
  p <- p + scale_y_continuous(labels = percent)
  p <- p + THEME + theme(legend.direction='vertical',
                         panel.margin= unit(0.35, 'lines'), legend.title = element_blank(), legend.key.height=unit(0.8,"line"),
                         legend.margin=unit(0, 'lines'), panel.grid = element_blank())
  #p <- add_title(p, paste(w, m, dd, "DCs", cc, "clients", v, var_name))
  p <- p + scale_linetype_discrete(name = "",
                                   breaks = RESPONSE_TIME_OPERATION_TYPES,
                                   labels = OPERATION_TYPES_LABELS)
  p <- p + scale_color_discrete(name = "",
                                breaks = RESPONSE_TIME_OPERATION_TYPES,
                                labels = OPERATION_TYPES_LABELS)
  
  cols <-  names(stats)
  permilles <- subset(stats, stat == "permille")
  LOCALITY_LEVELS <- c("High locality workload", "Low locality workload")
  LOCALITY_LABELS <- c("High locality load", "Low locality load")
  locality <- function(w) {
    if (grepl("locality", w)) {
      return ("Low locality workload")
    }
    return ("High locality workload")
  }
  permilles$locality <- factor(lapply(permilles$workload, locality), levels=LOCALITY_LEVELS, labels=LOCALITY_LABELS, ordered=T)
  locality_ann <- data.frame()
  rtts_ann <- data.frame()
  rtt_rect_ann <- data.frame()
  for (l in unique(permilles$locality)) {
    for (m in unique(permilles$mode)) {
      if (grepl("low|40", l, ignore.case=T)) {
        expected_locality <- 0.4
      } else {
        expected_locality <- 0.8
      }
      if (grepl("no-caching", m)) {
        label <- ""
        rtt_rect_ann <- rbind(rtt_rect_ann, data.frame(locality=l, mode=m, xmin = RTT, xmax = CDFS_RESPONSE_TIME_CUTOFF, ymin = 0, ymax = 1.01, alpha=.1))
        rtt_rect_ann <- rbind(rtt_rect_ann, data.frame(locality=l, mode=m, xmin = 2*RTT, xmax = CDFS_RESPONSE_TIME_CUTOFF, ymin = 0, ymax = 1.01, alpha=.17))
      } else {
        label <-"locality potential"
        rtt_rect_ann <- rbind(rtt_rect_ann, data.frame(locality=l, mode=m, xmin = RTT, xmax = CDFS_RESPONSE_TIME_CUTOFF, ymin = 0, ymax = 1.01, alpha=0.1))
      }
      locality_ann <- rbind(locality_ann,
                            data.frame(locality=l, mode=m, expected_locality=expected_locality, label=label))
      
      if (expected_locality == 0.8) {
        if (grepl("no-caching", m)) {
          rtts_ann <- rbind(rtts_ann, data.frame(locality=l, mode=m, x=RTT*(0.5+0:2), y=rep(0.411, 3), label=c("0 RTT", "1 RTT", "2 RTT")))
        } else {
          rtts_ann <- rbind(rtts_ann, data.frame(locality=l, mode=m, x=RTT*(0.5+0:1), y=rep(0.411, 2), label=c("0 RTT", "1 RTT")))
        }
      }
    }
  } 
  p <- p + geom_rect(data=locality_ann, aes(ymin = expected_locality-0.075, ymax=expected_locality+0.075), xmin=0, xmax=CDF_RESPONSE_TIME_CUTOFF, fill = "gray", alpha=0.75)
  p <- p + geom_rect(data=rtt_rect_ann, aes(ymin=ymin, ymax=ymax, xmin=xmin, xmax=xmax), alpha = 0.1)
  
  melted <- melt(permilles, id.vars=c("locality", "mode", "dcs", "clients", "var","stat_param"), cols[grepl("response_time_", cols)], na.rm=TRUE)
  melted <- transform(melted, variable = factor(variable, levels= RESPONSE_TIME_OPERATION_TYPES))
  filling <- subset(melted, stat_param==1 & value < CDFS_RESPONSE_TIME_CUTOFF)
  filling$value <- rep(CDFS_RESPONSE_TIME_CUTOFF, nrow(filling))
  melted <- rbind(melted, filling)
  p <- p + geom_line(data=melted,
                     mapping=aes(y=stat_param, x=value, group=interaction(locality, mode, dcs, clients, variable), colour=variable, linetype=variable))
  p <- p + geom_text(data=locality_ann, aes(y=expected_locality, label=label), x=CDFS_RESPONSE_TIME_CUTOFF*0.66,  size=2.6, show_guide=F)
  p <- p + geom_text(data=rtts_ann, aes(y=y, x=x, label=label), angle=90, size =2.6,hjust=1)
  
  mode_labeller <- function(value) {
    if (grepl("notifications", value)) {
      return ("SwiftCloud w/client replicas")
    } else {
      return ("Server replicas only")
    }
  }
  labeller <- function(variable, value) {
    if (variable == "locality") {
      return (levels(value)[value])
    } else if (variable == "mode") {
      return (lapply(value, mode_labeller))
    } else {
      return (NA)
    }
  }
  
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  filename <- paste(paste(file.path(output_dir, output_suffix), "-cdfs_locality", format_ext, sep=""))
  p <- p + facet_grid(locality ~ mode, labeller = labeller)
  p <- p + theme(legend.position = c(0.845, 0.135), legend.background=element_blank()) #, element_rect(fill="white",colour="black"))
                 #legend.text = element_text(size=8))
  ggsave(plot=p, file=filename, height=2.45, width=3.55)
}

scalabilitythroughput_workloada_cdfs_locality_plot <- function() {
  cdfs_locality_plot("~/Dropbox/INRIA/results/scalabilitythroughput/processed/",
                    var_name="opslimit",
                    files=c("workloada-mode-notifications-frequent-clients-500-opslimit-1000",
                            "workloada-mode-no-caching-clients-1000-opslimit-1000",
                            "workloada-lowlocality-mode-notifications-frequent-clients-500-opslimit-1000",
                            "workloada-lowlocality-mode-no-caching-clients-1000-opslimit-1000"),
                    output_suffix="YCSB-workloada")
}

# scalabilitythroughput_social_cdfs_locality_plot <- function() {
#   cdfs_locality_plot("~/Dropbox/INRIA/results/scalabilitythroughput/processed/",
#                      var_name="opslimit",
#                      files=c("workload-social-mode-notifications-frequent-clients-500-opslimit-2000",
#                             "workload-social-mode-no-caching-clients-1000-opslimit-2000"),
#                      output_suffix="SwiftSocials")
# }

CDF_RESPONSE_TIME_CUTOFF <- 400
cdf_locality_plot <- function(dir, var_name, file, output_dir = file.path(dir, "comparison"), output_suffix) {
  stats <- read_runs_full(dir, var_name, "ops.csv", files=file)
  
  p <- ggplot()
  p <- p + labs(x="operation response time [ms]",y = "CDF for all sessions")
  p <- p + coord_cartesian(xlim = c(-1, CDF_RESPONSE_TIME_CUTOFF), ylim = c(0, 1.01))
  p <- p + scale_y_continuous(labels = percent)
  p <- p + THEME + theme(legend.direction='vertical', #element_rect(fill = 'white', colour = 'black'),
                         panel.margin= unit(1, 'lines'), legend.title = element_blank(), legend.key.height=unit(0.7,"line"),
                         panel.grid = element_blank())
  #p <- add_title(p, paste(w, m, dd, "DCs", cc, "clients", v, var_name))
  p <- p + scale_linetype_discrete(name = "",
                                   breaks = RESPONSE_TIME_OPERATION_TYPES,
                                   labels = OPERATION_TYPES_LABELS)
  p <- p + scale_color_discrete(name = "",
                                breaks = RESPONSE_TIME_OPERATION_TYPES,
                                labels = OPERATION_TYPES_LABELS)
  p <- p + annotate("rect", xmin = c(RTT, 4*RTT), xmax = CDF_RESPONSE_TIME_CUTOFF, ymin = 0, ymax = 1, alpha = .1)
  p <- p + annotate("text", x=RTT*(0.2+c(0,1,4)), y=0.34, label=c("0 RTT", "1 RTT", "4 RTT"), angle=90, size=2.6,hjust=1)
  
  cols <-  names(stats)
  permilles <- subset(stats, stat == "permille")
  LOCALITY_LEVELS <- c("High locality workload", "Low locality workload")
  locality <- function(w) {
    if (grepl("locality", w)) {
      return ("Low locality workload")
    }
    return ("High locality workload")
  }
  permilles$locality <- factor(lapply(permilles$workload, locality), levels=LOCALITY_LEVELS)
  locality_ann <- data.frame()
  for (l in unique(permilles$locality)) {
    for (m in unique(permilles$mode)) {
      if (grepl("no-caching", m)) {
        next
      }
      if (grepl("Low locality", l)) {
        expected_locality <- 0.4
      } else {
        expected_locality <- 0.8
      }
      locality_ann <- rbind(locality_ann,
                            data.frame(locality=l, mode=m, expected_locality=expected_locality,
                                       label="locality potential"))
    }
  } 
  p <- p + geom_rect(data=locality_ann, aes(ymin = expected_locality-0.07, ymax=expected_locality+0.07), xmin=0, xmax=CDF_RESPONSE_TIME_CUTOFF, fill = "gray", alpha=0.75)
  
  melted <- melt(permilles, id.vars=c("locality", "mode", "dcs", "clients", "var","stat_param"), cols[grepl("response_time_", cols)], na.rm=TRUE)
  melted <- transform(melted, variable = factor(variable, levels= RESPONSE_TIME_OPERATION_TYPES))
  filling <- subset(melted, stat_param==1 & value < CDF_RESPONSE_TIME_CUTOFF)
  filling$value <- rep(CDF_RESPONSE_TIME_CUTOFF, nrow(filling))
  melted <- rbind(melted, filling)
  p <- p + geom_line(data=melted,
                     mapping=aes(y=stat_param, x=value, group=interaction(locality, mode, dcs, clients, variable), colour=variable, linetype=variable))
  p <- p + geom_text(data=locality_ann, aes(y=expected_locality, label=label), x=CDF_RESPONSE_TIME_CUTOFF*.85,  size=2.6, show_guide=F)
  
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  filename <- paste(paste(file.path(output_dir, output_suffix), "-cdf_locality", format_ext, sep=""))
  p <- p + theme(legend.position = c(0.823, 0.383), legend.background=element_blank()) #, legend.background=element_rect(fill="white", color="black"))
  ggsave(p, file=filename, scale=1, height=1.7, width=3.55)
}


scalabilitythroughput_social_cdf_locality_plot <- function() {
  cdf_locality_plot("~/Dropbox/INRIA/results/scalabilitythroughput/processed/",
                    var_name="opslimit",
                    file="workload-social-mode-notifications-frequent-clients-500-opslimit-2000",
                    output_suffix="SwiftSocial")
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
      melted <- melt(mode_stats, id.vars=c("workload", "mode", "var", "dcs", "clients"), na.rm=TRUE) 
      melted <- transform(melted, value=as.numeric(value))
      p <- p + geom_point(data=melted, mapping=aes(x=var, y=value, color=interaction(mode, dcs, clients), shape=variable),
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

normalize_batch <- function(stats, message, batch_size, norm="norm2") {
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
var_notifications_metadata_plot <- function(dir, var_name, var_label_axis, files=".+",
                                            output_dir = file.path(dir, "comparison"),
                                            modes=c(), modes_labels=c(), modes_colors=c(),
                                            errors_threshold=4000, unstable_behavior_markers=F) {
  p <- var_notifications_metadata_plot_impl(dir, var_name, var_label_axis, files, modes,
                                            modes_labels, modes_colors, errors_threshold, unstable_behavior_markers)
  p <- p + facet_grid(~workload)
  p <- p + theme(legend.background = element_blank(), panel.margin=unit(0.6, "line"))
  p <- p + scale_x_continuous(breaks=c(500, 1500, 2500))
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  ggsave(p, file=paste(paste(file.path(output_dir, var_name), "-notifications-metadata", format_ext, sep="")),
         width=3.6, height=2.1)
}

var_notifications_metadata_plot_impl <- function(dir, var_name, var_label_axis, files=".+",
                                                 modes, modes_labels, modes_colors,
                                                 errors_threshold, unstable_behavior_markers) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", params=c("BatchUpdatesNotification.meta.indep", "BatchUpdatesNotification.meta.dep.norm2"), files=files)
  error_stats <- read_runs_errors(dir, var_name, "errors.csv", files=files)
  stats <- merge(stats, error_stats, by=c("workload", "mode", "dcs", "clients", "var"))
  stats <- subset(stats, errors.total < errors_threshold)
  stats <- stats[order(stats$var), ]
  stats <- normalize_batch(stats, "BatchUpdatesNotification.meta", NOTIFICATIONS_EXAMPLE_BATCH_SIZE)
  multiple_dcs <- length(unique(stats$dcs)) > 1
  
  # TODO: there is probably a simpler way to do compute limited_clients_throughput_stats.
  melted_stats <- melt(stats, id.vars=c("workload", "mode", "dcs"), measure.vars = c("var"))
  max_var_stats <- dcast(melted_stats, workload + mode + dcs ~ variable, max, fill=0)
  limited_mode_var_stats <- subset(max_var_stats, var != max(stats$var))
  limited_mode_var_stats <- merge(limited_mode_var_stats, stats, by=c("workload", "mode", "dcs", "var"))
  limited_mode_var_stats$label <- rep("/ unstable", nrow(limited_mode_var_stats))
  
  p <- ggplot() + THEME + theme(panel.margin= unit(0.54, 'lines'), legend.key.height=unit(0.8,"line"),
                                legend.title=element_blank(), legend.margin=unit(-2, "lines"))
  #legend.direction='horizontal', legend.box='horizontal')
  p <- add_title(p, paste(w, "normalized notifications metadata"))
  p <- p + labs(x=var_label_axis, y = "notification metadata [b]")
  min_y <- 10^9
  max_y <- 1
  if (multiple_dcs) {
    p <- p + geom_line(data=stats,
                       mapping=aes(y=BatchUpdatesNotification.meta.mean.scaled,
                                   x=var, group=interaction(workload,dcs,mode), color=mode,
                                   linetype=mode, size=dcs))
  } else {
    p <- p + geom_line(data=stats,
                       mapping=aes(y=BatchUpdatesNotification.meta.mean.scaled,
                                   x=var, group=interaction(workload,dcs,mode), color=mode,
                                   linetype=mode))
  }
  
  p <- p + geom_errorbar(data=stats, position=position_dodge(width=2),
                         mapping=aes(ymax=BatchUpdatesNotification.meta.max.scaled,
                                     ymin=BatchUpdatesNotification.meta.min.scaled,
                                     group=interaction(workload,dcs,mode),
                                     x=var), width=15)
  
  if (unstable_behavior_markers) {
#     p <- p + geom_segment(data=limited_mode_var_stats,
#                           mapping=aes(x=var+50, xend=var + 50,
#                                       y=BatchUpdatesNotification.meta.mean.scaled**0.95, yend=BatchUpdatesNotification.meta.mean.scaled**1.05), color="black",
#                           linestyle=2)
    p <- p + geom_text(data=limited_mode_var_stats,
                       mapping=aes(x=var+80, y=BatchUpdatesNotification.meta.mean.scaled, label=label),
                       size = 2.2, hjust=0)
  }
  min_y <- min(na.omit(stats$BatchUpdatesNotification.meta.min.scaled))
  max_y <- max(na.omit(stats$BatchUpdatesNotification.meta.max.scaled))
  #p <- p + scale_y_continuous(limits=c(0, ceiling(max_y/1000)*1000)) #, breaks=c(1, 10, 100, 1000, 10000))
  p <- add_cs_log_scale(p, min_y, max_y)
  if (length(modes) > 0) {
    p <- p + scale_color_manual(name="System configuration", labels=modes_labels, breaks=modes,
                                values=modes_colors)
    p <- p + scale_linetype_discrete(name="System configuration", labels=modes_labels, breaks=modes)
  } else {
    p <- p + scale_color_discrete(name="System configuration")
    p <- p + scale_linetype_discrete(name="System configuration")
  }
  if (multiple_dcs) {
    p <- p + scale_size_manual(name="#server (DC) replicas ", values=c(0.3, 0.75), breaks=c(1, 3), labels=c("1 DC replica", "3 DC replicas"))
  }
  return (p)
}

scalabilitythroughput_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("scalabilitythroughput"), "opslimit", "load")
}

MODES_SCALABILITY_CLIENTS <- c("notifications-frequent", "notifications-frequent-practi", "notifications-frequent-practi-no-deltas") # "notifications-infrequent", 
MODES_LABELS_SCALABILITY_CLIENTS <- c("SwiftCloud","Client-assigned metadata à la PRACTI/Depot", "Client-assigned metadata à la PRACTI/Depot w/o optimization")
scalabilityclients_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("scalabilityclients"), "clients", "#client replicas",
                                  modes=c("notifications-frequent", "notifications-frequent-practi"),
                                  modes_labels=c("SwiftCloud metadata","Client-assigned metadata à la PRACTI/Depot"),
                                  modes_color=c(BASIC_MODES_COLORS["notifications-frequent"], MODES_COLORS["notifications-frequent-practi"]))
}

scalabilityclientssmalldb_notifications_metadata_plot <- function() {
  var_notifications_metadata_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#client replicas",
                                  files=c("workloada-uniform-mode-notifications-frequent-clients-.*",
                                          "workloada-uniform-mode-notifications-frequent-practi-clients-.*",
                                          "workloada-uniform-mode-notifications-frequent-dcs-1-clients-.*",
                                          "workloada-uniform-mode-notifications-frequent-practi-dcs-1-clients-.*",
                                          "workloadb-uniform-mode-notifications-frequent-clients-.*",
                                          "workloadb-uniform-mode-notifications-frequent-practi-clients-.*",
                                          "workloadb-uniform-mode-notifications-frequent-dcs-1-clients-.*",
                                          "workloadb-uniform-mode-notifications-frequent-practi-dcs-1-clients-.*",
                                          "workload-social-views-counter-mode-notifications-frequent-clients-.*",
                                          "workload-social-views-counter-mode-notifications-frequent-practi-clients-.*",
                                          "workload-social-views-counter-mode-notifications-frequent-dcs-1-clients-.*",
                                          "workload-social-views-counter-mode-notifications-frequent-practi-dcs-1-clients-.*"
                                  ),
                                  modes=c("notifications-frequent", "notifications-frequent-practi"),
                                  modes_labels=c("SwiftCloud metadata","client-assigned metadata (Depot*)"),
                                  modes_color=c(BASIC_MODES_COLORS["notifications-frequent"], MODES_COLORS["notifications-frequent-practi"]))
}

scalabilitydbsize_notifications_metadata_plot <- function() {
  p <- var_notifications_metadata_plot_impl(experiment_dir("scalabilitydbsize"), "dbsize", "#objects in database",
                                            c(#"workloada-uniform-mode-notifications-frequent-dbsize-.*",
                                              #"workloadb-uniform-mode-notifications-frequent-dbsize-.*",
                                              "workload-social-views-counter-mode-notifications-frequent-dbsize-.*"),
                                            modes=c("notifications-frequent"),
                                            modes_labels=c("SwiftCloud metadata"),
                                            modes_color=c(BASIC_MODES_COLORS["notifications-frequent"]),
                                            errors_threshold=1000, unstable_behavior_markers=F)
  p <- p + theme(legend.position = "none", legend.background=element_blank())
  p <- p + scale_x_continuous(breaks=c(10000, 30000, 50000)) + labs(y = "notification metadata [b]       ")
  output_dir <- file.path(experiment_dir("scalabilitydbsize"), "comparison")
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  ggsave(p, file=paste(paste(file.path(output_dir, "dbsize"), "-notifications-metadata", format_ext, sep="")),
         width=1.75, height=1.6)
}

clientfailures_notifications_metadata_plot <- function() {
  p <- var_notifications_metadata_plot_impl(experiment_dir("clientfailures"), "failures", "#unavailable client replicas",
                                            files=c("workloada-uniform-mode-notifications-frequent-failures-.*",
                                                    "workloada-uniform-mode-notifications-infrequent-practi-failures-.*",
                                                    "workloada-uniform-mode-notifications-infrequent-practi-no-deltas-failures-.*",
                                                    "workload-social-views-counter-mode-notifications-frequent-failures-.*",
                                                    # FILTERED MANUALLY w.r.t errorneous behavior
                                                    "workload-social-views-counter-mode-notifications-infrequent-practi-failures-0",
                                                    "workload-social-views-counter-mode-notifications-infrequent-practi-failures-500",
                                                    "workload-social-views-counter-mode-notifications-infrequent-practi-failures-1000",
                                                    "workload-social-views-counter-mode-notifications-infrequent-practi-failures-1500",
                                                    "workload-social-views-counter-mode-notifications-infrequent-practi-no-deltas-failures-.*"
                                            ), modes=c("notifications-frequent", "notifications-infrequent-practi", "notifications-infrequent-practi-no-deltas"),
                                            modes_labels=c("SwiftCloud metadata", "client-assigned metadata (Depot*)", "client-assigned metadata unoptimized"),
                                            modes_color=c(BASIC_MODES_COLORS["notifications-frequent"], MODES_COLORS["notifications-infrequent-practi"], MODES_COLORS["notifications-infrequent-practi-no-deltas"]),
                                            errors_threshold=10000, unstable_behavior_markers=T)
  p <- p + facet_grid(~workload)
  #p <- pscale_linetype(name="Metadata")
  p <- p + theme(legend.background=element_blank(), legend.key.height=unit(0.7, "line"))
  output_dir <- file.path(experiment_dir("clientfailures"), "comparison")
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  ggsave(p, file=paste(paste(file.path(output_dir, "failures"), "-notifications-metadata", format_ext, sep="")),
         width=3.6, height=2.1)
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

var_storage_plot <- function(dir, var_name, var_label, files, output_dir = file.path(dir, "comparison"),
                             modes=c(), modes_labels=c(), modes_colors=c(), modes_shapes=c(),
                             errors_threshold=10000, unstable_behavior_markers=T) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", c("idempotenceGuard", dc_table(TABLES)), files=files)
  error_stats <- read_runs_errors(dir, var_name, "errors.csv", files=files)
  stats <- merge(stats, error_stats, by=c("workload", "mode", "dcs", "clients", "var"))
  stats <- subset(stats, errors.total < errors_threshold)
  stats <- stats[order(stats$var), ]
  
  #HACK to unify presentation!
  stats$mode[stats$mode == "refresh-frequent"] <- "refresh-infrequent-bloated-counters"
  
  guard_stats <- subset(stats, mode == "notifications-frequent")
  guard_stats$mode <- rep("notifications-frequent-idempotence-guard", nrow(guard_stats))
  guard_stats$size <- guard_stats$idempotenceGuard.mean * IDEMPOTENCE_GUARD_ENTRY_BYTES
  stats$size <- mapply(function(workload, tableYCSB, tableSwiftSocial) {
    if (grepl("YCSB", workload)) {
      return (tableYCSB)
    }
    return (tableSwiftSocial)
  }, stats$workload, stats[[paste(dc_table(select_table("YCSB")), "mean", sep=".")]],
     stats[[paste(dc_table(select_table("swiftsocial")), "mean", sep=".")]])
  stats <- rbind(stats, guard_stats)
  melted <- melt(stats, id.vars=c("workload", "mode", "var"), measure.vars = c("size"))
  
  # TODO: there is probably a simpler way to do compute limited_mode_var_stats
  max_var_stats <- dcast(melt(stats, id.vars=c("workload", "mode"), measure.vars = c("var")), workload + mode ~ variable, max, fill=0)
  limited_mode_var_stats <- subset(max_var_stats, var != max(stats$var))
  limited_mode_var_stats <- merge(limited_mode_var_stats, stats, by=c("workload", "mode", "var"))
  limited_mode_var_stats$label <- rep("/ unstable", nrow(limited_mode_var_stats))
  

  p <- ggplot() + THEME + theme(legend.title=element_blank(), legend.background=element_blank(),
                                legend.key.height=unit(0.7, "line"), legend.margin=unit(-2.0, "line"))
  p <- p + labs(x=var_label,y = "storage occupation [b]")  

  p <- p + geom_path(data=melted, mapping=aes(y=value, x=var, color=mode, group=interaction(workload, mode), linetype=mode))
  p <- p + geom_point(data=melted, mapping=aes(y=value, x=var, color=mode, shape=mode), position=position_dodge(width=60))
  
  if (unstable_behavior_markers) {
    #p <- p + geom_segment(data=limited_mode_var_stats,
    #                      mapping=aes(x=var+80, xend=var + 80, y=total**0.95, yend=total**1.05), color="black",linestyle=2)
    p <- p + geom_text(data=limited_mode_var_stats,
                       mapping=aes(x=var+150, y=size, label=label), size = 2.2, hjust=0)
  }
  
  if (length(modes) > 0) {
    p <- p + scale_color_manual(name="System configuration", values=modes_colors, breaks=modes, labels=modes_labels)
    p <- p + scale_shape_manual(name="System configuration", values=modes_shapes, breaks=modes, labels=modes_labels)
    p <- p + scale_linetype_discrete(name="System configuration", breaks=modes, labels=modes_labels)
  }
  p <- add_cs_log_scale(p, 10**3, 10**9)
  labeller <- function(var, values) {
    return (lapply(values, function(value) {
      if (grepl("YCSB", value)) {
        return ("YCSB - all objects")
      }
      if (grepl("Social", value)) {
        return ("SwiftSocial-  stats counters")
      }
      return (value)
    }))
  }
  p <- p + facet_grid(~workload, labeller = labeller)
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  ggsave(p, file=paste(paste(file.path(output_dir, var_name), "-storage_dc", format_ext, sep="")),
         width=3.6, height=2.2)
}

clientfailures_storage_plot <- function() {
  var_storage_plot(experiment_dir("clientfailures"), "failures", "#unavailable client replicas",
                   files=c("workloada-uniform-mode-notifications-frequent-failures-.*",
                           "workloada-uniform-mode-refresh-frequent-failures-.*",
                           "workloada-uniform-mode-notifications-frequent-no-pruning-failures-.*",
                           "workload-social-views-counter-mode-notifications-frequent-failures-.*",
                           "workload-social-views-counter-mode-notifications-frequent-no-pruning-failures-.*",
                           "workload-social-views-counter-mode-refresh-infrequent-bloated-counters-failures-.*"
                           ),
                   modes=c("notifications-frequent", "notifications-frequent-idempotence-guard", "notifications-frequent-no-pruning", "refresh-infrequent-bloated-counters"),
                   modes_labels=c("SwiftCloud log compaction protocol", "\\-> size of idempotence guard of SwiftCloud", "stability-based log compaction protocol", "object-level metadata compaction protocol"),
                   modes_colors=c(BASIC_MODES_COLORS["notifications-frequent"], MODES_COLORS["notifications-frequent-idempotence-guard"],
                                  MODES_COLORS["notifications-frequent-no-pruning"], MODES_COLORS["refresh-infrequent-bloated-counters"]),
                   modes_shapes=c(0, 1, 2, 4),
                   errors_threshold=10000)
}

scalabilityclients_storage_plot <- function() {
  var_storage_plot(experiment_dir("scalabilityclients"), "clients", "#client replicas")
}

scalabilityclientssmalldb_storage_plot <- function() {
  modes <- c("notifications-infrequent", "notifications-infrequent-practi", "notifications-infrequent-practi-no-deltas", "notifications-infrequent-bloated-counters")
  var_storage_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#client replicas",
                   modes=modes,
                   modes_labels=modes)
}

DB_SIZE_CHECKPOINT_EXP <- 10000
var_checkpoint_size_plot <- function(dir, var_name, var_label, files, output_dir = file.path(dir, "comparison"),
                                     modes, modes_colors, modes_labels) {
  stats <- read_runs_params(dir, var_name, "meta_size.csv", params=dc_table("views.table"), files=files)
  stats <- stats[order(stats$var), ]
  stats$counter.checkpoint.mean <- stats$views.table.dc.mean / DB_SIZE_CHECKPOINT_EXP

  p <- ggplot()
  p <- p + coord_cartesian(ylim = c(0, 600))
  p <- add_title(p, "object checkpoint size (including metadata)")
  p <- p + labs(x=var_label,y ="object size [b]")
  p <- p + THEME  + theme(legend.title=element_blank(), #plot.margin = unit(c(2, 0, 0, 0), "cm"),
                          legend.position=c(0.52, 0.46),
                          legend.key.width=unit(0.75, "line"),
                          legend.margin=unit(-1.8, units="lines"), legend.key.height=unit(1.8,"line"),
                          legend.text = element_text(size=6.5), legend.background=element_blank())
                          #legend.direction="horizontal") #, legend.position=c(0.5, 0.2))
  #melted <- melt(mode_stats, id.vars=c("workload", "mode", "var"))
  #melted <- subset(melted, variable %in% c("counter.checkpoint.mean"))
  p <- p + geom_path(data=stats, mapping=aes(y=counter.checkpoint.mean, x=var, color=mode))
  p <- p + geom_point(data=stats, mapping=aes(y=counter.checkpoint.mean, x=var, color=mode, shape=mode))

  p <- p + scale_color_manual(name="System configuration", values=modes_colors, breaks=modes, labels=modes_labels)
  p <- p + scale_shape_discrete(name="System configuration", breaks=modes, labels=modes_labels)
  
  dir.create(output_dir, recursive=TRUE, showWarnings=FALSE)
  ggsave(p, file=paste(paste(file.path(output_dir, var_name), "-counter-checkpoint-dc", format_ext, sep="")),
         width=1.75, height=1.5)
}

scalabilityclientssmalldb_checkpoint_size_plot <- function() {
  var_checkpoint_size_plot(experiment_dir("scalabilityclients-smalldb"), "clients", "#client replicas",
                           files=c("workload-social-views-counter-mode-notifications-frequent-clients-.*",
                                   "workload-social-views-counter-mode-refresh-infrequent-bloated-counters-clients-.*"),
                           modes=c("refresh-infrequent-bloated-counters", "notifications-frequent"),
                           modes_labels=c("server-assigned\ntimestamps only\n(weak guarantees)", "SwiftCloud"),
                           modes_colors=c(BASIC_MODES_COLORS["notifications-frequent"], MODES_COLORS["refresh-infrequent-bloated-counters"]))
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
