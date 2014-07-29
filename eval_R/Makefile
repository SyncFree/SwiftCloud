RUNS=$(wildcard *.tar.gz)
RESULTS_DIR=processed
RUN_RESULTS=$(addprefix $(RESULTS_DIR)/,$(RUNS:.tar.gz=-operations_stats.csv))

EVAL_R_DIR=/home/zawir/code/swiftcloud/eval_R
RSCRIPT_RUN=Rscript $(EVAL_R_DIR)/analyze_ycsb_op.R
is_file_synced=dropbox filestatus $(1) | grep -q "up to date"

all: $(RUN_RESULTS)

$(RESULTS_DIR)/%-operations_stats.csv: %.tar.gz
	$(call is_file_synced,$<) && $(RSCRIPT_RUN) $<

clean:
	rm -Rf $(RESULTS_DIR)

.PHONY: clean

