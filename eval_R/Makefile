RUNS=$(wildcard *.tar.gz)
RESULTS_DIR=processed
# TODO: add -meta*.csv too
RUN_SUMMARIZED=$(addprefix $(RESULTS_DIR)/,$(RUNS:.tar.gz=-ops.csv))

EVAL_R_DIR=/home/zawir/code/swiftcloud/eval_R
RSCRIPT_RUN=Rscript $(EVAL_R_DIR)/analyze_run.R
is_file_synced=dropbox filestatus $(1) | grep -q "up to date"

all: $(RUN_SUMMARIZED)

# TODO add SPECTROGRAM only mode

$(RESULTS_DIR)/%-ops.csv: %.tar.gz
	$(call is_file_synced,$<) && $(RSCRIPT_RUN) all $<

clean:
	rm -Rf $(RESULTS_DIR)

.PHONY: clean

