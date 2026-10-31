PY ?= python3
.PHONY: test sample run demo benchmark large clean

test:        ## run the full automated test suite (stdlib unittest, no dependencies)
	$(PY) -m unittest discover -s tests -t . -v

sample:      ## regenerate the small reproducible sample (seed 42)
	$(PY) -m sla_monitor generate --out data/sample

run: sample  ## ingest + evaluate + write reports for the sample
	$(PY) -m sla_monitor run --as-of 2026-01-15T07:40:00Z

demo:        ## scripted walkthrough: rerun, partial failure, retry, replay
	bash scripts/demo.sh

benchmark:   ## 136k-row timing run + optimisation measurement
	$(PY) -m sla_monitor benchmark --datasets 60 --days 90 --out evidence/benchmark.json

large:       ## generate + run + validate the 136k-row dataset in /tmp
	$(PY) -m sla_monitor generate --out /tmp/sla_large/data --datasets 60 --days 90
	$(PY) -m sla_monitor run --data-dir /tmp/sla_large/data --db /tmp/sla_large/o/l.db --output-dir /tmp/sla_large/o --as-of 2026-01-15T07:40:00Z --lookback-hours 2160

clean:
	rm -rf output data/sample evidence/*.tmp __pycache__ */__pycache__
