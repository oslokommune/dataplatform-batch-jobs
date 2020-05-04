all: s3_access_log_aggregator.png s3_dataset_scanner.png

%.png: %.dot
	dot -Tpng $< -o $@

.PHONY: clean
clean:
	rm -f *.png
