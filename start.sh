#!/bin/bash

case $JOB_NAME in
    s3_access_log_aggregator)
        PYTHONPATH=. alembic upgrade head;
        luigid --background;
        python -m luigi --module batch.s3_access_log_aggregator.tasks Run --days ${DAYS:-4} ;;

    s3_dataset_scanner)
        PYTHONPATH=. alembic upgrade head;
        luigid --background;
        python -m luigi --module batch.s3_dataset_scanner.tasks ExtractDatasetsFromS3Metadata --date $(date -I) ;;

    "")
        echo "Please set JOB_NAME" ;;

    *)
        echo "Unknown job: \"$JOB_NAME\"" ;;
esac
