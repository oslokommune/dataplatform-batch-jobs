#!/bin/bash

PYTHONPATH=. alembic upgrade head;

luigid --background;
python -m luigi --module aggregator.tasks Run --hours 96;
