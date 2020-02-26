#!/bin/bash

luigid --background;
python -m luigi --module aggregator.tasks Run --hours 96;
