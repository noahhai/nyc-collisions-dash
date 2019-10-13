#!/bin/sh
docker-compose up -d db
docker-compose up script_load_stations
docker-compose up script_load_incidents
docker-compose up app
