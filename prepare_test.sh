#!/bin/bash

__DIR_FILE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "Preparing the database for locust test."
# ========================================= #
# Download the data

if [ ! -e "/tmp/Food/Food_1.csv" ]; then
   wget -P /tmp/Food                                                      \
           http://www.cwi.nl/~boncz/PublicBIbenchmark/Food/Food_1.csv.bz2
   cd /tmp/Food || exit
   echo "Extracting data"
   bzip2 -dk Food_1.csv.bz2
fi

# ======================================== #
# start docker for postgres
docker_id=$(sudo docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d postgres)
echo "Docker image is ${docker_id}"
sleep 5
sudo docker exec -t "${docker_id}" bash -c 'psql -U postgres -c "CREATE DATABASE \"SYSTEM\";"'

# ======================================== #
# load the data
python3 "${__DIR_FILE}"/load_data_in_database.py

echo "================ DONE ======================"
