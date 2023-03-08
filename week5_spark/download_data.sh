set -e

TAXI_TYPE=$1 #"yellow"
YEAR=$2 #2020

#/yellow/yellow_tripdata_2021-01.csv.gz
#https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2021-01.csv.gz

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

#log file 
LOGGER="log.txt"
LOGGER_URL="data/raw"
touch  "${LOGGER_URL}/${LOGGER}"
echo "Log file for ${TAXI_TYPE}_${YEAR} dataset:" >> "${LOGGER_URL}/${LOGGER}"

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`

    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "Downloading ${URL} to ${LOCAL_PATH}"

    mkdir -p ${LOCAL_PREFIX}

    wget ${URL} -O ${LOCAL_PATH}

    #save info when finished
    echo "" >> "${LOGGER_URL}/${LOGGER}"
    echo "${LOCAL_FILE} downloaded - $(date)" >> "${LOGGER_URL}/${LOGGER}"

done

echo "" >> "${LOGGER_URL}/${LOGGER}"
echo "Log info available in ${LOGGER_URL}/${LOGGER} file"
open "${LOGGER_URL}/${LOGGER}"