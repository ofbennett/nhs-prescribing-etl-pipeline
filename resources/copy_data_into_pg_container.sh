# Copies local data into your postgres database Docker container
# Example usage: $ bash copy_data_into_pg_container.sh <name of postgres docker container>
# The local paths below will need to be adapted

docker cp /Users/oscarbennett/Desktop/DEND/capstone/data/2019_11_Nov/T201911PDPI\ BNFT.csv ${1}:/home/T201911PDPI_BNFT.csv

docker cp /Users/oscarbennett/Desktop/DEND/capstone/data/2019_11_Nov/T201911ADDR\ BNFT.csv ${1}:/home/T201911ADDR_BNFT.csv

docker cp /Users/oscarbennett/Desktop/DEND/capstone/data/20200201_1580570906919_BNF_Code_Information.csv ${1}:/home/BNF_Code_Information.csv

docker cp /Users/oscarbennett/Desktop/DEND/capstone/data/postcode_info.json ${1}:/home/postcode_info.json

docker cp /Users/oscarbennett/Desktop/DEND/capstone/data/postcode_info.csv ${1}:/home/postcode_info.csv