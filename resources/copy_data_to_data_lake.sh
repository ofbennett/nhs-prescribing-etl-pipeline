# Copies local data to your s3 data lake
# Example usage: $ bash copy_data_to_data_lake.sh <name of s3 bucket>
# The local paths below will need to be adapted
# AWS credentials with suitable s3 write privileges should be in the .aws/credentials file

aws s3 cp postcode_info.csv s3://prescribing-data/postcode_info.csv
aws s3 cp 20200201_1580570906919_BNF_Code_Information.csv s3://${1}/BNF_Code_Information.csv
aws s3 cp 2019_11_Nov/T201911PDPI\ BNFT.csv s3://${1}/2019_11_Nov/T201911PDPI_BNFT.csv
aws s3 cp 2019_11_Nov/T201911ADDR\ BNFT.csv s3://${1}/2019_11_Nov/T201911ADDR_BNFT.csv