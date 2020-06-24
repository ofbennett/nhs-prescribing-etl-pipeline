bash /root/NHS_Prescribing_ETL_Pipeline/scripts/get_traffic.sh >> record.txt
/usr/local/bin/docker-compose -f NHS_Prescribing_ETL_Pipeline/visualisation_web_app/docker-compose-prod.yml down
/usr/local/bin/docker-compose -f NHS_Prescribing_ETL_Pipeline/visualisation_web_app/docker-compose-prod.yml up -d
