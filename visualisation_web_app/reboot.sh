bash NHS_Prescribing_ETL_Pipeline/scripts/get_traffic.sh >> record.txt
/usr/bin/docker compose -f NHS_Prescribing_ETL_Pipeline/visualisation_web_app/docker-compose-prod.yml down
sleep 5
/usr/bin/docker compose -f NHS_Prescribing_ETL_Pipeline/visualisation_web_app/docker-compose-init-ssl.yml up -d --build
sleep 20
/usr/bin/docker compose -f NHS_Prescribing_ETL_Pipeline/visualisation_web_app/docker-compose-init-ssl.yml down
sleep 5
/usr/bin/docker compose -f NHS_Prescribing_ETL_Pipeline/visualisation_web_app/docker-compose-prod.yml up -d --build