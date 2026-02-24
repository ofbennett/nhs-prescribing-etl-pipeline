bash nhs-prescribing-etl-pipeline/scripts/get_traffic.sh >> record.txt
/usr/bin/docker compose -f nhs-prescribing-etl-pipeline/visualisation_web_app/docker-compose-prod.yml down
sleep 5
/usr/bin/docker compose -f nhs-prescribing-etl-pipeline/visualisation_web_app/docker-compose-init-ssl.yml up -d --build
sleep 20
/usr/bin/docker compose -f nhs-prescribing-etl-pipeline/visualisation_web_app/docker-compose-init-ssl.yml down
sleep 5
/usr/bin/docker compose -f nhs-prescribing-etl-pipeline/visualisation_web_app/docker-compose-prod.yml up -d --build