# TP - Outils pour la Data

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
chown -R $(whoami) ./airflow/*

docker compose pull
docker compose up -d --build
```

Plusieurs urls : 
* Airflow : http://localhost:8080 avec identifiants airflow/airflow
* MinIO : http://localhost:9000 avec identifiants minioadmin/minioadmin
* RabbitMQ : http://localhost:15672 avec identifiants guest/guest
