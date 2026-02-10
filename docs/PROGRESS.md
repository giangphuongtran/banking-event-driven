## 10-Feb-2026:
 
``` bash
cd Volumes/Sandisk/learning
mkdir docker .github/workflows consumer postgres kafka-debezium data-generator
python -m venv .venv
source .venv/bin/activate
```

- Create `README.md, .gitignore, docker-compose.yml, dockerfile-airflow.dockerfile, .env`
- Add `platform: linux/amd64` because some images in the `docker-compose.yml` don't publish an arm64 manifest for that tag

```bash
docker compose exec airflow-webserver airflow users create --username mimi --firstname tran --lastna
me mi --role Admin --email admin@gmail.com --password airflow_password
```

- Airflow services failed because of no airflow db init -> Add airflow-init service into `docker-compose.yml`
- Create database and tables `postgres/01_create_tables.sql`
- Create data generator `data-generator/faker_generator.py`
- Create a connector to Kafka and Debezium `kafka-debezium/generate_post_connector.py`
- Check for existing topics on Kafka

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

- Commit and save today's progress to Github

```bash
git init
git add README.md
git commit -m "Initial commit: event-driven banking data platform"
git branch -M main
git remote add origin https://github.com/giangphuongtran/banking-event-driven-with-docker-airflow-postgres-tableau.git
git push -u origin main
```

