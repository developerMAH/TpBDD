FROM python:3.8

WORKDIR /app

# Copier le script Python
COPY ./src/1_rabbit_to_minio.py .

# Copier les données
COPY ./data/movies-stackexchange/json/Posts.json /app/data/movies-stackexchange/json/Posts.json

# Installer les dépendances
RUN pip3 install minio pika

# Définir la commande à exécuter au lancement
ENTRYPOINT ["python", "1_rabbit_to_minio.py"]