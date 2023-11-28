FROM python:3.8

WORKDIR /app

# Copier le script Python
COPY ./src/1_rabbit_to_minio.py /app/
COPY ./src/2_rabbit_to_db.py /app/


# Installer les dépendances
RUN pip3 install minio pika elasticsearch


# Définir la commande à exécuter au lancement
ENTRYPOINT ["python", "1_rabbit_to_minio.py","2_rabbit_to_db.py"]


