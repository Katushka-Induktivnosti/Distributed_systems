FROM python:latest
LABEL node_number="lab1iter3"

WORKDIR /usr/app/src
COPY ./* ./
RUN ["python", "-m", "pip", "install", "-r", "requirements.txt"]
CMD [ "python", "./main.py"]