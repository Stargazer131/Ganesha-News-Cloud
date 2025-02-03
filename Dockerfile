FROM python:3.10-bullseye

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

COPY . /app

EXPOSE 8000

CMD ["fastapi", "run", "server/main.py", "--port", "8000"]
