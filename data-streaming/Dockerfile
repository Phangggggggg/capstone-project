FROM --platform=linux/amd64 python:3.9 as build

WORKDIR /app

RUN pip install --upgrade pip

COPY . .
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "streaming.py"]