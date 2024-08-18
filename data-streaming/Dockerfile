FROM python:3.9

WORKDIR /app

RUN pip install --upgrade pip

COPY . .
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "streaming.py"]