FROM python:3.10

RUN apt-get update && \
    apt-get install -y graphviz && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN mkdir -p /app/static/ \
     && mkdir -p /app/media/ \
     && pip install --upgrade pip \
     && pip install -r requirements.txt --no-cache-dir

RUN rm -rf /usr/local/lib/python3.10/site-packages/redis
RUN rm -rf /usr/local/lib/python3.10/site-packages/redis-*

RUN pip install --no-cache-dir redis==6.2.0

COPY . .

EXPOSE 8000/tcp

#CMD ["gunicorn", "config.wsgi:application", "--bind", "0:8000", "--access-logfile", "-", "--error-logfile", "-", "--capture-output"]
CMD ["gunicorn", "config.wsgi:application", "--bind", "0:8000", "--access-logfile", "-", "--error-logfile", "-", "--capture-output", "--timeout", "300"]

