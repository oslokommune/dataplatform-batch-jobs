# python:3.7-bullseye
FROM python@sha256:7d5ba6ac052be01bc68beefcd62de0b7fb8084f9b1e56035c5edce0b4cde946e

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x /usr/src/app/start.sh

CMD ["sh", "-c", "/usr/src/app/start.sh"]
