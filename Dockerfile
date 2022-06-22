# python:3.9-bullseye
FROM python@sha256:efb5ab0fef765227c65ddc52e524b8e2766f65c6c119b908744ff5c840dfd2d3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x /usr/src/app/start.sh

CMD ["sh", "-c", "/usr/src/app/start.sh"]
