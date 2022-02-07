# python:3.7-bullseye
FROM python@sha256:3d9ede0e8822da1531ca1363e46219ce360c9be6070c5e4b12f919499a9a2a97

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x /usr/src/app/start.sh

CMD ["sh", "-c", "/usr/src/app/start.sh"]
