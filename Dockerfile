FROM python:3.12-slim

WORKDIR /app

COPY etherfi-bids-exporter.py .

RUN pip3 install pipreqs

RUN pipreqs . --force

RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["python3", "./etherfi-bids-exporter.py"]