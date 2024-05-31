import requests
import json
import os
import time
import sqlite3
import logging
import prometheus_client
from prometheus_client import start_http_server, Gauge
from dotenv import load_dotenv

prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class EtherFiBidsExporter():

    def __init__(self, bidder_address, api_url, fetching_bids_interval_minutes):
        self.bidder_address = bidder_address
        self.api_url = api_url
        self.fetching_bids_interval_minutes = fetching_bids_interval_minutes
        self.bids = list()

        self.api_health = Gauge('etherfi_bids_api_health', 'API status of etherFi bids')
        self.active_bids_min = Gauge('etherfi_bids_amount_min', 'Minimum amount of bids', labelnames=['status'])
        self.active_bids_max = Gauge('etherfi_bids_amount_max', 'Maximum amount of bids', labelnames=['status'])
        self.winning_bids = Gauge('etherfi_bids_winning', 'Number of winning etherfi bids', labelnames=['bidder_address'])
        self.active_bids = Gauge('etherfi_bids_active', 'Number of active etherfi bids', labelnames=['bidder_address'])
        self.cancelled_bids = Gauge('etherfi_bids_cancelled', 'Number of cancelled etherfi bids', labelnames=['bidder_address'])
        self.ready_validators = Gauge('etherfi_bids_validators_ready', 'Number of ready validators enabled by bids')
        self.live_validators = Gauge('etherfi_bids_validators_live', 'Number of live validators enabled by bids')
    
    def do(self):
        logger.info('Doing EtherFi Bids Exporter')
        while True:
            self.fetch_active_bids_min_max()
            self.fetch_our_bids()
            self.record_our_bids()
            logger.info(f'Sleeping {self.fetching_bids_interval_minutes}min')
            time.sleep(self.fetching_bids_interval_minutes * 60)
    
    def fetch_active_bids_min_max(self):
        response = requests.post(self.api_url, json={
            'query': '''
            {
                activeBidsMin: bids(where: {status: "ACTIVE"}, orderBy: amount, orderDirection: asc, first: 1) {
                    amount
                }
                activeBidsMax: bids(where: {status: "ACTIVE"}, orderBy: amount, orderDirection: desc, first: 1) {
                    amount
                }
            }
            '''
        }, headers={
            'Content-Type': 'application/json'
        })

        if response.status_code != 200:
            logger.info(f'Failed API Response [{response.status_code}] {response.text}')
            self.api_health.set(0)
            return None

        min_amount = response.json()['data']['activeBidsMin'][0]['amount']
        self.active_bids_min.labels(status='active').set(min_amount)
        logger.info(f'Minimum Amount of Active Bids: {min_amount}')

        max_amount = response.json()['data']['activeBidsMax'][0]['amount']
        self.active_bids_max.labels(status='active').set(max_amount)
        logger.info(f'Maximum Amount of Acitve Bids: {max_amount}')

        self.api_health.set(1)

    def fetch_our_bids(self):
        self.bids = list()
        all_bids = list()
        start = 0
        interval = 1000

        while True:
            response = requests.post(self.api_url, json={
                'query': '''
                {
                    bids(where: {bidderAddress: "%s", pubKeyIndex_gte: "%s", pubKeyIndex_lt: "%s"}, first: %d) {
                        id
                        bidderAddress
                        pubKeyIndex
                        status
                        amount
                        blockNumber
                        blockTimestamp
                        transactionHash
                        validator{
                            id
                            phase
                            validatorPubKey
                            blockNumber
                            blockTimestamp
                            transactionHash
                        }
                    }
                }
                ''' % (self.bidder_address, start, start + interval, interval)
            }, headers={
                'Content-Type': 'application/json'
            })

            if response.status_code != 200:
                logger.info(f'Failed API Response [{response.status_code}] {response.text}')
                self.api_health.set(0)
                return None

            body = response.json()
            if ('data' not in body) or ('bids' not in body.get('data')) or (len(body.get('data').get('bids')) < 1):
                break

            bids = body['data']['bids']
            all_bids.extend(bids)
            start += interval

        self.bids = sorted(all_bids,  key=lambda x: int(x['pubKeyIndex']))
        logger.info(f'Fetcted Bids: {len(self.bids)}')
        
        self.api_health.set(1)
    
    def record_our_bids(self):
        conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'etherfi-bids.db'))
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Bid (
            id TEXT PRIMARY KEY,
            bidderAddress TEXT,
            pubKeyIndex INTEGER,
            status TEXT,
            amount TEXT,
            blockNumber INTEGER,
            blockTimestamp INTEGER,
            transactionHash TEXT
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS Validator (
            bid_id TEXT PRIMARY KEY,
            phase TEXT,
            pubKey Text,
            blockNumber INTEGER,
            blockTimestamp INTEGER,
            transactionHash TEXT,
            FOREIGN KEY (bid_id) REFERENCES Bid (id)
        )
        ''')

        for bid in self.bids:
            cursor.execute('''
            INSERT OR REPLACE INTO Bid (id, bidderAddress, pubKeyIndex, status, amount, blockNumber, blockTimestamp, transactionHash) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                bid['id'],
                bid['bidderAddress'],
                bid['pubKeyIndex'],
                bid['status'],
                bid['amount'],
                bid['blockNumber'],
                bid['blockTimestamp'],
                bid['transactionHash']
            ))

            validator = bid.get('validator')
            if not validator:
                continue

            cursor.execute('''
            INSERT OR REPLACE INTO Validator (bid_id, phase, pubKey, blockNumber, blockTimestamp, transactionHash)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                bid['id'],
                validator['phase'],
                validator['validatorPubKey'],
                validator['blockNumber'],
                validator['blockTimestamp'],
                validator['transactionHash'],
            ))

        conn.commit()
        logger.info(f'Recorded Bids: {len(self.bids)}')

        cursor.execute(f'''
        SELECT status, COUNT(*)
        FROM Bid
        WHERE bidderAddress = '{self.bidder_address}' COLLATE NOCASE
        GROUP BY status
        ''')
        rows = cursor.fetchall()

        status_counts = {status: 0 for status in ['WON', 'ACTIVE', 'CANCELLED']}
        for status, count in rows:
            if status not in status_counts:
                logger.info(f'Unknown Bid Status: {status} ({count})')
                break
            status_counts[status] = count
        
        for status, count in status_counts.items():
            if status == 'WON':
                self.winning_bids.labels(bidder_address=self.bidder_address).set(count)
                logger.info(f'Winning Bids: {count}')
            elif status == 'ACTIVE':
                self.active_bids.labels(bidder_address=self.bidder_address).set(count)
                logger.info(f'Active Bids: {count}')
            elif status == 'CANCELLED':
                self.cancelled_bids.labels(bidder_address=self.bidder_address).set(count)
                logger.info(f'Cancelled Bids: {count}')
        
        cursor.execute(f'''
        SELECT Validator.phase, COUNT(*)
        FROM Validator
        JOIN Bid ON Bid.id = Validator.bid_id
        WHERE Bid.bidderAddress = '{self.bidder_address}' COLLATE NOCASE
        GROUP BY Validator.phase
        ''')
        rows = cursor.fetchall()

        phase_counts = {phase: 0 for phase in ['READY_FOR_DEPOSIT', 'LIVE']}
        for phase, count in rows:
            if phase not in phase_counts:
                logger.info(f'Unknown Validator Phase: {phase} ({count})')
                break
            phase_counts[phase] = count
        
        for phase, count in phase_counts.items():
            if phase == 'READY_FOR_DEPOSIT':
                self.ready_validators.set(count)
                logger.info(f'Ready Validtaors: {count}')
            elif phase == 'LIVE':
                self.live_validators.set(count)
                logger.info(f'Live Validators: {count}')

        conn.close()

def main():
    logger.info('Starting Etherfi Bids Exporter')
    load_dotenv()

    bidder_address = os.getenv('BIDDER_ADDRESS')
    logger.info(f'Bidder Address: {bidder_address}')

    api_url = os.getenv('API_URL')
    logger.info(f'API Url: {api_url}')

    fetching_bids_interval_minutes = int(os.getenv('FETCHING_BIDS_INTERVAL_MINUTES', 60))
    logger.info(f'Fetching Bids Interval Minutes: {fetching_bids_interval_minutes}')

    exporter_port = int(os.getenv('EXPORTER_PORT', 8000))
    logger.info(f'Exporter Port: {exporter_port}')
    
    exporter = EtherFiBidsExporter(
        bidder_address=bidder_address,
        api_url=api_url,
        fetching_bids_interval_minutes=fetching_bids_interval_minutes
    )
    
    start_http_server(exporter_port)
    exporter.do()

if __name__ == "__main__":
    main()
