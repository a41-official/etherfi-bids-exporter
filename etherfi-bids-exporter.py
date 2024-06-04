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

        self.api_health = Gauge('etherfi_bids_api_health', 'API status of etherFi bids')
        self.bids_amount_min = Gauge('etherfi_bids_amount_min', 'Minimum amount of bids', labelnames=['bidder_address', 'status'])
        self.bids_amount_max = Gauge('etherfi_bids_amount_max', 'Maximum amount of bids', labelnames=['bidder_address', 'status'])
        self.winning_bids = Gauge('etherfi_bids_winning', 'Number of winning etherfi bids', labelnames=['bidder_address'])
        self.active_bids = Gauge('etherfi_bids_active', 'Number of active etherfi bids', labelnames=['bidder_address'])
        self.cancelled_bids = Gauge('etherfi_bids_cancelled', 'Number of cancelled etherfi bids', labelnames=['bidder_address'])
        self.validators_phase = Gauge('etherfi_bids_validators_phase', 'Number of validators enabled by bids per phase', labelnames=['phase'])
    
    def do(self):
        logger.info('Doing EtherFi Bids Exporter')
        while True:
            self.record_bids()
            self.get_our_bids()
            self.get_active_bids()
            self.get_validators_phase()
            logger.info(f'Sleeping {self.fetching_bids_interval_minutes}min')
            time.sleep(self.fetching_bids_interval_minutes * 60)

    def record_bids(self):
        all_bids = list()
        start = 0
        interval = 1000

        while True:
            result = self.api_post_request(json_payload={
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
            })

            if ('data' not in result) or ('bids' not in result.get('data')) or (len(result.get('data').get('bids')) < 1):
                break
            
            bids = result['data']['bids']
            all_bids.extend(bids)
            start += interval
        
        all_bids = sorted(all_bids, key=lambda x: int(x['pubKeyIndex']))
        logger.info(f'Fetcted Bids: {len(all_bids)}')

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

        for bid in all_bids:
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
        conn.close()
        logger.info(f'Recorded Bids: {len(all_bids)}')

    def get_our_bids(self):
        conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'etherfi-bids.db'))
        cursor = conn.cursor()

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
                continue
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

        conn.close()

    def get_active_bids(self):
        result = self.api_post_request(json_payload={
            'query': '''
            {
                othersMin: bids(where: {bidderAddress_not: "%s", status: "ACTIVE"}, orderBy: amount, orderDirection: asc, first: 1) {
                    bidderAddress
                    amount
                }
                othersMax: bids(where: {bidderAddress_not: "%s", status: "ACTIVE"}, orderBy: amount, orderDirection: desc, first: 1) {
                    bidderAddress
                    amount
                }
            }
            ''' % (self.bidder_address, self.bidder_address)
        })

        if ('data' not in result) or ('othersMin' not in result.get('data')) or ('othersMax' not in result.get('data')) or (len(result.get('data').get('othersMin')) < 1) or (len(result.get('data').get('othersMax')) < 1):
            return

        others_min_bidder = result['data']['othersMin'][0]['bidderAddress']
        others_min_amount = result['data']['othersMin'][0]['amount']
        self.bids_amount_min.labels(bidder_address=others_min_bidder, status='active').set(others_min_amount)
        logger.info(f'Minimum Amount of Others Active Bids by {others_min_bidder}: {others_min_amount}')

        others_max_bidder = result['data']['othersMax'][0]['bidderAddress']
        others_max_amount = result['data']['othersMax'][0]['amount']
        self.bids_amount_max.labels(bidder_address=others_max_bidder, status='active').set(others_max_amount)
        logger.info(f'Maximum Amount of Others Active Bids by {others_max_bidder}: {others_max_amount}')

        conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'etherfi-bids.db'))
        cursor = conn.cursor()

        cursor.execute(f'''
        SELECT MIN(amount), MAX(amount)
        FROM Bid
        WHERE bidderAddress = '{self.bidder_address}' COLLATE NOCASE
            AND status = 'ACTIVE'
        ''')
        row = cursor.fetchone()
        conn.close()

        our_min_amount = 0
        our_max_amount = 0

        if row:
            our_min_amount, our_max_amount = row
        else:
            logger.info(f'No Our Active Bids in DB')

        self.bids_amount_min.labels(bidder_address=self.bidder_address, status='active').set(our_min_amount)
        logger.info(f'Minimum Amount of Our Active Bids by {self.bidder_address}: {our_min_amount}')

        self.bids_amount_max.labels(bidder_address=self.bidder_address, status='active').set(our_max_amount)
        logger.info(f'Maximum Amount of Our Active Bids by {self.bidder_address}: {our_max_amount}')

    def get_validators_phase(self):
        conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'etherfi-bids.db'))
        cursor = conn.cursor()

        cursor.execute(f'''
        SELECT Validator.phase, COUNT(*)
        FROM Validator
        JOIN Bid ON Bid.id = Validator.bid_id
        WHERE Bid.bidderAddress = '{self.bidder_address}' COLLATE NOCASE
        GROUP BY Validator.phase
        ''')
        rows = cursor.fetchall()

        phase_counts = {phase: 0 for phase in ['NOT_INITIALIZED', 'STAKE_DEPOSITED', 'WAITING_FOR_APPROVAL', 'LIVE', 'BEING_SLASHED', 'EXITED', 'FULLY_WITHDRAWN', 'CANCELLED', 'EVICTED', 'READY_FOR_DEPOSIT']}
        for phase, count in rows:
            if phase not in phase_counts:
                logger.info(f'Unknown Validator Phase: {phase} ({count})')
                continue
            phase_counts[phase] = count

        for phase, count in phase_counts.items():
            self.validators_phase.labels(phase=phase.lower()).set(count)
            logger.info(f'Phase {phase} Validators: {count}')

        conn.close()

    def api_post_request(self, json_payload):
        response = requests.post(
            self.api_url, 
            headers={'Content-Type': 'application/json'},
            json=json_payload,
            timeout=10
        )

        if response.status_code != 200:
            logger.info(f'Failed API Response [{response.status_code}] {response.text}')
            self.api_health.set(0)
            return None
        
        self.api_health.set(1)
        return response.json()

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
