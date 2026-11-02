import asyncio
import json
import logging
import os
import time
import aiohttp
import pandas as pd
from datetime import datetime, timedelta, timezone, date
import random
import re
import csv
from urllib.parse import urlencode
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# Kafka Producer Import
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("websocket_kafka_producer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("websocket_kafka_producer")

# --- KAFKA CONFIGURATION ---
KAFKA_BROKER = '32.195.69.185:9092'
KAFKA_TOPIC = 'Ticket-DataPipeline-Topic'

CSV_INPUT = r'C:\Users\Hp\Desktop\Cloud Data Engineering\Computickets DataPipeline using Apache Kafka\Computickets\locations.csv'


def extract_token():
    """Extract JWT token using Selenium"""
    logger.info("Extracting JWT token using Selenium...")

    driver = None
    try:
        options = Options()
        # options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-setuid-sandbox')
        options.add_argument('--window-size=1920,1080')
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])

        service = Service(r"C:\Users\Hp\.cache\selenium\chromedriver\win64\139.0.7258.154\chromedriver.exe")
        driver = webdriver.Chrome(service=service, options=options)

        driver.get(
            "https://computicket.com/travel/busses/search?"
            "from=ZAZABLOEMFONTEIN&to=ZAZAALIWALNORTH"
            "&date=2025-06-24&adult=1"
        )

        time.sleep(10)

        tdata_raw = driver.execute_script("return localStorage.getItem('tData')")
        if tdata_raw:
            try:
                tdata = json.loads(tdata_raw)
                token = tdata.get("access_token")
                session_state = tdata.get("session_state", "")
                if token:
                    logger.info("Successfully extracted token from localStorage")
                    return token, session_state
            except json.JSONDecodeError:
                logger.warning("Could not parse tData JSON")

        auth_token_raw = driver.execute_script("return localStorage.getItem('auth_token')")
        if auth_token_raw:
            try:
                auth_data = json.loads(auth_token_raw)
                token = auth_data.get("access_token")
                session_state = auth_data.get("session_state", "")
                if token:
                    logger.info("Successfully extracted auth_token")
                    return token, session_state
            except json.JSONDecodeError:
                logger.warning("Could not parse auth_token JSON")

        tdata_raw = driver.execute_script("return sessionStorage.getItem('tData')")
        if tdata_raw:
            try:
                tdata = json.loads(tdata_raw)
                token = tdata.get("access_token")
                session_state = tdata.get("session_state", "")
                if token:
                    logger.info("Successfully extracted token from sessionStorage")
                    return token, session_state
            except json.JSONDecodeError:
                logger.warning("Could not parse sessionStorage tData")

        page_source = driver.page_source
        token_match = re.search(r'"access_token"\s*:\s*"([^"]+)"', page_source)
        if token_match:
            token = token_match.group(1)
            session_match = re.search(r'"session_state"\s*:\s*"([^"]+)"', page_source)
            session_state = session_match.group(1) if session_match else ""
            logger.info("Found token in page source")
            return token, session_state

        logger.warning("Could not extract token using any method")
        return None, None

    except Exception as e:
        logger.error(f"Error extracting token: {str(e)}")
        return None, None
    finally:
        if driver:
            driver.quit()


def get_date(ms):
    dt = datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(timezone(timedelta(hours=0)))
    return dt.date().isoformat()


def get_time(ms):
    dt = datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(timezone(timedelta(hours=0)))
    return dt.time().strftime('%H:%M:%S')


def load_routes():
    routes = []
    try:
        with open(CSV_INPUT, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                logger.error("Input CSV has no headers")
                return routes

            reader.fieldnames = [h.strip() for h in reader.fieldnames]

            for row in reader:
                depart_raw = row['Depart'].strip().replace(' ', '').upper()
                arrive_raw = row['Arrive'].strip().replace(' ', '').upper()

                routes.append({
                    "from": f"ZAZA{depart_raw}",
                    "to": f"ZAZA{arrive_raw}",
                    "from_display": row['Depart'],
                    "to_display": row['Arrive']
                })
        logger.info(f"Loaded {len(routes)} routes from {CSV_INPUT}")
    except Exception as e:
        logger.error(f"Error loading routes: {str(e)}")
    return routes


def get_dates():
    today = date.today()
    return [(today + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(4)]


class ComputicketKafkaProducer:

    def __init__(self, max_concurrent=20, timeout=15):
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.processed_count = 0
        self.start_time = time.time()

        self.jwt_token = ""
        self.session_state = "368f66d8-83fe-4355-8187-d9acb3864c68"
        self.session_cookies = (
            f"AUTH_SESSION_ID={self.session_state}; "
            f"KEYCLOAK_SESSION=CTK_HQ/{self.session_state}/{self.session_state}"
        )
        self.session = None

        # --- Kafka Producer Initialize ---
        logger.info(f"Connecting to Kafka Broker at {KAFKA_BROKER}...")
        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1,
            retries=3
        )
        logger.info("Kafka Producer initialized successfully.")

    def refresh_token(self):
        token, session_state = extract_token()
        if token:
            self.jwt_token = token
            if session_state:
                self.session_state = session_state
                self.session_cookies = (
                    f"AUTH_SESSION_ID={session_state}; "
                    f"KEYCLOAK_SESSION=CTK_HQ/{session_state}/{session_state}"
                )
            return True
        return False

    def random_string(self, length):
        chars = "abcdefghijklmnopqrstuvwxyz0123456789"
        return ''.join(random.choice(chars) for _ in range(length))

    async def process_all_routes(self, routes, dates):
        try:
            headers = {
                "Cookie": self.session_cookies,
                "Origin": "https://computicket.com",
                "Host": "svc-socket.computicket.com",
                "Connection": "Upgrade",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/133.0.0.0 Safari/537.36"
                ),
                "Upgrade": "websocket",
                "Sec-WebSocket-Version": "13",
                "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits"
            }

            if self.session is None:
                self.session = aiohttp.ClientSession()

            logger.info("Establishing WebSocket connection...")

            async with self.session.ws_connect(
                "wss://svc-socket.computicket.com",
                headers=headers,
                heartbeat=30,
                autoclose=False,
                autoping=True
            ) as ws:

                session_id = f"{int(time.time() * 1000)}-{self.random_string(20)}"
                session_data = {
                    "type": "sessionCreation",
                    "data": {"sessionId": session_id}
                }

                await ws.send_str(json.dumps(session_data))
                session_msg = await asyncio.wait_for(ws.receive(), timeout=3.0)

                if session_msg.type != aiohttp.WSMsgType.TEXT:
                    logger.error("Failed to establish WebSocket session")
                    return

                session_response = json.loads(session_msg.data)
                if session_response.get('type') == 'error' or 'auth' in str(session_response).lower():
                    logger.warning("Authentication error, refreshing token...")
                    if self.refresh_token():
                        return await self.process_all_routes(routes, dates)

                batch_size = 10
                total_routes = len(routes) * len(dates)
                pending_requests = {}
                logger.info(f"Processing {total_routes} route/date combinations")

                route_counter = 0

                for route in routes:
                    for date_str in dates:
                        from_location = route["from"]
                        to_location = route["to"]
                        from_display = route["from_display"]
                        to_display = route["to_display"]

                        message_id = f"{int(time.time() * 1000)}-{session_id}-{route_counter}"
                        route_counter += 1

                        inner_data = {
                            "metadata": {
                                "urlOnCreate": "computicket.com",
                                "channelId": "1960",
                                "channelType": "WEB",
                                "width": 725,
                                "height": 593,
                                "jwttoken": self.jwt_token,
                                "sessionId": session_id,
                                "messageId": message_id,
                                "productType": "bus",
                                "operation": "availability",
                                "catproductId": "0",
                                "profileId": "0"
                            },
                            "searches": [
                                {
                                    "tripType": "oneWay",
                                    "depart": from_location,
                                    "arrive": to_location,
                                    "departDate": date_str,
                                    "discounts": [{"id": "ADULT", "quantity": "1", "displayText": "Adult"}]
                                }
                            ]
                        }

                        availability_request = {
                            "type": "getAvailability",
                            "data": {"data": json.dumps(inner_data)}
                        }

                        await ws.send_str(json.dumps(availability_request))

                        scrape_url = (
                            "https://computicket.com/travel/busses/search?"
                            + urlencode({
                                'from': from_location,
                                'to': to_location,
                                'date': date_str,
                                'adult': 1
                            })
                        )

                        pending_requests[message_id] = {
                            "from": from_display,
                            "to": to_display,
                            "date": date_str,
                            "sent_time": time.time(),
                            "scrape_url": scrape_url
                        }

                        await asyncio.sleep(0.1)

                        if len(pending_requests) >= batch_size:
                            await self._process_websocket_responses(ws, pending_requests)

                while pending_requests:
                    await self._process_websocket_responses(ws, pending_requests, final=True)

                logger.info("All routes processed and published to Kafka!")

        except Exception as e:
            logger.error(f"Error in process_all_routes: {str(e)}")

    async def _process_websocket_responses(self, ws, pending_requests, final=False):
        timeout = 15 if final else 1.0
        max_age = 30

        try:
            start_time = time.time()
            while pending_requests and (final or time.time() - start_time < timeout):
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=0.5)

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        response_type = data.get("type")

                        message_id = None
                        if "data" in data and isinstance(data["data"], dict):
                            metadata = data["data"].get("metadata", {})
                            if isinstance(metadata, dict):
                                message_id = metadata.get("messageId")

                        if response_type in ["availabilityResponse", "avalibilityResponse", "multidayResponse"]:
                            for req_msg_id in list(pending_requests.keys()):
                                if message_id and req_msg_id.endswith(message_id.split("-")[-1]):
                                    request_info = pending_requests.pop(req_msg_id)

                                    formatted_rows = self._extract_formatted_rows(
                                        data,
                                        request_info["from"],
                                        request_info["to"],
                                        request_info["date"],
                                        request_info["scrape_url"]
                                    )

                                    # --- STREAMING DIRECTLY TO KAFKA ---
                                    if formatted_rows:
                                        for row in formatted_rows:
                                            self.producer.send(KAFKA_TOPIC, value=row)
                                            logger.info(
                                                f"[KAFKA PUBLISHED] {row['serviceNumber']} | "
                                                f"{row['departCity']} -> {row['arriveCity']} | R{row['price_total']}"
                                            )
                                        self.producer.flush()

                                    self.processed_count += 1
                                    break

                    elif msg.type in [aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR]:
                        logger.warning(f"WebSocket connection closed or error: {msg}")
                        break

                except asyncio.TimeoutError:
                    current_time = time.time()
                    expired_ids = [
                        msg_id for msg_id, req in pending_requests.items()
                        if current_time - req["sent_time"] > max_age
                    ]
                    for msg_id in expired_ids:
                        req = pending_requests.pop(msg_id)
                        logger.warning(f"Request for {req['from']} to {req['to']} on {req['date']} timed out")
                        self.processed_count += 1

                    if not pending_requests or not final:
                        break

        except Exception as e:
            logger.error(f"Error processing WebSocket responses: {str(e)}")

    def _extract_formatted_rows(self, response, from_display, to_display, date_str, scrape_url):
        formatted_rows = []
        try:
            if "data" in response and "availability" in response["data"]:
                availability = response["data"]["availability"]

                if isinstance(availability, str):
                    try:
                        availability = json.loads(availability)
                    except Exception:
                        return formatted_rows

                if not isinstance(availability, list):
                    return formatted_rows

                for a in availability:
                    try:
                        if not isinstance(a, dict):
                            continue

                        if not all(k in a for k in ['arrive', 'depart', 'price']):
                            continue

                        arrive_city = a['arrive']['stop']['city']
                        normalized_arrive_city = re.sub(r'[^A-Z]', '', arrive_city.upper())
                        normalized_expected = re.sub(r'[^A-Z]', '', to_display.upper().replace(' ', ''))

                        if normalized_arrive_city != normalized_expected:
                            continue

                        formatted_rows.append({
                            'scrape_url': scrape_url,
                            'serviceNumber': a.get('serviceNumber'),
                            'carrierName': a.get('carrierName'),
                            'carrierCode': a.get('carrierCode'),
                            'routeDesc': a.get('routeDesc'),
                            'totalDuration': a.get('totalDuration'),
                            'availableSeats': a.get('availableSeats'),
                            'price_total': a['price']['totalPrice'],
                            'currency': a['price']['currency'],
                            'departDate': get_date(a['depart']['dateTimeMS']),
                            'departtime': get_time(a['depart']['dateTimeMS']),
                            'departCity': a['depart']['stop']['city'],
                            'departDescription': a['depart']['stop']['description'],
                            'arriveDate': get_date(a['arrive']['dateTimeMS']),
                            'arrivetime': get_time(a['arrive']['dateTimeMS']),
                            'arriveCity': a['arrive']['stop']['city'],
                            'arriveDescription': a['arrive']['stop']['description'],
                            'ingestion_timestamp': datetime.now().isoformat()
                        })

                    except (KeyError, TypeError, ValueError):
                        continue

        except Exception as e:
            logger.error(f"Error extracting formatted rows: {str(e)}")

        return formatted_rows


async def main():
    logger.info("Starting Computicket WebSocket-to-Kafka streaming pipeline...")

    token, session_state = extract_token()
    routes = load_routes()
    dates = get_dates()

    if not routes:
        logger.error("No routes found. Please check Book1.csv.")
        return

    scraper = ComputicketKafkaProducer(max_concurrent=20, timeout=15)

    if token:
        logger.info("Using freshly extracted JWT token")
        scraper.jwt_token = token
        if session_state:
            scraper.session_state = session_state
            scraper.session_cookies = (
                f"AUTH_SESSION_ID={session_state}; "
                f"KEYCLOAK_SESSION=CTK_HQ/{session_state}/{session_state}"
            )
    else:
        logger.warning("Using fallback JWT token")

    try:
        await scraper.process_all_routes(routes, dates)
    finally:
        if scraper.session:
            await scraper.session.close()
        if scraper.producer:
            scraper.producer.close()
            logger.info("Kafka Producer connection closed.")


if __name__ == "__main__":
    import sys
    sys.setrecursionlimit(10000)
    asyncio.run(main())




