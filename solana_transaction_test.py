import asyncio
import websockets
import json
import aiohttp
import time

wss_url = "ws://localhost:8900"
https_url = "http://localhost:8899"

# Stats
response_times_with_delay = []
response_times_without_delay = []
null_counts_with_delay = 0
null_counts_without_delay = 0

# Max transactions to process
max_transactions = 1500

# Shared queue between producer and consumer
transaction_queue = asyncio.Queue()

async def get_transaction_details(session, transaction_hash):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            transaction_hash,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
        ]
    }
    headers = {"Content-Type": "application/json"}

    try:
        start_time = time.time()
        async with session.post(https_url, json=payload, headers=headers) as resp:
            resp.raise_for_status()
            response_json = await resp.json()
            response_time = time.time() - start_time
            is_null = 1 if response_json.get("result") is None else 0
            return response_json, response_time, is_null
    except aiohttp.ClientError as e:
        print(f"HTTP Request failed: {e}")
        return None, None, 1

async def process_transaction(transaction_hash, session):

    print(f"\n🧾 Processing transaction: {transaction_hash}")

    # Fire two parallel fetches: with delay and without delay
    async def with_delay():
        global null_counts_with_delay, null_counts_without_delay
        nonlocal transaction_hash
        await asyncio.sleep(1)
        result, rtime, is_null = await get_transaction_details(session, transaction_hash)
        response_times_with_delay.append(rtime)
        if is_null: null_counts_with_delay += 1
        print(f"🕓 With delay: {rtime:.2f}s")

    async def without_delay():
        global null_counts_with_delay, null_counts_without_delay
        nonlocal transaction_hash
        result, rtime, is_null = await get_transaction_details(session, transaction_hash)
        response_times_without_delay.append(rtime)
        if is_null: null_counts_without_delay += 1
        print(f"⚡️ Without delay: {rtime:.2f}s")

    await asyncio.gather(with_delay(), without_delay())

    if response_times_with_delay and response_times_without_delay:
        avg_delay = sum(response_times_with_delay) / len(response_times_with_delay)
        avg_no_delay = sum(response_times_without_delay) / len(response_times_without_delay)
        percentage_diff = ((avg_no_delay - avg_delay) / avg_no_delay) * 100

        print(f"\n📊 Averages after {len(response_times_with_delay)} txs:")
        print(f"  • With delay:     {avg_delay:.2f}s")
        print(f"  • Without delay:  {avg_no_delay:.2f}s")
        print(f"  • Difference:     {percentage_diff:.2f}%")
        print(f"  • Nulls: {null_counts_with_delay} (with), {null_counts_without_delay} (without)")

async def transaction_consumer():
    async with aiohttp.ClientSession() as session:
        for _ in range(max_transactions):
            transaction_hash = await transaction_queue.get()
            await process_transaction(transaction_hash, session)
            transaction_queue.task_done()

async def transaction_producer():
    try:
        async with websockets.connect(wss_url, timeout=300) as websocket:
            print(f"🌐 Connected to {wss_url}")

            subscription = json.dumps({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "logsSubscribe",
                "params": [
                    {"mentions": ["ComputeBudget111111111111111111111111111111"]},
                    {"commitment": "finalized"}
                ]
            })
            await websocket.send(subscription)
            print(f"📨 Sent subscription: {subscription}")

            while True:
                msg = await websocket.recv()
                data = json.loads(msg)

                if "params" in data and "result" in data["params"] and "value" in data["params"]["result"]:
                    logs = data["params"]["result"]["value"]
                    if isinstance(logs, dict) and "signature" in logs:
                        tx_hash = logs["signature"]
                        print(f"📥 New transaction: {tx_hash}")
                        await transaction_queue.put(tx_hash)
    except Exception as e:
        print(f"💥 WebSocket error: {e}")

async def main():
    await asyncio.gather(transaction_producer(), transaction_consumer())

asyncio.run(main())

