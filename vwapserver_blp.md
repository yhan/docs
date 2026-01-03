Building a resilient VWAP system on top of a data feed requires shifting
from a \"direct processing\" mindset to an **Event Sourcing**
architecture. Since your VWAP is \"custom\" (applying filters like
excluding dark venues), you cannot rely on Bloomberg\'s pre-calculated
fields to fix gaps; you *must* possess every single raw tick to
calculate the numerator (Price × Volume) and denominator (Volume)
correctly.

Here is a resilient architecture pattern designed to survive network
partitions, server crashes, and API disconnects.

**1. High-Level Architecture: The \"Capture-Log-Process\" Pattern**

Instead of your VWAP server connecting directly to Bloomberg, you should
decouple the **Ingestion** (getting data) from the **Calculation**
(math).

**The Pipeline:** Bloomberg API → **Feed Handler** → **Persistent Log
(Kafka/Redpanda)** → **VWAP Engine**

**Why this resists downtime:**

- **If the VWAP Server dies:** The Feed Handler keeps writing to Kafka.
  When the VWAP server restarts, it just \"replays\" the messages it
  missed from the log.

- **If the Connection dies:** The Feed Handler manages the reconnection
  and \"Gap Fill\" logic before pushing downstream. The VWAP server just
  sees a pause, then a burst of data, but no data loss.

------------------------------------------------------------------------

**2. Component Deep Dive**

**A. The Feed Handler (The \"Guardian\")**

This is a lightweight process whose *only* job is to maintain the
session with Bloomberg and publish raw trade messages to the Log.

- **Responsibility:** It subscribes to the Trade stream.

- **Sequence Numbers:** Bloomberg B-Pipe messages contain sequence
  numbers. The Feed Handler must track Last_Seq_Num.

- **Gap Detection Logic:**

  1.  **Disconnect:** Connection to Bloomberg drops.

  2.  **Buffer:** The Feed Handler enters a \"Disconnected\" state.

  3.  **Reconnect:** Connection is restored.

  4.  **Check:** Compare the new message\'s sequence number with
      Last_Seq_Num.

  5.  **Recovery:** If there is a gap, **do not** just resume. You must
      trigger a **\"Backfill Request\"** (Intraday Tick Request) for the
      missing time window.

  6.  **Publish:** Publish the \"Backfilled\" ticks to Kafka first, then
      resume publishing live ticks.

**Note:** Tag your messages in Kafka with flag: historical or flag: live
so downstream systems know if they are processing real-time data or
catching up.

**B. The Persistent Log (The \"Truth\")**

Use a distributed log like **Kafka** or **Redpanda**.

- **Topic Design:** Create a topic market_data_trades. Partition by
  Stock_Ticker (e.g., AAPL goes to Partition 1). This ensures all trades
  for one stock arrive in strict order, which is critical for VWAP.

- **Retention:** Set retention to at least 1 day (since VWAP resets
  daily).

**C. The VWAP Engine (The \"Calculator\")**

This service consumes the Kafka topic. It holds the state (Cumulative
Volume, Cumulative Notional) in memory.

- **The Filter Logic:**

def on_trade_message(trade):

if trade.venue_type == \'DARK\' and config.filter_dark == True:

return \# Ignore this trade for VWAP

state.total_volume += trade.volume

state.total_notional += (trade.price \* trade.volume)

state.vwap = state.total_notional / state.total_volume

- **Resilience (Crash Recovery):** If this server crashes, you lose the
  in-memory variables. To fix this:

  - **Option 1 (Replay):** On restart, re-read the Kafka topic from the
    beginning of the day (offset 0). It\'s fast to recompute millions of
    simple math operations.

  - **Option 2 (Snapshots):** Every minute, write the current
    Total_Volume and Total_Notional to a fast KV store (like Redis). On
    restart, load from Redis and consume Kafka starting from the
    snapshot timestamp.

**3. Handling Specific \"Down\" Scenarios**

  -----------------------------------------------------------------------
  **Failure           **Solution Mechanism**
  Scenario**          
  ------------------- ---------------------------------------------------
  **VWAP Server       **Kafka Replay.** The stream is safe in Kafka.
  Crash**             Restart the service, rewind to start-of-day (or
                      last checkpoint), and catch up.

  **Network Blink     **TCP Buffer.** The standard TCP stack + Bloomberg
  (Milliseconds)**    API buffer handles tiny drops.

  **Bloomberg API     **Gap Fill.** The Feed Handler detects the gap. It
  Disconnect          pauses the live stream, requests historical ticks
  (Minutes)**         for the outage window via //blp/refdata (Intraday
                      Tick Request), publishes them to Kafka, and then
                      resumes live publishing.

  **Bad Data / Filter **Reprocessing.** If you realize at 2 PM that you
  Change**            should have filtered \"Off-Exchange\" trades too,
                      you can spin up a *new* VWAP Engine with new rules
                      and replay the same Kafka stream from the morning
                      to get the corrected values instantly.
  -----------------------------------------------------------------------

**4. Diagram of the Resilient Flow**

graph LR

BBG\[Bloomberg Server\] \-- \"TCP/IP\" \--\> FH\[Feed Handler\]

FH \-- \"Raw Trades\" \--\> K\[Kafka / Log\]

subgraph \"Resilient Core\"

K \-- \"Offset 100\...\" \--\> V1\[VWAP Engine A\]

K \-- \"Offset 100\...\" \--\> V2\[VWAP Engine B\]

end

V1 \-- \"Publish VWAP\" \--\> DB\[(Redis/DB)\]

style BBG fill:#f9f,stroke:#333

style K fill:#orange,stroke:#333

style FH fill:#add,stroke:#333

**5. Essential \"Gotcha\" to Avoid**

**Don\'t filter at the Source.** Do not put your \"Dark Venue\" filter
in the Feed Handler.

- **Why?** If your business requirement changes (e.g., \"Actually,
  let\'s include Dark Venues\"), you are stuck because you discarded the
  data.

- **Fix:** Log *everything* to Kafka. Apply the filters in the VWAP
  Engine. This allows you to run multiple VWAP calculations (e.g.,
  \"VWAP Lit\" and \"VWAP All\") from the same single data source
  without reconnecting to Bloomberg.

**Using NATS**

Yes, absolutely. In fact, for a **VWAP computation system** where you
need low latency and a simpler operational footprint than Kafka (no
Zookeeper/KRaft, no JVM), **NATS JetStream** is often a superior choice
in 2026.

You must specifically use **JetStream** (the persistence engine), not
Core NATS (which is fire-and-forget).

Here is how you re-architect the \"Resilient VWAP\" system using NATS
JetStream to replace both Kafka and Redis.

**1. The NATS Advantage for this specific use case**

- **Subject Hierarchy:** unlike Kafka\'s flat \"Topics,\" NATS uses
  subjects like deals.us.equity.AAPL. This allows your VWAP engine to
  subscribe to deals.us.equity.\* or just deals.us.equity.AAPL without
  needing complex partition management.

- **Built-in KV Store:** NATS JetStream has a built-in Key-Value store.
  You can store your \"VWAP State Snapshots\" directly in NATS,
  eliminating the need for a separate Redis instance.

**2. The NATS Architecture: \"Stream-Based Event Sourcing\"**

**Step A: Configure the Stream (The Log)**

Instead of a Kafka Topic, you create a **JetStream Stream**.

- **Stream Name:** MARKET_DATA

- **Subjects:** deals.\> (The \> wildcard captures everything:
  deals.us.AAPL, deals.eu.SAN\...)

- **Storage:** File (Persistent on disk)

- **Retention:** Limits (MaxAge = 24h, since VWAP resets daily)

**Step B: The Feed Handler (Publisher)**

The Feed Handler connects to Bloomberg and publishes messages to
specific subjects.

\# Feed Handler Logic

subject = f\"deals.{market}.{ticker}\" \# e.g., deals.us.AAPL

\# NATS guarantees ordering per subject

js.publish(subject, payload=deal_data)

**Step C: The VWAP Engine (Consumer)**

This is where NATS shines. You use a **Durable Consumer** to ensure you
never miss a message, even if the VWAP server restarts.

- **Durable Name:** VWAP_PROCESSOR_US

- **Filter Subject:** deals.us.\> (Only process US deals)

- **Ack Policy:** Explicit (Only delete from the \"pending\" list once
  processed)

**3. Resilience Scenarios with NATS**

**Scenario 1: VWAP Server Crash & Restart**

In Kafka, you have to manage offsets manually or rely on consumer
groups. In NATS, the **Durable Consumer** tracks this for you on the
server side.

- **Action:** When your VWAP server comes back online, it simply binds
  to the existing Durable Consumer VWAP_PROCESSOR_US.

- **Result:** NATS immediately delivers all the \"Unacknowledged\"
  messages that accumulated while you were down.

**Scenario 2: Replay for \"Correction\"**

Imagine you find a bug in your math at 12:00 PM. You need to recalculate
VWAP from 9:30 AM.

- **Action:** You create a *new* Ephemeral Consumer with a specific
  start policy.

\# Replay from morning open

sub = js.subscribe(\"deals.us.\>\",

deliver_policy=DeliverPolicy.ByStartTime,

opt_start_time=\"2026-01-03T09:30:00Z\")

This allows you to replay the \"tape\" instantly without affecting the
live production consumer.

**Scenario 3: Eliminating Redis (State Snapshots)**

To make start-up faster, you don\'t want to replay 5 million trades. You
want the last known state.

- **Kafka approach:** Read Kafka + Write to Redis.

- **NATS approach:** Use **NATS KV**.

  - Bucket: VWAP_STATE

  - Key: AAPL -\> Value: {\"vol\": 50000, \"notional\": 4750000}

  - *Every 10 seconds, the VWAP engine saves its state to this KV
    bucket.*

  - *On restart, it loads from KV, then consumes the Stream starting
    from the KV timestamp.*

**4. Code Concept (Python with nats-py)**

Here is the resilient loop using NATS JetStream.

import asyncio

import nats

from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy

async def run_vwap_engine():

nc = await nats.connect(\"nats://localhost:4222\")

js = nc.jetstream()

\# 1. Ensure the Stream exists (The \"Topic\")

await js.add_stream(name=\"MARKET_DATA\", subjects=\[\"deals.\>\"\],
storage=\"file\")

\# 2. Create/Bind to a Durable Consumer (Resilience)

\# This ensures if we crash, NATS remembers where we left off.

\# We filter only for \"US\" stocks here.

sub = await js.pull_subscribe(

subject=\"deals.us.\>\",

durable=\"VWAP_ENGINE_US\",

config=ConsumerConfig(

ack_policy=AckPolicy.EXPLICIT,

deliver_policy=DeliverPolicy.ALL, \# Or LastPerSubject for current price

)

)

while True:

try:

\# Fetch batch of messages

msgs = await sub.fetch(batch=10, timeout=1)

for msg in msgs:

data = decode(msg.data)

\# \-\-- YOUR VWAP LOGIC \-\--

if is_dark_venue(data):

pass \# Skip logic

else:

update_vwap(data)

\# \-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--

\# Acknowledge: \"I have safely processed this deal\"

await msg.ack()

except TimeoutError:

continue \# No new deals, keep looping

if \_\_name\_\_ == \'\_\_main\_\_\':

asyncio.run(run_vwap_engine())

**Summary: Kafka vs. NATS for VWAP**

  ------------------------------------------------------------------
  **Feature**           **Kafka**           **NATS JetStream**
  --------------------- ------------------- ------------------------
  **Operational         High (JVM,          **Low** (Single Go
  Complexity**          KRaft/ZK)           Binary)

  **Ordering**          By Partition        **By Subject** (Native &
                        (Complex)           Simple)

  **Filtering**         Client-side or KSQL **Server-side**
                                            (Wildcards)

  **State Storage**     External (Redis)    **Internal** (NATS KV)

  **Latency**           Millisecond         **Microsecond**

  **Throughput**        Extreme (Petabytes) **High** (Gigabytes)
  ------------------------------------------------------------------

**Recommendation:** For a dedicated VWAP calculation engine (market
data), **NATS is the better fit.** Kafka is often \"overkill\" for
market data feeds unless you are storing years of history. NATS gives
you the speed of a memory grid with the persistence of a log.

Example of filters VWAP server:

 **Venue / MIC** include/exclude list (e.g., exclude dark pools)

 **Trade conditions** (auction, opening/closing, out-of-sequence,
negotiated, etc.)

 **Odd-lot rules** (exclude lots \< board-lot; depends on market)

 **Correction / cancel** handling (important if the feed sends
bust/correct messages)

 **Currency / units** normalization (avoid mixing if your stream can
vary)

 **Session/time window** (market hours vs extended)

**Testing Resilience**

You can actually simulate failures to prove the system works:

- **Chaos Engineering**: Randomly disconnect Bloomberg for 30 seconds
  during trading hours

- **Load Testing**: Flood the system with 10x normal volume

- **Recovery Drills**: Kill VWAP engines and verify they restart
  correctly

**Criterias of success:**

1.  **No Single Point of Failure**: Every component has redundancy

2.  **Data Never Lost**: Multiple copies at every stage

3.  **Automatic Recovery**: Human intervention isn\'t needed for common
    failures

4.  **Accuracy Maintained**: Gap detection ensures complete data sets

5.  **Scalable**: Can add new filter types without redesign

6.  **Auditable**: Every deal and calculation state can be traced
