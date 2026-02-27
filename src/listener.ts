// src/listener.ts
import { WebSocketProvider, JsonRpcProvider, Log } from "ethers";
import { CONFIG, INDEXED_ADDRESSES } from "./config";
import { iface } from "./abi";
import { ch } from "./db";

import {
  handleOrderFilled,
  handlePayoutRedemption,
  handleConditionResolution,
  handlePositionSplit
} from "./indexer";

/* ------------------------------------------------ */
/* STATE                                           */
/* ------------------------------------------------ */

let lastProcessedBlock = CONFIG.START_BLOCK;
let rawBuffer: any[] = [];

const MAX_BUFFER = CONFIG.MAX_BUFFER;
const FLUSH_INTERVAL = CONFIG.FLUSH_INTERVAL;

/* ------------------------------------------------ */
/* BigInt Safe JSON                                */
/* ------------------------------------------------ */

function safeStringify(obj: any): string {
  return JSON.stringify(obj, (_, value) =>
    typeof value === "bigint" ? value.toString() : value
  );
}

/* ------------------------------------------------ */
/* DB Progress Recovery                            */
/* ------------------------------------------------ */

async function getLastProcessedBlock(): Promise<number> {
  try {
    const result = await ch.query({
      query: "SELECT max(block_number) AS max FROM raw_events",
      format: "JSONEachRow",
    });

    const rows = await result.json() as Array<{ max: number | null }>;
    const block = rows[0]?.max;

    if (!block) {
      console.log("No previous progress → using START_BLOCK");
      return CONFIG.START_BLOCK;
    }

    console.log(`Recovered lastProcessedBlock = ${block}`);
    return Number(block);

  } catch {
    console.warn("Failed to load progress → using START_BLOCK");
    return CONFIG.START_BLOCK;
  }
}

/* ------------------------------------------------ */
/* RAW EVENT TRANSFORM                             */
/* ------------------------------------------------ */

function toRow(log: Log, parsed: any) {
  return {
    block_number: log.blockNumber,
    tx_hash: log.transactionHash,
    log_index: log.index,
    contract: log.address,
    event_name: parsed.name,
    data: safeStringify(parsed.args),
  };
}

/* ------------------------------------------------ */
/* RAW BUFFER FLUSH                                */
/* ------------------------------------------------ */

async function flushRaw() {
  if (!rawBuffer.length) return;

  const batch = rawBuffer;
  rawBuffer = [];

  try {
    await ch.insert({
      table: "raw_events",
      values: batch,
      format: "JSONEachRow",
    });

    if (CONFIG.DEBUG) {
      console.log(`Inserted ${batch.length} raw events`);
    }

  } catch (err) {
    console.error("Raw insert failed:", err);
    rawBuffer.unshift(...batch);
  }
}

setInterval(flushRaw, FLUSH_INTERVAL);

/* ------------------------------------------------ */
/* LOG PROCESSING                                  */
/* ------------------------------------------------ */

async function processLog(log: Log) {
  try {
    const parsed = iface.parseLog(log);
    if (!parsed) return;

    const blockNumber = log.blockNumber;

    /* ===================== */
    /* DERIVED LOGIC ROUTING */
    /* ===================== */
    console.log("parsed.name", parsed.name)
    switch (parsed.name) {

      case "PositionSplit":
        handlePositionSplit(parsed);
        break;

      case "OrderFilled":
        handleOrderFilled(parsed, blockNumber);
        break;

      case "PayoutRedemption":
        handlePayoutRedemption(parsed, blockNumber);
        break;

      case "ConditionResolution":
        handleConditionResolution(parsed, blockNumber);
        break;

      default:
        break;
    }

    /* Store raw event (optional but recommended) */
    rawBuffer.push(toRow(log, parsed));

    if (rawBuffer.length >= MAX_BUFFER) {
      await flushRaw();
    }

    lastProcessedBlock = Math.max(lastProcessedBlock, blockNumber);

  } catch (err) {
    console.error("Error processing log:", err);
  }
}

/* ------------------------------------------------ */
/* BACKFILL                                        */
/* ------------------------------------------------ */

async function backfill(provider: JsonRpcProvider, from: number, to: number) {
  for (let start = from; start <= to; start += CONFIG.BACKFILL_CHUNK) {
    const end = Math.min(start + CONFIG.BACKFILL_CHUNK - 1, to);

    console.log(`Backfill ${start} → ${end}`);

    const logs = await provider.getLogs({
      address: INDEXED_ADDRESSES,
      fromBlock: start,
      toBlock: end,
    });

    for (const log of logs) {
      await processLog(log);
    }

    if (CONFIG.RPC_DELAY_MS) {
      await new Promise(r => setTimeout(r, CONFIG.RPC_DELAY_MS));
    }
  }
}

/* ------------------------------------------------ */
/* MAIN RUNTIME                                    */
/* ------------------------------------------------ */

async function start() {

  const ws = new WebSocketProvider(CONFIG.POLYGON_WSS);
  const http = new JsonRpcProvider(CONFIG.POLYGON_HTTP);

  /* Load previous progress */
  lastProcessedBlock = await getLastProcessedBlock();

  const head = await http.getBlockNumber();

  const safeStart = Math.max(
    lastProcessedBlock - CONFIG.REORG_SAFETY_BLOCKS,
    CONFIG.START_BLOCK
  );

  console.log(`Starting backfill from ${safeStart} → ${head}`);

  await backfill(http, safeStart, head);

  console.log("WS listening…");

  ws.on({ address: INDEXED_ADDRESSES }, processLog);

  (ws.websocket as any).onclose = async () => {
    console.warn("WS disconnected → recovering");

    ws.removeAllListeners();

    const latest = await http.getBlockNumber();

    const resumeFrom = Math.max(
      lastProcessedBlock - CONFIG.REORG_SAFETY_BLOCKS,
      CONFIG.START_BLOCK
    );

    console.log(`Recovery backfill ${resumeFrom} → ${latest}`);

    await backfill(http, resumeFrom, latest);

    start(); // restart
  };
}

start().catch(console.error);