// src/ws.ts
import {
    WebSocketProvider,
    JsonRpcProvider,
    Log,
    keccak256,
    solidityPacked
} from "ethers";

import { CONFIG } from "./config";
import { decodeLog } from "./decoder";
import {
    insertTrade,
    insertResolution,
    insertConditionPartitions,
} from "./insert";

/* ---------------- PROVIDER ---------------- */

const http = new JsonRpcProvider(CONFIG.POLYGON_HTTP);

/* ---------------- CONSTANTS ---------------- */

const PRICE_SCALE = 1_000_000n;
const BACKFILL_CHUNK = 500;

const ZERO_BYTES32 =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

/* ---------------- CONFIG ---------------- */

const RECONNECT_DELAY = 2000;
const HEARTBEAT_INTERVAL = 15_000;
const HEARTBEAT_TIMEOUT = 30_000;

const MAX_BATCH = 500;
const FLUSH_INTERVAL = 200;

/* ---------------- STATE ---------------- */

let lastBlockTime = Date.now();
let lastProcessedBlock = 0;
let processingBlock = false;

/* ---------------- BUFFERS ---------------- */

const tradeBuffer: any[] = [];
const resolutionBuffer: any[] = [];
const partitionBuffer: any[] = [];

/* ---------------- HELPERS ---------------- */

function computePositionId(
    collateralToken: string,
    conditionId: string,
    indexSet: bigint
) {
    const collectionId = keccak256(solidityPacked(
        ["bytes32", "bytes32", "uint256"],
        [ZERO_BYTES32, conditionId, indexSet]
    ));

    return keccak256(solidityPacked(
        ["address", "bytes32"],
        [collateralToken, collectionId]
    ));
}

/* ---------------- BLOCK CACHE ---------------- */

const blockCache = new Map<number, number>();

async function getBlockTimestamp(blockNumber: number): Promise<number> {
    const cached = blockCache.get(blockNumber);
    if (cached) return cached;

    const block = await http.getBlock(blockNumber);
    if (!block) throw new Error(`Missing block ${blockNumber}`);

    blockCache.set(blockNumber, block.timestamp);

    if (blockCache.size > 10_000) {
        const firstKey = blockCache.keys().next().value;
        blockCache.delete(firstKey!);
    }

    return block.timestamp;
}

/* ---------------- FLUSH LOOP ---------------- */

setInterval(async () => {
    try {
        await flushTrades();
        await flushResolutions();
        await flushPartitions();
    } catch (err) {
        console.error("Flush error:", err);
    }
}, FLUSH_INTERVAL);

/* ---------------- WS LOOP ---------------- */

export async function startWS() {
    while (true) {
        let wsProvider: WebSocketProvider | null = null;
        let heartbeatTimer: NodeJS.Timeout;

        try {
            console.log("Connecting WS...");

            wsProvider = new WebSocketProvider(CONFIG.POLYGON_WSS);
            await wsProvider._waitUntilReady();

            console.log("WS READY");

            if (!lastProcessedBlock) {
                lastProcessedBlock = await http.getBlockNumber();
                console.log("Starting from block", lastProcessedBlock);
            }

            wsProvider.on("block", async (blockNumber) => {
                lastBlockTime = Date.now();

                if (processingBlock) return;
                processingBlock = true;

                try {
                    await backfill(lastProcessedBlock + 1, blockNumber);
                    lastProcessedBlock = blockNumber;
                } catch (err) {
                    console.log("Backfill error:", err);
                } finally {
                    processingBlock = false;
                }
            });

            wsProvider.on({
                address: [
                    CONFIG.CONTRACTS.CTF_EXCHANGE,
                    CONFIG.CONTRACTS.NEG_RISK_CTF_EXCHANGE,
                    CONFIG.CONTRACTS.CONDITIONAL_TOKENS
                ],
                topics: [null]
            }, handleLog);

            heartbeatTimer = setInterval(() => {
                if (Date.now() - lastBlockTime > HEARTBEAT_TIMEOUT) {
                    console.warn("Heartbeat timeout — reconnecting...");
                    wsProvider?.destroy();
                }
            }, HEARTBEAT_INTERVAL);

            await new Promise<void>((resolve) => {
                const nativeWS = wsProvider!.websocket as any;
                nativeWS.on("close", resolve);
                nativeWS.on("error", resolve);
            });

            clearInterval(heartbeatTimer);
            console.log("WS DISCONNECTED");

        } catch (err) {
            console.log("WS ERROR:", err);
            try { wsProvider?.destroy(); } catch {}
        }

        console.log(`Reconnecting in ${RECONNECT_DELAY} ms...`);
        await sleep(RECONNECT_DELAY);
    }
}

/* ---------------- BACKFILL ---------------- */

async function backfill(from: number, to: number) {
    if (to < from) return;

    for (let start = from; start <= to; start += BACKFILL_CHUNK) {

        const end = Math.min(start + BACKFILL_CHUNK - 1, to);

        const logs = await http.getLogs({
            fromBlock: start,
            toBlock: end,
            address: [
                CONFIG.CONTRACTS.CTF_EXCHANGE,
                CONFIG.CONTRACTS.NEG_RISK_CTF_EXCHANGE,
                CONFIG.CONTRACTS.CONDITIONAL_TOKENS
            ],
        });

        for (const log of logs) {
            await handleLog(log);
        }
    }
}

/* ---------------- LOG HANDLER ---------------- */

async function handleLog(log: Log) {
    const decoded = decodeLog(log);
    if (!decoded) return;

    const ts = await getBlockTimestamp(log.blockNumber);
    const timestamp = new Date(ts * 1000);

    /* ---------------- TRADES ---------------- */

    if (decoded.kind === "trade") {

        const makerAssetId = BigInt(decoded.makerAssetId);
        const takerAssetId = BigInt(decoded.takerAssetId);

        const makerAmount = BigInt(decoded.makerAmountFilled);
        const takerAmount = BigInt(decoded.takerAmountFilled);

        let side: "buy" | "sell";
        let priceScaled: bigint;
        let size: bigint;
        let notional: bigint;
        let asset_id: bigint;

        if (makerAssetId === 0n) {
            size = takerAmount;
            notional = makerAmount;
            if (size === 0n) return;

            priceScaled = (notional * PRICE_SCALE) / size;
            side = "buy";
            asset_id = takerAssetId;

        } else if (takerAssetId === 0n) {
            size = makerAmount;
            notional = takerAmount;
            if (size === 0n) return;

            priceScaled = (notional * PRICE_SCALE) / size;
            side = "sell";
            asset_id = makerAssetId;

        } else return;

        tradeBuffer.push({
            trade_id: `${log.transactionHash}:${log.index}`,

            tx_hash: log.transactionHash,
            tx_index: log.transactionIndex,
            log_index: log.index,

            block_number: log.blockNumber,
            block_hash: log.blockHash,
            timestamp,

            trader: decoded.taker.toLowerCase(),
            asset_id: "0x" + asset_id.toString(16).padStart(64, "0"),

            side,

            price_scaled: priceScaled.toString(),
            price_scale: PRICE_SCALE.toString(),

            size_raw: size.toString(),
            notional_raw: notional.toString(),
        });

        if (tradeBuffer.length >= MAX_BATCH)
            await flushTrades();
    }

    /* ---------------- SPLITS / MERGES ---------------- */

    if (decoded.kind === "position_split" || decoded.kind === "positions_merge") {

        for (const indexSet of decoded.partition ?? []) {

            const positionId = computePositionId(
                decoded.collateralToken,
                decoded.conditionId,
                BigInt(indexSet)
            );

            partitionBuffer.push({
                position_id: positionId.toLowerCase(),
                condition_id: decoded.conditionId.toLowerCase(),
                index_set: indexSet.toString(),

                collateral_token: decoded.collateralToken.toLowerCase(),
                parent_collection_id: decoded.parentCollectionId.toLowerCase(),

                tx_hash: log.transactionHash,
                tx_index: log.transactionIndex,   // ✅ FIX (needed by worker ordering)
                log_index: log.index,             // ✅ FIX (needed by worker ordering)

                block_number: log.blockNumber,
                block_hash: log.blockHash,
                timestamp,
            });
        }

        if (partitionBuffer.length >= MAX_BATCH)
            await flushPartitions();
    }

    /* ---------------- CONDITION RESOLUTION ---------------- */

    if (decoded.kind === "condition_resolution") {

        resolutionBuffer.push({
            resolution_id: `${log.transactionHash}:${log.index}`,

            tx_hash: log.transactionHash,
            tx_index: log.transactionIndex,   // ✅ FIX (needed by worker ordering)
            log_index: log.index,             // ✅ FIX (needed by worker ordering)

            block_number: log.blockNumber,
            block_hash: log.blockHash,
            timestamp,

            condition_id: decoded.conditionId.toLowerCase(),
            payouts: decoded.payoutNumerators,

            event_type: "resolved",
        });

        if (resolutionBuffer.length >= MAX_BATCH)
            await flushResolutions();
    }

    /* ---------------- PAYOUT REDEMPTION ---------------- */

    if (decoded.kind === "payout_redemption") {

        resolutionBuffer.push({
            resolution_id: `${log.transactionHash}:${log.index}`,

            tx_hash: log.transactionHash,
            tx_index: log.transactionIndex,   // ✅ FIX (needed by worker ordering)
            log_index: log.index,             // ✅ FIX (needed by worker ordering)

            block_number: log.blockNumber,
            block_hash: log.blockHash,
            timestamp,

            condition_id: decoded.conditionId.toLowerCase(),
            redeemer: decoded.redeemer.toLowerCase(),
            index_sets: decoded.indexSets,
            payout: decoded.payout,

            event_type: "redemption",
        });

        if (resolutionBuffer.length >= MAX_BATCH)
            await flushResolutions();
    }
}

/* ---------------- FLUSH ---------------- */

async function flushTrades() {
    if (!tradeBuffer.length) return;
    await insertTrade(tradeBuffer.splice(0));
}

async function flushResolutions() {
    if (!resolutionBuffer.length) return;
    await insertResolution(resolutionBuffer.splice(0));
}

async function flushPartitions() {
    if (!partitionBuffer.length) return;
    await insertConditionPartitions(partitionBuffer.splice(0));
}

/* ---------------- UTILS ---------------- */

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}