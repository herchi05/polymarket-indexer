// src/stateEngineWorker.ts
import { ch } from "./db";

/* ---------------- TYPES ---------------- */

type PositionState = {
    position: bigint;
    cost_basis: bigint;
    realized_pnl: bigint;
};

type TraderStats = {
    realized_pnl: bigint;
    wins: number;
    losses: number;
};

type Cursor = {
    last_block: number;
    last_tx_index: number;
    last_log_index: number;
};

type TokenMeta = {
    condition_id: string;
    index_set: string;
    collateral_token: string;
    parent_collection_id: string;
};

/* ---------------- CONFIG ---------------- */

const BATCH_SIZE = 1000;
const BASE_POLL_INTERVAL_MS = 500;
const IDLE_POLL_INTERVAL_MS = 2000;

const PRICE_SCALE: bigint = 1_000_000n;

/* ---------------- STATE ---------------- */

const state = new Map<string, PositionState>();
const conditionIndex = new Map<string, Set<string>>();
const traderStats = new Map<string, TraderStats>();
const traderMarketPnl = new Map<string, bigint>();

/* ---------------- TOKEN META CACHE ---------------- */

const tokenMetaCache = new Map<string, TokenMeta>(); // position_id -> meta

function normalizeHexPositionId(x: string): string {
    if (!x) return x;
    let s = x.toLowerCase();
    if (!s.startsWith("0x")) s = "0x" + s;
    return s;
}

/* ---------------- KEYS ---------------- */

function buildPositionKey(trader: string, conditionId: string, indexSet: string) {
    return `${trader}:${conditionId}:${indexSet}`;
}

function buildMarketKey(trader: string, conditionId: string) {
    return `${trader}:${conditionId}`;
}

/* ---------------- CONDITION INDEX ---------------- */

function indexPosition(conditionId: string, key: string) {
    let set = conditionIndex.get(conditionId);
    if (!set) {
        set = new Set();
        conditionIndex.set(conditionId, set);
    }
    set.add(key);
}

function removeFromIndex(conditionId: string, key: string) {
    const set = conditionIndex.get(conditionId);
    if (!set) return;
    set.delete(key);
    if (set.size === 0) conditionIndex.delete(conditionId);
}

/* ---------------- TRADER STATS ---------------- */

function getOrCreateTrader(trader: string): TraderStats {
    let stats = traderStats.get(trader);
    if (!stats) {
        stats = { realized_pnl: 0n, wins: 0, losses: 0 };
        traderStats.set(trader, stats);
    }
    return stats;
}

/* ---------------- LOAD STATE ---------------- */

async function loadLatestState() {
    console.log("Loading latest state...");

    const r = await ch.query({
        query: `
            SELECT *
            FROM positions_latest
        `
    });

    const rows = (await r.json<any>()).data;

    for (const row of rows) {
        const key = buildPositionKey(row.trader, row.condition_id, row.index_set);

        state.set(key, {
            position: BigInt(row.position),
            cost_basis: BigInt(row.cost_basis),
            realized_pnl: BigInt(row.realized_pnl),
        });

        indexPosition(row.condition_id, key);

        const stats = getOrCreateTrader(row.trader);
        stats.realized_pnl += BigInt(row.realized_pnl);
    }

    console.log(`Loaded ${rows.length} positions`);
}

/* ---------------- CURSOR ---------------- */

async function getCursor(): Promise<Cursor> {
    const r = await ch.query({
        query: `
            SELECT anyLast(last_block) AS last_block,
                   anyLast(last_tx_index) AS last_tx_index,
                   anyLast(last_log_index) AS last_log_index
            FROM engine_cursor
            WHERE id = 1
        `
    });

    return (await r.json<any>()).data[0] ?? {
        last_block: 0,
        last_tx_index: 0,
        last_log_index: 0
    };
}

async function updateCursor(c: Cursor) {
    await ch.insert({
        table: "engine_cursor",
        values: [{ id: 1, ...c, updated_at: new Date() }],
        format: "JSONEachRow",
    });
}

/* ---------------- LOAD EVENTS ---------------- */

async function loadNewTrades(cursor: Cursor): Promise<any[]> {
    const r = await ch.query({
        query: `
            SELECT *
            FROM trades_raw
            WHERE
                (block_number > {block:UInt64})
                OR (
                    block_number = {block:UInt64}
                    AND (
                        tx_index > {tx:UInt32}
                        OR (tx_index = {tx:UInt32} AND log_index > {log:UInt32})
                    )
                )
            ORDER BY block_number, tx_index, log_index
            LIMIT 10000
        `,
        query_params: {
            block: cursor.last_block,
            tx: cursor.last_tx_index,
            log: cursor.last_log_index,
        }
    });

    const rows = (await r.json<any>()).data;
    for (const t of rows) {
        t.asset_id = normalizeHexPositionId(t.asset_id);
        t.trader = (t.trader ?? "").toLowerCase();
    }
    return rows;
}

async function loadNewResolutions(cursor: Cursor): Promise<any[]> {
    const r = await ch.query({
        query: `
            SELECT *
            FROM resolutions_raw
            WHERE event_type = 'resolved'
            AND (
                (block_number > {block:UInt64})
                OR (
                    block_number = {block:UInt64}
                    AND (
                        tx_index > {tx:UInt32}
                        OR (tx_index = {tx:UInt32} AND log_index > {log:UInt32})
                    )
                )
            )
            ORDER BY block_number, tx_index, log_index
            LIMIT 10000
        `,
        query_params: {
            block: cursor.last_block,
            tx: cursor.last_tx_index,
            log: cursor.last_log_index,
        }
    });

    return (await r.json<any>()).data;
}

/* ---------------- TOKEN RESOLUTION ---------------- */

async function resolveTokenFromDB(positionId: string): Promise<TokenMeta | null> {
    const pid = normalizeHexPositionId(positionId);
    const cached = tokenMetaCache.get(pid);
    if (cached) return cached;

    const r = await ch.query({
        query: `
            SELECT
                condition_id,
                index_set,
                collateral_token,
                parent_collection_id
            FROM condition_partitions
            WHERE position_id = {pid:String}
            ORDER BY block_number DESC, tx_index DESC, log_index DESC
            LIMIT 1
        `,
        query_params: { pid }
    });

    const row = (await r.json<any>()).data[0];
    if (!row) return null;

    const meta: TokenMeta = {
        condition_id: row.condition_id,
        index_set: row.index_set,
        collateral_token: row.collateral_token,
        parent_collection_id: row.parent_collection_id
    };

    tokenMetaCache.set(pid, meta);
    return meta;
}

/* ---------------- TRADE LOGIC ---------------- */

function applyBuy(pos: PositionState, size: bigint, notional: bigint) {
    pos.position += size;
    pos.cost_basis += notional;
}

function applySell(
    pos: PositionState,
    trader: string,
    condition_id: string,
    size: bigint,
    priceScaled: bigint
) {
    if (pos.position === 0n || size > pos.position) return;

    const avgCost = pos.cost_basis / pos.position;

    const costSold = size * avgCost;
    const revenue = (size * priceScaled) / PRICE_SCALE;
    const pnl = revenue - costSold;

    pos.realized_pnl += pnl;

    const stats = getOrCreateTrader(trader);
    stats.realized_pnl += pnl;

    const marketKey = buildMarketKey(trader, condition_id);
    traderMarketPnl.set(marketKey, (traderMarketPnl.get(marketKey) ?? 0n) + pnl);

    pos.position -= size;
    pos.cost_basis -= costSold;
}

async function applyTrade(t: any) {
    const meta = await resolveTokenFromDB(t.asset_id);

    if (!meta) {
        console.log("UNRESOLVED TOKEN:", t.asset_id);
        return;
    }

    const key = buildPositionKey(t.trader, meta.condition_id, meta.index_set);

    const pos = state.get(key) ?? {
        position: 0n,
        cost_basis: 0n,
        realized_pnl: 0n
    };

    const size = BigInt(t.size_raw);
    const notional = BigInt(t.notional_raw);

    if (t.side === "buy") {
        applyBuy(pos, size, notional);
    } else {
        applySell(pos, t.trader, meta.condition_id, size, BigInt(t.price_scaled));
    }

    if (pos.position === 0n && pos.cost_basis === 0n) {
        state.delete(key);
        removeFromIndex(meta.condition_id, key);
    } else {
        state.set(key, pos);
        indexPosition(meta.condition_id, key);
    }

    snapshotBuffer.push(buildSnapshot(key, pos, t));
}

/* ---------------- RESOLUTION LOGIC ---------------- */

function computePayout(indexSet: string, payouts: bigint[]): bigint {
    const mask = BigInt(indexSet);

    let value = 0n;
    for (let i = 0; i < payouts.length; i++)
        if (mask & (1n << BigInt(i))) value += payouts[i];

    return value;
}

function applyResolution(r: any) {
    if (!r.condition_id) return;

    const payouts = (r.payouts ?? []).map((x: string) => BigInt(x));
    const affected = conditionIndex.get(r.condition_id);
    if (!affected) return;

    for (const key of affected) {
        const pos = state.get(key);
        if (!pos) continue;

        const [trader, condition_id, index_set] = key.split(":");

        const payoutValue = computePayout(index_set, payouts);
        const pnl = (pos.position * payoutValue) - pos.cost_basis;

        const stats = getOrCreateTrader(trader);
        stats.realized_pnl += pnl;

        const marketKey = buildMarketKey(trader, condition_id);
        const totalMarketPnl = (traderMarketPnl.get(marketKey) ?? 0n) + pnl;

        if (totalMarketPnl > 0n) stats.wins++;
        else if (totalMarketPnl < 0n) stats.losses++;

        traderMarketPnl.delete(marketKey);

        state.delete(key);
        removeFromIndex(condition_id, key);

        snapshotBuffer.push({
            trader,
            condition_id,
            index_set,
            position: "0",
            cost_basis: "0",
            realized_pnl: stats.realized_pnl.toString(),
            last_block: r.block_number,
            last_tx_index: r.tx_index,
            last_log_index: r.log_index,
            event_time: new Date(),
        });
    }
}

/* ---------------- SNAPSHOTS ---------------- */

let snapshotBuffer: any[] = [];

function buildSnapshot(key: string, pos: PositionState, t: any) {
    const [trader, condition_id, index_set] = key.split(":");

    return {
        trader,
        condition_id,
        index_set,
        position: pos.position.toString(),
        cost_basis: pos.cost_basis.toString(),
        realized_pnl: pos.realized_pnl.toString(),
        last_block: t.block_number,
        last_tx_index: t.tx_index,
        last_log_index: t.log_index,
        event_time: new Date(),
    };
}

async function flushSnapshots() {
    if (!snapshotBuffer.length) return;

    const batch = snapshotBuffer;
    snapshotBuffer = [];

    await ch.insert({
        table: "positions_latest",
        values: batch,
        format: "JSONEachRow",
    });
}

/* ---------------- ENGINE LOOP ---------------- */

export async function startStateEngine() {
    console.log("State Engine starting...");

    await loadLatestState();

    let idle = false;

    while (true) {
        try {
            const cursor = await getCursor();

            const [trades, resolutions] = await Promise.all([
                loadNewTrades(cursor),
                loadNewResolutions(cursor)
            ]);

            const events = [...trades, ...resolutions];

            if (!events.length) {
                await sleep(idle ? IDLE_POLL_INTERVAL_MS : BASE_POLL_INTERVAL_MS);
                idle = true;
                continue;
            }

            idle = false;

            events.sort((a, b) =>
                a.block_number - b.block_number ||
                a.tx_index - b.tx_index ||
                a.log_index - b.log_index
            );

            let lastCursor = cursor;

            for (const e of events) {
                if (e.trade_id) await applyTrade(e);
                if (e.resolution_id) applyResolution(e);

                lastCursor = {
                    last_block: e.block_number,
                    last_tx_index: e.tx_index,
                    last_log_index: e.log_index,
                };

                if (snapshotBuffer.length >= BATCH_SIZE)
                    await flushSnapshots();
            }

            await flushSnapshots();
            await updateCursor(lastCursor);

        } catch (err) {
            console.error("ENGINE ERROR", err);
            await sleep(1000);
        }
    }
}

/* ---------------- UTILS ---------------- */

function sleep(ms: number) {
    return new Promise(res => setTimeout(res, ms));
}