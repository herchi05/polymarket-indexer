// src/config.ts
import 'dotenv/config';

/* ---------------- ENV VALIDATION ---------------- */

function required(name: string): string {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Missing required env var: ${name}`);
    }
    return value;
}

export const CONFIG = {

    /* ---------- RPC ---------- */

    POLYGON_WSS: required("POLYGON_WSS"),
    POLYGON_HTTP: required("POLYGON_HTTP"),

    /* ---------- CLICKHOUSE ---------- */

    CLICKHOUSE_URL: required("CLICKHOUSE_URL"),
    CLICKHOUSE_USERNAME: required("CLICKHOUSE_USERNAME"),
    CLICKHOUSE_PASS: required("CLICKHOUSE_PASS"),

    /* ---------- CONTRACTS (ONLY WHAT WE NEED) ---------- */

    CONTRACTS: {
        CTF_EXCHANGE: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
        CONDITIONAL_TOKENS: "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
        NEG_RISK_CTF_EXCHANGE: "0xC5d563A36AE78145C45a50134d48A1215220f80a",
    },

    /* ---------- INDEXER SAFETY ---------- */

    START_BLOCK: Number(process.env.START_BLOCK ?? 0),
    REORG_SAFETY_BLOCKS: Number(process.env.REORG_SAFETY_BLOCKS ?? 50),

    /* ---------- BACKFILL TUNING ---------- */

    BACKFILL_CHUNK: Number(process.env.BACKFILL_CHUNK ?? 50),
    MAX_BUFFER: Number(process.env.MAX_BUFFER ?? 1_000),
    FLUSH_INTERVAL: Number(process.env.FLUSH_INTERVAL ?? 1_000),

    /* ---------- OPTIONAL ---------- */

    RPC_DELAY_MS: Number(process.env.RPC_DELAY_MS ?? 200),
    DEBUG: process.env.DEBUG === "true",
};

/* ✅ ONLY CONTRACTS THAT ACTUALLY EMIT ABI EVENTS */

export const INDEXED_ADDRESSES = [
    CONFIG.CONTRACTS.CTF_EXCHANGE,
    CONFIG.CONTRACTS.CONDITIONAL_TOKENS,
    CONFIG.CONTRACTS.NEG_RISK_CTF_EXCHANGE,
];