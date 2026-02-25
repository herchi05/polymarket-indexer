// src/insert.ts
import { ch } from "./db";

/* ---------------- TRADES ---------------- */

export async function insertTrade(rows: any[]) {

    if (!rows.length) return;

    try {
        await ch.insert({
            table: "trades_raw",
            values: rows,
            format: "JSONEachRow",
        });
    } catch (err) {
        console.error("insertTrade failed:", err);
        throw err;
    }
}

/* ---------------- RESOLUTIONS ---------------- */

export async function insertResolution(rows: any[]) {

    if (!rows.length) return;

    try {
        await ch.insert({
            table: "resolutions_raw",
            values: rows,
            format: "JSONEachRow",
        });
    } catch (err) {
        console.error("insertResolution failed:", err);
        throw err;
    }
}

/* ---------------- CONDITIONAL TOKEN PARTITIONS ---------------- */

export async function insertConditionPartitions(rows: any[]) {

    if (!rows.length) return;

    try {
        await ch.insert({
            table: "condition_partitions",
            values: rows,
            format: "JSONEachRow",
        });
    } catch (err) {
        console.error("insertConditionPartitions failed:", err);
        throw err;
    }
}
