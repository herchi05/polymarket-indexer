// src/api.ts
import express from "express";
import rateLimit from "express-rate-limit";
import NodeCache from "node-cache";
import { ch } from "./db";

const app = express();
const PORT = 3000;

/* ---------------- RATE LIMIT ---------------- */

const limiter = rateLimit({
    windowMs: 60 * 1000,
    max: 20,
    standardHeaders: true,
    legacyHeaders: false,
});

app.use(limiter);

/* ---------------- CACHE ---------------- */

const cache = new NodeCache({ stdTTL: 5 });

function cacheKey(req: express.Request) {
    return req.path + JSON.stringify(req.query);
}

/* ---------------- TRADER SUMMARY ---------------- */

app.get("/trader/:address", async (req, res) => {

    const trader = req.params.address.toLowerCase();
    const key = cacheKey(req);

    const cached = cache.get(key);
    if (cached) return res.json(cached);

    try {

        /* ---------------- DERIVED STATS (CLICKHOUSE) ---------------- */

        const statsQuery = await ch.query({
            query: `
                SELECT
                    trader,
                    sum(realized_pnl) AS realized_pnl,
                    sumIf(position, position > 0) AS open_position
                FROM positions_latest
                WHERE trader = {trader:String}
                GROUP BY trader
            `,
            query_params: { trader }
        });

        const statsResult = await statsQuery.json<any>();

        const row = statsResult.data[0] ?? {
            trader,
            realized_pnl: "0",
            open_position: "0"
        };

        const realized_pnl = row.realized_pnl ?? "0";
        const open_position = row.open_position ?? "0";

        const response = {
            trader,
            realized_pnl,
            open_position,

            // Optional placeholders (safe to remove if unused)
            wins: 0,
            losses: 0,
            win_rate: 0
        };

        cache.set(key, response);
        res.json(response);

    } catch (err) {
        console.error("Trader route error:", err);
        res.status(500).json({ error: "internal_error" });
    }
});

/* ---------------- POSITIONS ---------------- */

app.get("/trader/:address/positions", async (req, res) => {

    const trader = req.params.address.toLowerCase();
    const key = cacheKey(req);

    const cached = cache.get(key);
    if (cached) return res.json(cached);

    try {

        const r = await ch.query({
            query: `
                SELECT *
                FROM positions_latest
                WHERE trader = {trader:String}
                  AND position > 0
            `,
            query_params: { trader }
        });

        const result = await r.json<any>();

        cache.set(key, result.data);
        res.json(result.data);

    } catch (err) {
        console.error("Positions route error:", err);
        res.status(500).json({ error: "internal_error" });
    }
});

/* ---------------- HEALTH ---------------- */

app.get("/health", (_, res) => res.send("ok"));

export function startAPI(): Promise<void> {
    return new Promise((resolve) => {
        app.listen(PORT, () => {
            console.log(`API running on port ${PORT}`);
            resolve();
        });
    });
}