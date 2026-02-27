// src/api.ts
import express from "express";
import rateLimit from "express-rate-limit";
import NodeCache from "node-cache";
import { ch } from "./db";

const app = express();
app.use(express.json());

/* -------------------------------- */
/* CONFIG                           */
/* -------------------------------- */

const USDC_DECIMALS = 1_000_000;

/* -------------------------------- */
/* RATE LIMITING                    */
/* -------------------------------- */

const limiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 60, // 60 requests per minute per IP
  standardHeaders: true,
  legacyHeaders: false,
});

app.use(limiter);

/* -------------------------------- */
/* CACHE                            */
/* -------------------------------- */

const cache = new NodeCache({
  stdTTL: 30, // 30 second cache
  checkperiod: 60,
});

/* -------------------------------- */
/* Generic Typed Query Helper       */
/* -------------------------------- */

async function queryOne<T>(
  query: string,
  params: Record<string, any>
): Promise<T | null> {
  const result = await ch.query({
    query,
    query_params: params,
    format: "JSONEachRow",
  });

  const rows = (await result.json()) as T[];
  return rows[0] ?? null;
}

/* -------------------------------- */
/* GET /trader/:address/metrics     */
/* -------------------------------- */

app.get("/trader/:address/metrics", async (req, res) => {
  const trader = req.params.address.toLowerCase();

  const cacheKey = `metrics:${trader}`;
  const cached = cache.get(cacheKey);

  if (cached) {
    return res.json(cached);
  }

  try {

    /* ========================================= */
    /* REALIZED PNL (CASH LEDGER ONLY)          */
    /* ========================================= */

    const realizedRow = await queryOne<{
      realized_pnl: number | null;
    }>(
      `
      SELECT
        sum(collateral_delta + fee_delta) AS realized_pnl
      FROM trader_ledger
      WHERE trader = {trader:String}
      `,
      { trader }
    );

    const realizedMicros = realizedRow?.realized_pnl ?? 0;
    const realized = realizedMicros / USDC_DECIMALS;

    /* ========================================= */
    /* UNREALIZED PNL (ONLY OPEN POSITIONS)     */
    /* ========================================= */

    const unrealizedRow = await queryOne<{
      unrealized_pnl: number | null;
    }>(
      `
      SELECT
        sum(intDiv(size * mp.last_price, 1000000) - cost) AS unrealized_pnl
      FROM
      (
        SELECT
          position_id,
          sum(size_delta) AS size,
          sum(collateral_delta) AS cost
        FROM trader_ledger
        WHERE trader = {trader:String}
        GROUP BY position_id
        HAVING size != 0
      ) p
      LEFT JOIN market_prices mp USING position_id
      `,
      { trader }
    );

    const unrealizedMicros = unrealizedRow?.unrealized_pnl ?? 0;
    const unrealized = unrealizedMicros / USDC_DECIMALS;

    /* ========================================= */
    /* WIN RATE (ONLY RESOLVED CONDITIONS)      */
    /* ========================================= */

    const winRateRow = await queryOne<{
      win_rate: number | null;
    }>(
      `
      SELECT
        countIf(pnl > 0) / count() AS win_rate
      FROM
      (
        SELECT
          pos.condition_id,
          sum(l.collateral_delta + l.fee_delta) AS pnl
        FROM trader_ledger l
        INNER JOIN positions pos
          ON l.position_id = pos.position_id
        INNER JOIN conditions c
          ON pos.condition_id = c.condition_id
        WHERE l.trader = {trader:String}
          AND c.resolved = 1
        GROUP BY pos.condition_id
      )
      `,
      { trader }
    );

    const winRate = winRateRow?.win_rate ?? 0;

    /* ========================================= */
    /* RESPONSE                                  */
    /* ========================================= */

    const response = {
      trader,
      realized_pnl: realized,
      unrealized_pnl: unrealized,
      total_pnl: realized + unrealized,
      win_rate: winRate,
    };

    cache.set(cacheKey, response);

    return res.json(response);

  } catch (err) {
    console.error("API error:", err);
    return res.status(500).json({ error: "Internal error" });
  }
});

/* -------------------------------- */

app.listen(3000, () => {
  console.log("API running on port 3000");
});