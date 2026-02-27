// src/indexer.ts
import { ch } from "./db";
import { calculatePositionIds } from "@polymarket/ctf-utils";

/* ---------------- CONFIG ---------------- */

const MAX_BATCH = 2000;
const FLUSH_INTERVAL = 1000;

/* ---------------- BUFFERS ---------------- */

let ledgerBuffer: any[] = [];
let conditionBuffer: any[] = [];
let positionBuffer: any[] = [];
let priceBuffer: any[] = [];

/* ========================================================= */
/* ================= ORDER FILLED ========================== */
/* ========================================================= */

export function handleOrderFilled(parsed: any, blockNumber: number) {
  const {
    maker,
    taker,
    makerAssetId,
    takerAssetId,
    makerAmountFilled,
    takerAmountFilled,
    fee
  } = parsed.args;

  const makerAddr = maker.toLowerCase();
  const takerAddr = taker.toLowerCase();

  const makerIsBuyer = makerAssetId === 0n;

  const collateral = makerIsBuyer ? makerAmountFilled : takerAmountFilled;
  const shares = makerIsBuyer ? takerAmountFilled : makerAmountFilled;

  if (shares === 0n) return;

  const positionId = makerIsBuyer
    ? takerAssetId.toString()
    : makerAssetId.toString();

  const buyer = (makerIsBuyer ? makerAddr : takerAddr);
  const seller = (makerIsBuyer ? takerAddr : makerAddr);

  const feeAmount = BigInt(fee ?? 0n);

  /* ================= PRICE UPDATE ================= */

  const priceMicros = (collateral * 1_000_000n) / shares;

  priceBuffer.push({
    position_id: positionId,
    last_price: priceMicros.toString(),
    block_number: blockNumber
  });

  /* ================= BUYER LEDGER ================= */

  ledgerBuffer.push({
    trader: buyer,
    position_id: positionId,
    block_number: blockNumber,
    size_delta: shares.toString(),
    collateral_delta: (-collateral).toString(),
    fee_delta: buyer === takerAddr ? (-feeAmount).toString() : "0",
    event_type: "trade_buy",
    tx_hash: parsed.log?.transactionHash ?? "",
    log_index: parsed.log?.index ?? 0
  });

  /* ================= SELLER LEDGER ================= */

  ledgerBuffer.push({
    trader: seller,
    position_id: positionId,
    block_number: blockNumber,
    size_delta: (-shares).toString(),
    collateral_delta: collateral.toString(),
    fee_delta: seller === takerAddr ? (-feeAmount).toString() : "0",
    event_type: "trade_sell",
    tx_hash: parsed.log?.transactionHash ?? "",
    log_index: parsed.log?.index ?? 0
  });

  if (ledgerBuffer.length >= MAX_BATCH) flushLedger();
  if (priceBuffer.length >= MAX_BATCH) flushPrices();
}

/* ========================================================= */
/* ================= PAYOUT REDEMPTION ===================== */
/* ========================================================= */

export function handlePayoutRedemption(parsed: any, blockNumber: number) {
  const {
    redeemer,
    collateralToken,
    conditionId,
    indexSets,
    payout
  } = parsed.args;

  const trader = redeemer.toLowerCase();

  // Calculate position IDs for this condition
  const positionIds = calculatePositionIds(
    conditionId,
    collateralToken,
    indexSets.length
  );

  // Each indexSet corresponds to a position
  for (let i = 0; i < positionIds.length; i++) {
    const positionId = positionIds[i];

    ledgerBuffer.push({
      trader,
      position_id: positionId,
      block_number: blockNumber,
      size_delta: "0", // shares already burned in redemption
      collateral_delta: payout.toString(),
      fee_delta: "0",
      event_type: "redemption"
    });
  }

  if (ledgerBuffer.length >= MAX_BATCH) flushLedger();
}

/* ========================================================= */
/* ================= CONDITION RESOLUTION ================== */
/* ========================================================= */

export function handleConditionResolution(parsed: any, blockNumber: number) {
  const {
    conditionId,
    payoutNumerators,
    outcomeSlotCount
  } = parsed.args;

  conditionBuffer.push({
    condition_id: conditionId.toString(), // <-- important
    payout_numerators: payoutNumerators.map((x: any) => x.toString()),
    outcome_slot_count: Number(outcomeSlotCount), // safe conversion
    resolved: 1,
    resolved_block: blockNumber
  });

  if (conditionBuffer.length >= MAX_BATCH) flushConditions();
}

/* ========================================================= */
/* ================= POSITION SPLIT ======================== */
/* ========================================================= */

export function handlePositionSplit(parsed: any) {
  if (parsed.name !== "PositionSplit") return;

  const {
    conditionId,
    collateralToken,
    partition
  } = parsed.args;

  const positionIds = calculatePositionIds(
    conditionId,
    collateralToken,
    partition.length
  );

  for (let i = 0; i < positionIds.length; i++) {
    positionBuffer.push({
      position_id: positionIds[i],
      condition_id: conditionId,
      index_set: partition[i].toString()
    });
  }

  if (positionBuffer.length >= MAX_BATCH) flushPositions();
}

/* ========================================================= */
/* ================= FLUSH FUNCTIONS ======================= */
/* ========================================================= */

async function flushLedger() {
  if (!ledgerBuffer.length) return;
  const batch = ledgerBuffer;
  ledgerBuffer = [];
  await ch.insert({
    table: "trader_ledger",
    values: batch,
    format: "JSONEachRow"
  });
}

async function flushConditions() {
  if (!conditionBuffer.length) return;
  const batch = conditionBuffer;
  conditionBuffer = [];
  await ch.insert({
    table: "conditions",
    values: batch,
    format: "JSONEachRow"
  });
}

async function flushPositions() {
  if (!positionBuffer.length) return;
  const batch = positionBuffer;
  positionBuffer = [];
  await ch.insert({
    table: "positions",
    values: batch,
    format: "JSONEachRow"
  });
}

async function flushPrices() {
  if (!priceBuffer.length) return;
  const batch = priceBuffer;
  priceBuffer = [];
  await ch.insert({
    table: "market_prices",
    values: batch,
    format: "JSONEachRow"
  });
}

setInterval(() => {
  flushLedger();
  flushConditions();
  flushPositions();
  flushPrices();
}, FLUSH_INTERVAL);