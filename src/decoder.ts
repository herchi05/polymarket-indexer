// src/decoder.ts
import { Interface, Log } from "ethers";

import CTF_EXCHANGE_ABI from "./abi/ctf_exchange.json";
import CONDITIONAL_TOKENS_ABI from "./abi/conditional_tokens.json";
import UMA_ADAPTER_ABI from "./abi/uma_adapter.json";

const ctfExchange = new Interface(CTF_EXCHANGE_ABI);
const conditionalTokens = new Interface(CONDITIONAL_TOKENS_ABI);
const umaAdapter = new Interface(UMA_ADAPTER_ABI);

export function buildEventId(txHash: string, logIndex: number): string {
    return `${txHash}:${logIndex}`;
}

function tryParse(iface: Interface, log: Log) {
    try { return iface.parseLog(log); }
    catch { return null; }
}

export function decodeLog(log: Log) {

    /* ---------------- TRADES ---------------- */

    const trade = tryParse(ctfExchange, log);

    if (trade && trade.name === "OrderFilled") {
        const a = trade.args;

        return {
            kind: "trade",

            orderHash: a.orderHash,
            maker: a.maker,
            taker: a.taker,

            makerAssetId: a.makerAssetId.toString(),
            takerAssetId: a.takerAssetId.toString(),

            makerAmountFilled: a.makerAmountFilled.toString(),
            takerAmountFilled: a.takerAmountFilled.toString(),

            fee: a.fee.toString(),
        };
    }

    /* ---------------- CONDITIONAL TOKENS EVENTS ---------------- */

    const ctEvent = tryParse(conditionalTokens, log);

    /* ---- PositionSplit ---- */

    if (ctEvent?.name === "PositionSplit") {
        return {
            kind: "position_split",

            stakeholder: ctEvent.args.stakeholder,
            collateralToken: ctEvent.args.collateralToken,

            parentCollectionId: ctEvent.args.parentCollectionId,
            conditionId: ctEvent.args.conditionId,

            partition: Array.from(ctEvent.args.partition).map(x => Number(x)),
            amount: ctEvent.args.amount.toString()
        };
    }

    /* ---- PositionsMerge ---- */

    if (ctEvent?.name === "PositionsMerge") {
        return {
            kind: "positions_merge",

            stakeholder: ctEvent.args.stakeholder,
            collateralToken: ctEvent.args.collateralToken,

            parentCollectionId: ctEvent.args.parentCollectionId,
            conditionId: ctEvent.args.conditionId,

            partition: Array.from(ctEvent.args.partition).map(x => Number(x)),
            amount: ctEvent.args.amount.toString()
        };
    }

    /* ---- ConditionPreparation ---- */

    if (ctEvent?.name === "ConditionPreparation") {
        return {
            kind: "condition_preparation",

            conditionId: ctEvent.args.conditionId,
            oracle: ctEvent.args.oracle,
            questionId: ctEvent.args.questionId,
            outcomeSlotCount: Number(ctEvent.args.outcomeSlotCount)
        };
    }

    /* ---- ConditionResolution ⭐ CRITICAL ---- */

    if (ctEvent?.name === "ConditionResolution") {
        return {
            kind: "condition_resolution",

            conditionId: ctEvent.args.conditionId,
            oracle: ctEvent.args.oracle,
            questionId: ctEvent.args.questionId,

            outcomeSlotCount: Number(ctEvent.args.outcomeSlotCount),

            payoutNumerators: Array.from(ctEvent.args.payoutNumerators)
                .map(x => (x as bigint).toString())
        };
    }

    /* ---- PayoutRedemption ⭐ CRITICAL ---- */

    if (ctEvent?.name === "PayoutRedemption") {
        return {
            kind: "payout_redemption",

            redeemer: ctEvent.args.redeemer,
            collateralToken: ctEvent.args.collateralToken,
            parentCollectionId: ctEvent.args.parentCollectionId,

            conditionId: ctEvent.args.conditionId,

            indexSets: Array.from(ctEvent.args.indexSets)
                .map(x => (x as bigint).toString()),

            payout: ctEvent.args.payout.toString()
        };
    }

    /* ---- ERC-1155 Transfers (Optional) ---- */

    if (ctEvent?.name === "TransferSingle") {
        return {
            kind: "transfer",

            operator: ctEvent.args.operator,
            from: ctEvent.args.from,
            to: ctEvent.args.to,

            tokenId: ctEvent.args.id.toString(),
            value: ctEvent.args.value.toString()
        };
    }

    if (ctEvent?.name === "TransferBatch") {
        return {
            kind: "transfer_batch",

            operator: ctEvent.args.operator,
            from: ctEvent.args.from,
            to: ctEvent.args.to,

            tokenIds: Array.from(ctEvent.args.ids).map(x => (x as bigint).toString()),
            values: Array.from(ctEvent.args.values()).map(x => (x as bigint).toString())
        };
    }

    /* ---------------- UMA ADAPTER (Optional) ---------------- */

    const resolution = tryParse(umaAdapter, log);

    if (resolution?.name === "QuestionResolved") {
        return {
            kind: "uma_resolution",

            questionID: resolution.args.questionID,
            settledPrice: resolution.args.settledPrice.toString(),

            payouts: Array.from(resolution.args.payouts)
                .map(x => (x as bigint).toString())
        };
    }

    return null;
}