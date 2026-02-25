// src/types.ts
export type TradeEvent = {
    type: 'trade';
    tradeId: string;

    txHash: string;
    logIndex: number;
    blockNumber: number;
    blockHash: string;

    trader: string;
    marketId: string;
    outcome: string;

    side: 'buy' | 'sell';
    price: number;
    size: number;
    notional: number;

    timestamp: number;
};

export type ResolutionEvent = {
    type: 'resolution';
    resolutionId: string;

    txHash: string;
    logIndex: number;
    blockNumber: number;
    blockHash: string;

    marketId: string;
    resolvedOutcome: string;
    timestamp: number;
};

export type PolymarketEvent = TradeEvent | ResolutionEvent;