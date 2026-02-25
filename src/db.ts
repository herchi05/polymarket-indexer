// src/db.ts
import { createClient } from '@clickhouse/client';
import { CONFIG } from "./config";

export const ch = createClient({
    url: CONFIG.CLICKHOUSE_URL,
    username: CONFIG.CLICKHOUSE_USERNAME,
    password: CONFIG.CLICKHOUSE_PASS,
    request_timeout: 30_000,
});
