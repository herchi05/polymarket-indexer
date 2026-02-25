// src/index.ts
import { startStateEngine } from "./stateEngineWorker";
import { startWS } from "./ws";
import { startAPI } from "./api";

async function main() {
    console.log("Starting system...");

    startWS();
    startStateEngine();
    await startAPI();

    console.log("System started");
}

main();
