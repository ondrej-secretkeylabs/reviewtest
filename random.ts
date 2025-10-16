import { ResponseTransaction } from "../types";
import { TransactionStream } from "./types";

function getTransactionTimestamp(tx: ResponseTransaction): number {
  switch (tx.type) {
    case "bitcoin":
      return tx.data.blockTime ?? 0;
    case "stacks": {
      const blockTime = tx.data.tx.block_time;
      if (typeof blockTime === "number" && blockTime > 0) {
        return blockTime;
      }
      const burnBlockTime = tx.data.tx.burn_block_time;
      if (typeof burnBlockTime === "number") {
        return burnBlockTime;
      }
      // Pending Stacks transactions can lack a block time. Treat them as the most
      // recent items so they remain ahead of confirmed activity, matching the page
      // builder ordering that stringifies `undefined` timestamps.
      return Number.POSITIVE_INFINITY;
    }
    case "starknet": {
      const timestamp = new Date(tx.data.blockTimestamp).getTime();
      if (isNaN(timestamp)) {
        throw new Error(`Invalid blockTimestamp for starknet transaction: ${tx.data.blockTimestamp}`);
      }
      return Math.floor(timestamp / 1000);
    }
    case "spark": {
      const createdAt = tx.data.createdAt ?? 0;
      const timestamp = new Date(createdAt).getTime();
      if (isNaN(timestamp)) {
        throw new Error(`Invalid createdAt for spark transaction: ${createdAt}`);
      }
      return Math.floor(timestamp / 1000);
    }
    default: {
      // Exhaustive type check - this will cause a compile error if new transaction types are added
      const _exhaustiveCheck: never = tx;
      throw new Error(`Unhandled transaction type: ${JSON.stringify(_exhaustiveCheck)}`);
    }
  }
}

// Maximum allowed limit to prevent DoS attacks via resource exhaustion
const MAX_ALLOWED_LIMIT = 10000;

export class StreamMerger {
  constructor(private streams: TransactionStream[]) {
    if (!streams || streams.length === 0) {
      throw new Error("StreamMerger requires at least one stream");
    }
  }

  async takeN(limit: number): Promise<ResponseTransaction[]> {
    // Validate limit to prevent DoS attacks
    if (limit <= 0 || !Number.isFinite(limit)) {
      throw new Error(`Invalid limit: ${limit}. Must be a positive finite number.`);
    }
    if (limit > MAX_ALLOWED_LIMIT) {
      throw new Error(`Limit ${limit} exceeds maximum allowed limit of ${MAX_ALLOWED_LIMIT}`);
    }

    const results: ResponseTransaction[] = [];
    let emptyPeeksCount = 0;
    const MAX_EMPTY_PEEKS = 3; // Safeguard against infinite loops

    while (results.length < limit) {
      const peeks = await Promise.all(
        this.streams.map(async (stream, idx) => ({
          transaction: await stream.peek(),
          streamIndex: idx,
        })),
      );

      const available = peeks.filter((p) => p.transaction !== undefined);
      if (available.length === 0) break;

      const winner = available.reduce((best, curr) => {
        // Safe to use non-null assertion here because filter guarantees non-undefined
        const bestTimestamp = getTransactionTimestamp(best.transaction!);
        const currTimestamp = getTransactionTimestamp(curr.transaction!);

        if (currTimestamp > bestTimestamp) {
          return curr;
        }
        if (currTimestamp === bestTimestamp) {
          return curr.streamIndex < best.streamIndex ? curr : best;
        }
        return best;
      });

      const result = await this.streams[winner.streamIndex].next();
      if (!result.done && result.value) {
        results.push(result.value);
        emptyPeeksCount = 0; // Reset counter on successful retrieval
      } else {
        // Safeguard against streams that repeatedly return empty results
        emptyPeeksCount++;
        if (emptyPeeksCount >= MAX_EMPTY_PEEKS) {
          throw new Error(`Stream returned empty results ${MAX_EMPTY_PEEKS} times consecutively. Possible infinite loop detected.`);
        }
      }
    }

    return results;
  }
}
