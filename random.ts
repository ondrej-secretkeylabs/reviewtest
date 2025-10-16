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
    case "starknet":
      return Math.floor(new Date(tx.data.blockTimestamp).getTime() / 1000);
    case "spark":
      return Math.floor(new Date(tx.data.createdAt ?? 0).getTime() / 1000);
  }
}

export class StreamMerger {
  constructor(private streams: TransactionStream[]) {}

  async takeN(limit: number): Promise<ResponseTransaction[]> {
    const results: ResponseTransaction[] = [];

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
      }
    }

    return results;
  }
}
