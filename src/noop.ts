import { Tinybird } from "./client";
/**
 * NoopTinybird is a mock implementation of the Tinybird client that doesn't do anything and returns empty data.
 */
export class NoopTinybird extends Tinybird {
  constructor() {
    super({ noop: true });
  }
}
