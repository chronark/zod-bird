import { Tinybird } from './client';

/**
 * NoopTinybird is a mock implementation of the Tinybird client that doesn't do anything and returns empty data.
 */
export class NoopTinybird extends Tinybird {
  public buildPipe: Tinybird['buildPipe'] = (req) => {
    return async (params) => {
      if (req.parameters) {
        const v = req.parameters.safeParse(params);
        if (!v.success) {
          throw new Error(v.error.message);
        }
      }
      return [];
    };
  };

  public buildIngestEndpoint: Tinybird['buildIngestEndpoint'] = (req) => {
    return async (events) => {
      if (req.event) {
        const v = Array.isArray(events)
            ? req.event.array().safeParse(events)
            : req.event.safeParse(events);
        if (!v.success) {
          throw new Error(v.error.message);
        }
      }

      return {
        successful_rows: 0,
        quarantined_rows: 0,
      };
    };
  };
}