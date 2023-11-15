import { z } from "zod";

import { eventIngestReponseData, pipeResponseWithoutData } from "./util";

/**
 * NoopTinybird is a mock implementation of the Tinybird client that doesn't do anything and returns empty data.
 */
export class NoopTinybird {
  private async fetch() {
    return {
      meta: [],
      data: [],
    };
  }

  public buildPipe<
    TParameters extends Record<string, unknown>,
    TData extends Record<string, unknown>,
  >(req: {
    pipe: string;
    parameters?: z.ZodSchema<TParameters>;
    // rome-ignore lint/suspicious/noExplicitAny: <explanation>
    data: z.ZodSchema<TData, any, any>;
    opts?: {
      cache?: RequestCache;
      /**
       * Number of seconds to revalidate the cache
       */
      revalidate?: number;
    };
  }): (
    params: TParameters,
  ) => Promise<z.infer<typeof pipeResponseWithoutData> & { data: TData[] }> {
    const outputSchema = pipeResponseWithoutData.setKey("data", z.array(req.data));
    return async (params: TParameters) => {
      let validatedParams: TParameters | undefined = undefined;
      if (req.parameters) {
        const v = req.parameters.safeParse(params);
        if (!v.success) {
          throw new Error(v.error.message);
        }
        validatedParams = v.data;
      }

      const res = await this.fetch();
      const validatedResponse = outputSchema.safeParse(res);
      if (!validatedResponse.success) {
        throw new Error(validatedResponse.error.message);
      }

      return validatedResponse.data;
    };
  }

  public buildIngestEndpoint<TEvent extends Record<string, unknown>,>(req: {
    datasource: string;
    event: z.ZodSchema<TEvent>;
  }): (events: TEvent | TEvent[]) => Promise<z.infer<typeof eventIngestReponseData>> {
    return async (events: TEvent | TEvent[]) => {
      let validatedEvents: typeof events | undefined = undefined;
      if (req.event) {
        const v = req.event.safeParse(events);
        if (!v.success) {
          throw new Error(v.error.message);
        }
        validatedEvents = v.data;
      }


      const res = await this.fetch();

      const validatedResponse = eventIngestReponseData.safeParse(res);

      if (!validatedResponse.success) {
        throw new Error(validatedResponse.error.message);
      }

      return validatedResponse.data;
    };
  }
}
