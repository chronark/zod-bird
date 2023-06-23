import { z } from "zod";

type PipeErrorResponse = {
  error: string;
  documentation: string;
};

const meta = z.object({
  name: z.string(),
  type: z.string(),
});

export type Meta = z.infer<typeof meta>;

const pipeResponseWithoutData = z.object({
  meta: z.array(meta),
  rows: z.number().optional(),
  rows_before_limit_at_least: z.number().optional(),
  statistics: z
    .object({
      elapsed: z.number().optional(),
      rows_read: z.number().optional(),
      bytes_read: z.number().optional(),
    })
    .optional(),
});

const eventIngestReponseData = z.object({
  successful_rows: z.number(),
  quarantined_rows: z.number(),
});

export class Tinybird {
  private readonly baseUrl: string;
  private readonly token: string;

  constructor(opts: { token: string; baseUrl?: string }) {
    this.baseUrl = opts.baseUrl ?? "https://api.tinybird.co";
    this.token = opts.token;
  }

  private async fetch(
    pipe: string,
    parameters: Record<string, unknown> = {},
    opts?: { cache?: RequestCache; revalidate?: number },
  ): Promise<unknown> {
    const url = new URL(`/v0/pipes/${pipe}.json`, this.baseUrl);
    for (const [key, value] of Object.entries(parameters)) {
      if (typeof value === "undefined" || value === null) {
        continue;
      }
      url.searchParams.set(key, value.toString());
    }
    const res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${this.token}`,
      },
      cache: opts?.cache,
      // @ts-ignore
      next: {
        revalidate: opts?.revalidate,
      },
    });
    if (!res.ok) {
      const error = (await res.json()) as PipeErrorResponse;
      throw new Error(error.error);
    }
    const body = await res.json();

    return body;
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
    params?: TParameters,
  ) => Promise<z.infer<typeof pipeResponseWithoutData> & { data: TData[] }> {
    const outputSchema = pipeResponseWithoutData.setKey("data", z.array(req.data));
    return async (params?: TParameters) => {
      let validatedParams: TParameters | undefined = undefined;
      if (req.parameters) {
        const v = req.parameters.safeParse(params);
        if (!v.success) {
          throw new Error(v.error.message);
        }
        validatedParams = v.data;
      }

      const res = await this.fetch(req.pipe, validatedParams, req.opts);
      const validatedResponse = outputSchema.safeParse(res);
      if (!validatedResponse.success) {
        throw new Error(validatedResponse.error.message);
      }

      return validatedResponse.data;
    };
  }

  public buildIngestEndpoint<TEvent extends Record<string, unknown>,>(req: {
    datasource: string;
    event?: z.ZodSchema<TEvent>;
  }): (event: TEvent) => Promise<z.infer<typeof eventIngestReponseData>> {
    return async (event: TEvent) => {
      let validatedParams: TEvent | undefined = undefined;
      if (req.event) {
        const v = req.event.safeParse(event);
        if (!v.success) {
          throw new Error(v.error.message);
        }
        validatedParams = v.data;
      }

      const url = new URL("/v0/events", this.baseUrl);
      url.searchParams.set("name", req.datasource);

      const body = (Array.isArray(event) ? event : [event])
        .map((p) => JSON.stringify(p))
        .join("\n");
      let res = await fetch(url, {
        method: "POST",
        body,
        headers: { Authorization: `Bearer ${this.token}` },
      });

      /**
       * Add one retry in case of 429 ratelimit response
       */
      if (res.status === 429) {
        const limit = res.headers.get("X-RateLimit-Limit");
        const remaining = res.headers.get("X-RateLimit-Remaining");
        const reset = res.headers.get("X-RateLimit-Reset");
        const retryAfter = res.headers.get("Retry-After");
        console.warn(`Hit Tinybird ratelimit: ${url}`, {
          limit,
          remaining,
          reset,
          retryAfter,
        });

        await new Promise((r) => setTimeout(r, retryAfter ? parseInt(retryAfter) : 1000));
        res = await fetch(url, {
          method: "POST",
          body,
          headers: { Authorization: `Bearer ${this.token}` },
        });
      }

      if (!res.ok) {
        throw new Error(
          `Unable to ingest to ${req.datasource}: [${res.status}] ${await res.text()}`,
        );
      }

      const validatedResponse = eventIngestReponseData.safeParse(await res.json());

      if (!validatedResponse.success) {
        throw new Error(validatedResponse.error.message);
      }

      return validatedResponse.data;
    };
  }
}
