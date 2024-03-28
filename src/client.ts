import { z } from "zod";
import { type PipeErrorResponse, eventIngestReponseData, pipeResponseWithoutData } from "./util";

export type Config = {
  baseUrl?: string;
} & (
  | {
      token: string;
      noop?: never;
    }
  | {
      token?: never;
      noop: true;
    }
);

export class Tinybird {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly noop: boolean;

  constructor(config: Config) {
    this.baseUrl = config.baseUrl ?? "https://api.tinybird.co";
    if (config.noop) {
      this.token = "";
      this.noop = true;
    } else {
      this.token = config.token;
      this.noop = false;
    }
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

    for (let i = 0; i < 10; i++) {
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
      if (res.ok) {
        return res.json();
      }

      if (res.status === 429) {
        await new Promise((r) => setTimeout(r, 1000 + i ** 2 * 50));
        continue;
      }

      if (!res.ok) {
        const error = (await res.json()) as PipeErrorResponse;
        throw new Error(error.error);
      }
    }
  }

  public buildPipe<TParameters extends z.ZodSchema<any>, TData extends z.ZodSchema<any>>(req: {
    pipe: string;
    parameters?: TParameters;
    data: TData;
    opts?: {
      cache?: RequestCache;
      /**
       * Number of seconds to revalidate the cache
       */
      revalidate?: number;
    };
  }): (
    params: z.input<TParameters>,
  ) => Promise<z.infer<typeof pipeResponseWithoutData> & { data: z.output<TData>[] }> {
    const outputSchema = pipeResponseWithoutData.setKey("data", z.array(req.data));
    return async (params: z.input<TParameters>) => {
      let validatedParams: z.input<TParameters> | undefined = undefined;
      if (req.parameters) {
        const v = req.parameters.safeParse(params);
        if (!v.success) {
          throw new Error(v.error.message);
        }
        validatedParams = v.data;
      }
      if (this.noop) {
        return { meta: [], data: [] };
      }
      const res = await this.fetch(req.pipe, validatedParams, req.opts);
      const validatedResponse = outputSchema.safeParse(res);
      if (!validatedResponse.success) {
        throw new Error(validatedResponse.error.message);
      }

      return validatedResponse.data;
    };
  }

  public buildIngestEndpoint<TSchema extends z.ZodSchema<any>>(req: {
    datasource: string;
    event: TSchema;
  }): (
    events: z.input<TSchema> | z.input<TSchema>[],
  ) => Promise<z.infer<typeof eventIngestReponseData>> {
    return async (events: z.input<TSchema> | z.input<TSchema>[]) => {
      let validatedEvents: z.output<TSchema> | z.output<TSchema>[] | undefined = undefined;
      if (req.event) {
        const v = Array.isArray(events)
          ? req.event.array().safeParse(events)
          : req.event.safeParse(events);
        if (!v.success) {
          throw new Error(v.error.message);
        }
        validatedEvents = v.data;
      }

      if (this.noop) {
        return {
          successful_rows: Array.isArray(validatedEvents) ? validatedEvents.length : 1,
          quarantined_rows: 0,
        };
      }
      const url = new URL("/v0/events", this.baseUrl);
      url.searchParams.set("name", req.datasource);

      const body = (Array.isArray(validatedEvents) ? validatedEvents : [validatedEvents])
        .map((p) => JSON.stringify(p))
        .join("\n");

      const res = await fetch(url, {
        method: "POST",
        body,
        headers: { Authorization: `Bearer ${this.token}` },
      });

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
