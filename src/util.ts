import { z } from "zod";
export type PipeErrorResponse = {
  error: string;
  documentation: string;
};

export const meta = z.object({
  name: z.string(),
  type: z.string(),
});

export type Meta = z.infer<typeof meta>;

export const pipeResponseWithoutData = z.object({
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

export const eventIngestReponseData = z.object({
  successful_rows: z.number(),
  quarantined_rows: z.number(),
});
