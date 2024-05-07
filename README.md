<div align="center">
    <h1 align="center">@chronark/zod-bird</h1>
    <h5>Fully typed Tinybird pipes using zod</h5>
</div>

<br/>

- typesafe
- handles building the url params for you
- easy transformation of resulting data
- built in cache directives for nextjs
- built in retry logic for ratelimited requests


```ts
import { Tinybird } from "@chronark/zod-bird";
import { z } from "zod";

const tb = new Tinybird({ token: "token" });

export const getEvents = tb.buildPipe({
  pipe: "get_events__v1",
  parameters: z.object({
    tenantId: z.string(),
  }),
  data: z.object({
    event: z.string(),
    time: z.number().transform((t) => new Date(t)),
  }),
});


const res = await getEvents({ tenantId: "chronark" })

// res.data = {event: string, time: Date}[]
// if no rows found, res.data will be an empty array
```

## Install

```
npm i @chronark/zod-bird
```


## Ingesting Data

```ts
// lib/tinybird.ts
import { Tinybird } from "./client";
import { z } from "zod";

const tb = new Tinybird({ token: process.env.TINYBIRD_TOKEN! });

export const publishEvent = tb.buildIngestEndpoint({
  datasource: "events__v1",
  event: z.object({
    id: z.string(),
    tenantId: z.string(),
    channelId: z.string(),
    time: z.number().int(),
    event: z.string(),
    content: z.string().optional().default(""),
    metadata: z.string().optional().default(""),
  }),
});
```

```ts
// somewhere.ts
import { publishEvent } from "./lib/tinybird";

await publishEvent({
  id: "1",
  tenantId: "1",
  channelId: "1",
  time: Date.now(),
  event: "test",
  content: "test",
  metadata: JSON.stringify({ test: "test" }),
});
```

Or multiple events in bulk, using a single request to Tinybird.

```ts
// somewhere.ts
import { publishEvent } from "./lib/tinybird";

await publishEvent([
  {
    id: "1",
    tenantId: "1",
    channelId: "1",
    time: Date.now(),
    event: "test",
    content: "test",
    metadata: JSON.stringify({ test: "test" }),
  },
  {
    id: "2",
    tenantId: "2",
    channelId: "2",
    time: Date.now(),
    event: "test2",
    content: "test2",
    metadata: JSON.stringify({ test2: "test2" }),
  },
]);
```



## Querying Endpoints

```ts
// lib/tinybird.ts
import { Tinybird } from "./client";
import { z } from "zod";

const tb = new Tinybird({ token: process.env.TINYBIRD_TOKEN! });

export const getChannelActivity = tb.buildPipe({
  pipe: "get_channel_activity__v1",
  parameters: z.object({
    tenantId: z.string(),
    channelId: z.string().optional(),
    start: z.number(),
    end: z.number().optional(),
    granularity: z.enum(["1m", "1h", "1d", "1w", "1M"]),
  }),
  data: z.object({
    time: z.string().transform((t) => new Date(t).getTime()),
    count: z
      .number()
      .nullable()
      .optional()
      .transform((v) => (typeof v === "number" ? v : 0)),
  }),
});
```

```ts
// somewhere.ts
import { getChannelActivity } from "./lib/tinybird";


const res = await getChannelActivity({
   tenantId: "1",
   channelId: "1",
   start: 123,
   end: 1234,
   granularity: "1h"
})

```
`res` is the response from the tinybird endpoint, but now fully typed and the data has been parsed according to the schema defined in `data`.

## Error handling

For `429` (rate limit exceeded) and `500` errors, `zod-bird` automatically retries them ([source code](https://github.com/chronark/zod-bird/blob/52944e5aebcfb547f9ccaf1e3fca03b158953e8f/src/client.ts#L45-L60)).

All other errors will throw an `Exception`, so you should catch it via a try catch:

```typescript
try {
    const res = await getEvents({ tenantId: "chronark" })
    return res.data
} catch (e) {
    console.error(e)
    return e.messsage
}
```

## Advanced

### Caching

You can add cache directives to each pipe.


#### Disable caching (useful in Next.js where everything is cached by default)

```ts
tb.buildPipe({
  pipe: "some_pipe__v1",
  parameters: z.object({
   hello: z.string()
  }),
  data: z.object({
    response: z.string()
  }),
   opts: {
      cache: "no-store" // <-------- Add this
   };
});

```

#### Cache for 15 minutes

```ts

tb.buildPipe({
  pipe: "some_pipe__v1",
  parameters: z.object({
   hello: z.string()
  }),
  data: z.object({
    response: z.string()
  }),
   opts: {
      next: {
        revalidate: 900 ;// <-------- Add this
      }
    };
});
