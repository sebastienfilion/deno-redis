import { BufReader, BufWriter } from "./vendor/https/deno.land/std/io/bufio.ts";
import {
  createRequest,
  readReply,
  RedisRawReply,
  CommandExecutor,
  ErrorReply,
} from "./io.ts";
import { ErrorReplyError } from "./errors.ts";
import { create } from "./redis.ts";
import {
  deferred,
  Deferred,
} from "./vendor/https/deno.land/std/async/mod.ts";
import { RedisCommands } from "./command.ts";
import { RedisConnection } from "./connection.ts";

const encoder = new TextEncoder();
export type RawReplyOrError = RedisRawReply | ErrorReply;
export type RedisPipeline = {
  enqueue(command: string, ...args: (number | string)[]): void;
  flush(): Promise<RawReplyOrError[]>;
} & RedisCommands;

export function createRedisPipeline(
  connection: RedisConnection,
  opts?: { tx: true },
): RedisPipeline {
  let commands: string[] = [];
  let queue: {
    commands: string[];
    d: Deferred<RawReplyOrError[]>;
  }[] = [];

  function dequeue() {
    const [e] = queue;
    if (!e) return;
    send(e.commands)
      .then(e.d.resolve)
      .catch(e.d.reject)
      .finally(() => {
        queue.shift();
        dequeue();
      });
  }

  async function send(cmds: string[]): Promise<RawReplyOrError[]> {
    const writerAsBuffer = connection.writer! as BufWriter;
    const msg = cmds.join("");
    await writerAsBuffer.write(encoder.encode(msg));
    await writerAsBuffer.flush();
    const ret: RawReplyOrError[] = [];
    for (let i = 0; i < cmds.length; i++) {
      try {
        const rep = await readReply(connection.reader! as BufReader);
        ret.push(rep);
      } catch (e) {
        if (e instanceof ErrorReplyError) {
          ret.push(["error", e]);
        } else {
          throw e;
        }
      }
    }
    return ret;
  }

  function enqueue(command: string, ...args: (number | string)[]): void {
    const msg = createRequest(command, ...args);
    commands.push(msg);
  }

  async function flush(): Promise<RawReplyOrError[]> {
    // wrap pipelined commands with MULTI/EXEC
    if (opts?.tx) {
      commands.unshift(createRequest("MULTI"));
      commands.push(createRequest("EXEC"));
    }
    const d = deferred<RawReplyOrError[]>();
    queue.push({ commands, d });
    if (queue.length === 1) {
      dequeue();
    }
    commands = [];
    return d;
  }
  async function exec(
    command: string,
    ...args: (string | number)[]
  ): Promise<RedisRawReply> {
    enqueue(command, ...args);
    return ["status", "OK"];
  }
  const d = dummyReadWriteCloser();
  const executor: CommandExecutor = { exec };
  const fakeRedis = create(d, d, d, executor);
  return Object.assign(fakeRedis, executor, { enqueue, flush });
}

function dummyReadWriteCloser(): Deno.Reader & Deno.Writer & Deno.Closer {
  return {
    close() {},
    async read(p) {
      return 0;
    },
    async write(p) {
      return 0;
    },
  };
}
