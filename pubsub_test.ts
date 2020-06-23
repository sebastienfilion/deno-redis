import { assert, assertEquals } from "./vendor/https/deno.land/std/testing/asserts.ts";
import { delay } from "./vendor/https/deno.land/std/async/mod.ts";

import { connect } from "./redis.ts";
const { test } = Deno;
const addr = {
  hostname: "127.0.0.1",
  port: 6379,
};

test({
  name: "testSubscribe",
  async fn() {
    try {
      const redis = await connect(addr);
      const sub = await redis.subscribe("subsc");
      // const hoge = await redis.get("hoge");
      const unsub = await sub.unsubscribe("subsc");
      await sub.close();
      assertEquals(sub.isClosed, true);
      redis.close();
    } catch (error) {
      throw error;
    }
  },
});

test({
  name: "testSubscribe2",
  async fn() {
    const redis = await connect(addr);
    const pub = await connect(addr);
    const sub = await redis.subscribe("subsc2");
    const p = (async function () {
      const it = sub.receive();
      return (await it.next()).value;
    })();
    await pub.publish("subsc2", "wayway");
    const message = await p;
    assertEquals(message, {
      channel: "subsc2",
      message: "wayway",
    });
    await sub.close();

    assertEquals(sub.isClosed, true);
    assertEquals(redis.isClosed, true);

    try {
      await redis.get("aaa");
    } catch (error) {
      // This should now fail because the connection has been closed and
      //   RedisConnection instance is shared properly.
      if (!(error instanceof Deno.errors.BadResource)) throw error;
    } finally {
      pub.close();
    }
  },
});

test({
  name: "testSubscribe3",
  async fn() {
    const redis = await connect(addr);
    const pub = await connect(addr);
    const sub = await redis.psubscribe("ps*");
    let message1;
    let message2;
    const it = sub.receive();
    const p = (async function () {
      message1 = (await it.next()).value;
      message2 = (await it.next()).value;
    })();
    await pub.publish("psub", "wayway");
    await pub.publish("psubs", "heyhey");
    await p;
    assertEquals(message1, {
      pattern: "ps*",
      channel: "psub",
      message: "wayway",
    });
    assertEquals(message2, {
      pattern: "ps*",
      channel: "psubs",
      message: "heyhey",
    });
    await sub.close();
    pub.close();
    redis.close();
  },
});

test({
  name: "testSubscribe4 (#83)",
  // sanitizeResources: false,
  // sanitizeOps: false,
  async fn(): Promise<any> {
    let parallelPromiseList: Promise<any>[] = [];

    const throwawayRedisServerPort = 6464;
    let promiseList;
    let throwawayRedisServerChildProcess = createThrowawayRedisServer(throwawayRedisServerPort);

    await delay(500);

    const redisClient = await connect({ ...addr, name: "Main", port: throwawayRedisServerPort });
    const publisherRedisClient = await connect(
      { ...addr, maxRetryCount: 10, name: "Publisher", port: throwawayRedisServerPort }
    );
    const subscriberRedisClient = await redisClient.psubscribe("ps*");

    const messageIterator = subscriberRedisClient.receive();

    const interval = setInterval(
      () => {
        try {
          parallelPromiseList.push(publisherRedisClient.publish("psub", "wayway"));
        } catch (error) {}
      },
      900
    );

    setTimeout(
      () => {
        // console.debug("Kill the Redis server...");
        throwawayRedisServerChildProcess.kill(Deno.Signal.SIGTERM);
        throwawayRedisServerChildProcess.close();

        // console.debug("    ...done!");
      },
      1000
    )

    setTimeout(async () => {
      assertEquals(redisClient.isConnected, false, "The main client still thinks it is connected.");
      assertEquals(publisherRedisClient.isConnected, false, "The publisher client still thinks it is connected.");
      assert(parallelPromiseList.length < 5, "Too many messages were published.");

      // console.debug("Reconnect Redis client...");

      throwawayRedisServerChildProcess = createThrowawayRedisServer(throwawayRedisServerPort);

      await delay(500);
      const temporaryRedisClient = await connect({ ...addr, port: throwawayRedisServerPort });
      await temporaryRedisClient.ping();
      temporaryRedisClient.close();

      // console.debug("    ...done!");

      await delay(1000);

      assertEquals(redisClient.isConnected, true, "The main client is not connected.");
      assertEquals(publisherRedisClient.isConnected, true, "The publisher client is not connected.");
    }, 2000);

    promiseList = Promise.all([
      messageIterator.next(),
      messageIterator.next(),
      messageIterator.next(),
      messageIterator.next(),
      messageIterator.next()
    ]);

    await promiseList;

    // console.debug("All done... Cleaning up.");

    clearInterval(interval);

    throwawayRedisServerChildProcess.kill(Deno.Signal.SIGTERM);
    throwawayRedisServerChildProcess.close();
    publisherRedisClient.close();
    redisClient.close();
  },
});

function createThrowawayRedisServer (port: number) {

  return Deno.run(
    {
      cmd: [ "redis-server", "--port", port.toString() ],
      stdin: "null",
      stdout: "null"
    }
  );
}
