import dotenv from 'dotenv'
import path from "node:path";
import EventEmitter from "node:events";
import {connectToDatabase} from "./util/db";
import {Worker} from "node:worker_threads";
import {readTimezone} from "./util/time";

import {FirehoseSubscription} from "./subscription";

export type configType = {
    threadId: string,
    index: number,
    db: any,
    controller: AbortController,
    worker?: Worker,
    locked:boolean,
    gate: EventEmitter,
    cursor: number,
    endAt: number
    lastSubmit: number,
    active: boolean,
    slowCount: number,
    subscription?: FirehoseSubscription
};

const projection = {
    _id:1, everyList:1, everyListSync:1, blockList:1, blockListSync:1,
    allowList:1, allowListSync:1, keywords:1, keywordsQuote:1, mode:1, everyListBlockKeyword:1,
    pics:1, keywordSetting: 1, allowLabels:1, mustLabels:1, postLevels:1, languages:1
};

(async () => {
    console.log("\n=====================================================\n");
    dotenv.config();
    const headerId = `${new Date().getTime()}`.slice(-6);
    readTimezone(headerId);
    console.log(headerId,"start");

    const db = await connectToDatabase(`${headerId} root:`);
    console.log(headerId,"downloading initial feeds");

    const feedData = await db.feeds.find({}).project(projection).toArray();
    console.log(headerId,"feeds downloaded");
    const configs:configType[] = [];

    const val = parseInt(process.env.NUM_OF_PARALLEL || "", 10);
    const NUM_OF_PARALLEL = isNaN(val)? 1 : val;
    console.log("PARALLEL", NUM_OF_PARALLEL);

    const promises:any[] = [];
    for (let i=0; i < NUM_OF_PARALLEL; i++) {
        const threadId = `${headerId}-${i}`;
        const config:configType = {
            threadId,
            index: i,
            db,
            controller: new AbortController(),
            locked: true,
            gate: new EventEmitter(),
            cursor: NaN,
            endAt: -1,
            lastSubmit: NaN,
            active: false,
            slowCount: 0
        };
        configs.push(config);
        // Only the RAW feed is passed because processing it involves pointers that doesn't work when copied across to worker thread
        const worker = new Worker(path.resolve(__dirname, './worker/worker'), {workerData: {feedData, threadId, index:i}});
        promises.push(new Promise(resolve => config.gate.once('open', resolve)));

        config.worker = worker;
        worker.on('message', ({t, n, ts, slow, firstPush}) => {
            if (t === "n") {
                if (ts === config.lastSubmit || config.lastSubmit === -1) {
                    if (config.lastSubmit === -1) {
                        console.log(threadId, "BANDAID", ts, "!=", config.lastSubmit, n);
                    }
                    if (slow) {
                        config.slowCount++;
                        if (config.slowCount >= 3) {
                            config.slowCount = 0; // reset to 0 to stop re-attempting too often

                            // Start other worker to listen to latest
                            const otherIndex = (config.index+1)%configs.length;
                            const otherConfig = configs[otherIndex];
                            if (!otherConfig.active) {
                                console.log(threadId, "SLOW!! Start Other");
                                config.slowCount = 0;
                                db.data.updateOne({_id: `c_${otherIndex}`}, {$set: {cursor: -1, endAt:-1}}, {upsert:true});

                                otherConfig.active = true;
                                otherConfig.slowCount = 0;
                                otherConfig.cursor = -1;
                                otherConfig.endAt = -1;
                                otherConfig.subscription!.run().then(() => {});
                            } else {
                                console.log(threadId, "SLOW!! Other running");
                            }
                        } else {
                            console.log(threadId, "SLOW!! Wait for ", config.slowCount);
                        }
                    } else {
                        config.slowCount = 0; // reset to 0
                    }

                    if (config.endAt > 0 && config.cursor >= config.endAt) {
                        config.controller.abort("complete");
                        config.active = false;
                        config.slowCount = 0;
                        config.cursor = NaN;
                        config.endAt = -1;
                        db.data.deleteOne({_id: `c_${i}`});
                    } else {
                        config.cursor = n;
                        db.data.updateOne({_id: `c_${i}`}, {$set: {cursor: n, endAt: config.endAt}}, {upsert:true});

                        if (firstPush) {
                            const otherIndex = (config.index+1)%configs.length;
                            const otherConfig = configs[otherIndex];
                            if (otherConfig.active && otherConfig.cursor > 0 && otherConfig.endAt === -1) {
                                if (otherConfig.cursor < config.cursor) {
                                    otherConfig.endAt = n;
                                    db.data.updateOne({_id: `c_${otherIndex}`}, {$set: {endAt: n}});
                                }
                            }
                        }
                    }
                } else {
                    console.log(threadId, "cursor rejected", ts, "!=", config.lastSubmit, n);
                    // Gate will remain locked, do something?
                    return;
                }
            }
            config.locked = false;
            config.gate.emit('open');
        });
        worker.on('error', (err) => {
            console.error(err);
            throw `${threadId} worker fail!`;
        });

    }

    if (promises.length > 0) {
        console.log(headerId, "blocked until workers loaded", promises.length);
        await Promise.all(promises);
        console.log(headerId, "workers loaded");
    }

    await updateConfigFromDb(db, configs, NUM_OF_PARALLEL);


    for (let i= 0; i<NUM_OF_PARALLEL;i++) {
        const {active, threadId, cursor, endAt} = configs[i];
        console.log(threadId, active, cursor, endAt);
    }


    for (const config of configs) {
        const firehose = new FirehoseSubscription('wss://bsky.network', config);
        config.subscription = firehose;
        if (config.active) {
            firehose.run().then(() => {});
        }
    }
})();

async function updateConfigFromDb(db, configs: configType[], NUM_OF_PARALLEL) {
    const $in:string[] = [];
    for (const config of configs) {
        $in.push(`c_${config.index}`);
    }

    let active = 0;
    const results = await db.data.find({_id: {$in}}).toArray();
    console.log(JSON.stringify(results, null,2));
    for (const result of results) {
        const {_id, cursor, endAt} = result;
        const [_, i] = _id.split("_");
        const index = parseInt(i);
        if (!isNaN(index) && index < NUM_OF_PARALLEL) {
            if (endAt < 0 || cursor < endAt) {
                active++;
                configs[index].active = true;
                configs[index].cursor = cursor;
                configs[index].endAt = endAt;
            }
        }
    }
    if (active === 0) {
        configs[0].active = true;
    }
}
