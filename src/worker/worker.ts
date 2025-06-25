import {connectToDatabase, Database} from "../util/db";
import {parentPort, workerData} from "worker_threads"
import {Cron} from "croner";
import {readTimezone} from "../util/time";
import {OperationsByType} from "../util/subscription";
import {SEQ_TO_COLLATE, THRESHOLD_DIVERGENCE} from "../util/defaults";
import {processFeedData} from "../util/feed";
import {processOpps} from "./process";

const CREATE_POST_KEEP_KEY_SEQ = 30 * SEQ_TO_COLLATE;

export type workerConfig = {
    db: Database,
    opsByType?: OperationsByType,
    lastSeq: number,
    threadId: string,
    index: number,
    divergence: number,
    listToFeedMap?:Map<string, any[]>
    listItemToFeedMap?:Map<string, any[]>
    cachedFeeds?: any,
    postIds:Map<string, number>
}

const run = async () => {
    const {feedData, threadId, index} = workerData;
    console.log(threadId, "worker start");
    readTimezone(threadId);
    console.log(threadId, "get db");
    const db = await connectToDatabase(`${threadId}`);
    if (!db) { throw `${threadId} cannot connect to db`; }


    const config:workerConfig = {
        index, db, lastSeq: NaN, threadId:`${threadId}`, divergence:NaN, postIds: new Map()
    }

    console.log("thread id", threadId);
    processFeedData(feedData, config);

    let toUpdateFeeds = false, updatingFeeds = false;
    let changeStream;
    let cachedResumeToken;

    const establishChangeStream = async (collection, config, callback) => {
        let options:any = { fullDocument: "updateLookup" };
        if (cachedResumeToken) {
            options.resumeAfter = cachedResumeToken
        }

        changeStream = db.db.collection(collection).watch(config, options);
        changeStream.on('change', async (change) => {
            console.log(`${collection}: changed`);
            cachedResumeToken = change._id;
            callback(change);
        });

        changeStream.on('error', async () => {
            console.log(`${collection}: change stream ERROR`);
            await new Promise(resolve => setTimeout(resolve, 10 * 1000));
            await establishChangeStream(collection, config, callback);
        });
        console.log(threadId, `${collection}: trying to watch`);
        toUpdateFeeds = true; // trigger a future update
    }
    await establishChangeStream("feeds",[
        {
            $match: {
                $or: [
                    {operationType: "insert"},
                    {operationType: "delete"},
                    {operationType: "update", "updateDescription.updatedFields.updated": { $exists: true } },
                    {operationType: "update", "updateDescription.updatedFields.everyList": { $exists: true } } // triggered by self
                ]
            }
        }
    ],change => {toUpdateFeeds = true;});
    updatingFeeds = true;
    const cronJob = Cron('*/10 * * * *', async () => {
        if (toUpdateFeeds && !updatingFeeds) {
            toUpdateFeeds = false;
            console.log(threadId, "Update feeds triggered!!");
            updatingFeeds = true;
            const feedData = await db.feeds.find({}).toArray();
            processFeedData(feedData, config);
            updatingFeeds = false;
        }
    });
    updatingFeeds = false;

    parentPort!.postMessage({t:"k"});
    parentPort!.on("message", async ({opsByType, lastSeq, ts, firstPush}) => {
        console.log("  ", threadId, lastSeq, "received");
        // Delete old postIds
        let toDelete:string[] = [];
        for (const [key, value] of Object.entries(config.postIds)) {
            if (lastSeq - value > CREATE_POST_KEEP_KEY_SEQ) {
                toDelete.push(key);
            }
        }
        for (const key of toDelete) {
            config.postIds.delete(key);
        }

        config.lastSeq = lastSeq;
        config.opsByType = opsByType;

       // await new Promise(resolve => setTimeout(resolve, 10000));
        await processOpps (config);

        if (!isNaN(config.divergence) && config.divergence > THRESHOLD_DIVERGENCE) {
            parentPort?.postMessage({t:"n", n:lastSeq, ts, slow:true, firstPush});
        } else {
            parentPort?.postMessage({t:"n", n:lastSeq, ts, firstPush});
        }
    });


}

run();