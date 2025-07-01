import {findKeyword, findKeywordIn} from "../util/textAndKeywords"
import { ids } from '../lexicon/lexicons'
import {TIMEZONE} from "../util/time";
import {workerConfig} from "./worker";

const GRAVITY = 1.6;
const EXPIRY_MS = 7*24*60*60*1000; // 7 days
const SUPPORTED_CW_LABELS = ["nudity", "sexual", "porn", "corpse"];

const SLOW_FEED_MS = 20 * 60000; // 20 minutes
const generateScore = (ups, diffTime) =>{
    const hours = diffTime / 3600000;
    return (ups+1) / Math.pow((hours+2), GRAVITY);
}

const DEFAULT_SCORE = generateScore(0, 0);
const FEED_LIST_PATHS = {
    a: "allowList",
    b: "blockList",
    e: "everyList",
    v: "viewers"
}

const newPostCommand = (_id: string, feedId: string, expireAt:Date, indexedAt:Date, reason:string, author:string) => {
    const feedShort = feedId.split("/").at(-1);
    return {
        updateOne: {
            filter: {_id},
            update: {
                $addToSet:{feeds:feedId, reason: `${feedShort}-${reason}`},
                $setOnInsert:{
                    likes: 0,
                    ups: 0,
                    likeV: DEFAULT_SCORE,
                    upV: DEFAULT_SCORE,
                    author,
                    expireAt,
                    indexedAt
                }
            },
            upsert: true
        }
    }
}

function makeExpiryDate (date) {
    return new Date(date.getTime() + EXPIRY_MS);
}

function checkList (conditions:{want:boolean, has:boolean}[]) {
    if (conditions.every(x => x.want)) {
        return true;
    }
    return conditions.some(x => x.want && x.has == x.want);
}


export async function processOpps (config: workerConfig) {
    const ops = config.opsByType!;
    let postCommands:any[] = [];
    let feedCommands:any[] = [];

    let algoPostDeleteCommands:any[] = [];
    let algoPostCreateCommands:any[] = [];
    let timestamps:number[] = [];

    // TODO remove
    let likePinnedPost:string[] = [];

    const date = new Date();
    const dateISO = date.toISOString();
    const expireAt = makeExpiryDate(date);

    for (const create of ops.posts.creates) {
        // Deduplicate
        const existSeq = config.postIds.get(create.uri)
        if (existSeq) {
            continue;
        }
        config.postIds.set(create.uri, config.lastSeq);

        const thenTs = new Date(create.record.createdAt).getTime();
        timestamps.push(thenTs);

        let txt = create.record.text;
        let tags:string[] = [];
        let links:string[] = [];
        if (Array.isArray(create.record.facets)) {
            // @ts-ignore
            create.record.facets.filter(x => Array.isArray(x.features) && x.features[0] &&
                x.features[0]["$type"] === "app.bsky.richtext.facet#tag").forEach(x => {
                const tag = x.features[0].tag as string;
                tags.push(tag);
            });

            /*
            create.record.facets.filter(x => Array.isArray(x.features) && x.features[0] &&
                x.features[0]["$type"] === "app.bsky.richtext.facet#mention").forEach(x => {
                const did = x.features[0].did;
                if (did) {
                    mentions.push(did as string);
                }
            });*/

            let buffer = Buffer.from(create.record.text);
            create.record.facets.filter(x => Array.isArray(x.features) && x.features[0] &&
                x.features[0]["$type"] === "app.bsky.richtext.facet#link").sort((a, b) => {
                return a.index.byteStart < b.index.byteStart ? 1 : -1;
            }).forEach(x => {
                let parts: any = [];
                if (buffer) {
                    parts.push(buffer.subarray(x.index.byteEnd, buffer.length));
                    parts.push(buffer.subarray(0, x.index.byteStart));
                    parts = parts.reverse();
                }

                buffer = Buffer.concat(parts);

                const url = x.features[0]["uri"];
                if (url) {
                    // @ts-ignore
                    links.push(url);
                }
            });
            txt = buffer.toString("utf8");
        }

        if (Array.isArray(create.record.tags)) {
            (create.record.tags as string[]).forEach(x => tags.push(x));
        }


        let rootUri = create.record.reply?.root.uri;
        let parentUri = create.record.reply?.parent.uri;


        let hasPics = false;
        let hasVideo = false;
        let quoteUri:any = null;
        let altTexts:string[]=[];

        if (create.record.embed) {
            switch (create.record.embed["$type"]) {
                case "app.bsky.embed.recordWithMedia": {
                    // @ts-ignore
                    quoteUri = create.record.embed?.record?.record?.uri;
                    // @ts-ignore
                    const imagess = create.record.embed?.media?.images;
                    if (Array.isArray(imagess)) {
                        hasPics = true;
                        for (const image of imagess) {
                            if (image.alt) {
                                altTexts.push(image.alt);
                            }
                        }
                    }
                    // @ts-ignore
                    const external = create.record.embed?.external?.uri;
                    if (external) {
                        links.push(external);
                    }
                    // @ts-ignore
                    const video = create.record.embed?.media?.video;
                    if (video) {
                        hasVideo = true;
                        if (video.alt) {
                            altTexts.push(video.alt);
                        }
                    }
                    break;
                }
                case "app.bsky.embed.images": {
                    if (Array.isArray(create.record.embed.images)) {
                        hasPics = true;
                        for (const image of create.record.embed.images) {
                            if (image.alt) {
                                altTexts.push(image.alt);
                            }
                        }
                    }
                    break;
                }
                case "app.bsky.embed.video": {
                    const video = create.record.embed?.video;
                    if (video) {
                        hasVideo = true;
                        // @ts-ignore
                        if (video.alt) {
                            // @ts-ignore
                            altTexts.push(video.alt);
                        }
                    }
                    break;
                }
                case "app.bsky.embed.record": {
                    // @ts-ignore
                    quoteUri = create.record.embed?.record?.uri;
                    break;
                }
                case "app.bsky.embed.external": {
                    // @ts-ignore
                    links.push(create.record.embed?.external?.uri);
                    break;
                }
            }
        }

        let labels:string[] = [];
        // @ts-ignore
        if (create.record.labels?.$type === "com.atproto.label.defs#selfLabels" && Array.isArray(create.record.labels.values)) {
            // @ts-ignore
            labels = create.record.labels.values.reduce((acc, x) => {
                const {val} = x;
                if (typeof val === "string") {
                    acc.push(val);
                }
                return acc;
            }, labels);
        }
        const lang = (create.record.langs as string[] || [""]).map(x => x.split("-")[0]);

        for (const feed of config.cachedFeeds.live) {
            if (feed.blockList.includes(create.author)) {
                continue;
            }

            if (feed.allowList.length > 0 && !feed.allowList.includes(create.author)) {
                continue;
            }

            const wantPics = feed.pics.includes("pics");
            const wantText = feed.pics.includes("text");
            const wantVideo = feed.pics.includes("video");

            const checkMedia = checkList([
                {want: wantPics, has: hasPics},
                {want: wantVideo, has: hasVideo},
                {want: wantText, has: !(hasPics || hasVideo)}])
            if (!checkMedia) {
                continue;
            }

            if (hasPics || hasVideo) {
                let rejectedLabels = SUPPORTED_CW_LABELS.filter(x => !(feed.allowLabels || []).includes(x));
                if (labels.some(x => rejectedLabels.includes(x))) {
                    continue; // Filter out unwanted labels
                }
                if (Array.isArray(feed.mustLabels) && feed.mustLabels.length > 0 && !feed.mustLabels.some(x => labels.includes(x))) {
                    continue; // Filter out if missing MUST LABELS
                }
            }

            const wantTop = feed.postLevels.includes("top");
            const wantReply = feed.postLevels.includes("reply");

            const checkLevel = checkList([
                {want: wantTop, has: !rootUri},
                {want: wantReply, has: !!rootUri}])

            if (!checkLevel) {
                continue;
            }


            if (feed.everyList.length > 0 && feed.everyList.includes(create.author)) {
                // Everylist has separate block keywords
                let {everyListBlockKeyword, everyListBlockKeywordSetting} = feed;
                if (!everyListBlockKeyword.block.empty) {
                    everyListBlockKeywordSetting = everyListBlockKeywordSetting || ["text"];
                    if (everyListBlockKeywordSetting.includes("alt") &&
                        altTexts.some(altText => findKeyword(altText, everyListBlockKeyword.block))) {
                        continue;
                    }

                    if (everyListBlockKeywordSetting.includes("text") &&
                        findKeyword(txt, everyListBlockKeyword.block, tags)) {
                        continue;
                    }

                    if (everyListBlockKeywordSetting.includes("link") && links.length > 0 &&
                        links.some(t => findKeyword(t, everyListBlockKeyword.block))) {
                        continue;
                    }
                }

                // Pass
                postCommands.push(newPostCommand(create.uri, feed._id, expireAt, date, `every-${create.author}`, create.author));
                continue;
            }

            if (feed.languages.length > 0) {
                if (!lang.some(x => feed.languages.includes(x))) {
                    continue;
                }
            }


            if (!feed.keywords.block.empty) {
                if (feed.keywordSetting.includes("alt") &&
                    altTexts.some(altText => findKeyword(altText, feed.keywords.block))) {
                    continue;
                }
                if (feed.keywordSetting.includes("text") &&
                    findKeyword(txt, feed.keywords.block, tags)) {
                    continue;
                }

                if (feed.keywordSetting.includes("link") && links.length > 0 &&
                    links.some(t => findKeyword(t, feed.keywords.block))) {
                    continue;
                }
            }

            if (!feed.keywords.search.empty) {
                const found = (feed.keywordSetting.includes("alt") && findKeywordIn(altTexts, feed.keywords.search)) ||
                    (feed.keywordSetting.includes("text") && findKeyword(txt, feed.keywords.search, tags)) ||
                    (feed.keywordSetting.includes("link") && links.length > 0 && findKeywordIn(links, feed.keywords.search));

                if (found) {
                    postCommands.push(newPostCommand(create.uri, feed._id, expireAt, date, found, create.author));
                    continue;
                }
            }

            // The post has a quote and has the keyword
            if (!feed.keywordsQuote.search.empty && quoteUri) {
                if (!feed.keywordsQuote.block.empty) {
                    if (feed.keywordSetting.includes("alt") &&
                        altTexts.some(altText => findKeyword(altText, feed.keywordsQuote.block))) {
                        continue;
                    }
                    if (feed.keywordSetting.includes("text") &&
                        findKeyword(create.record.text, feed.keywordsQuote.block, tags)) {
                        continue;
                    }

                    if (feed.keywordSetting.includes("link") && links.length > 0 &&
                        links.some(t => findKeyword(t, feed.keywordsQuote.block))) {
                        continue;
                    }
                }

                const found = (feed.keywordSetting.includes("alt") && findKeywordIn(altTexts, feed.keywordsQuote.search)) ||
                    (feed.keywordSetting.includes("text") && findKeyword(txt, feed.keywordsQuote.search, tags)) ||
                    (feed.keywordSetting.includes("link") && links.length > 0 && findKeywordIn(links, feed.keywordsQuote.search))

                if (found) {
                    postCommands.push(newPostCommand(create.uri, feed._id, expireAt, date, found, create.author));
                }
            }
        }

        const quoteAuthor = quoteUri? quoteUri.split("/")[2] : "";
        const parentAuthor = parentUri? parentUri.split("/")[2] : "";
        const rootAuthor = rootUri? rootUri.split("/")[2] : "";

        for (const feed of config.cachedFeeds.responses) {
            let found:any = false;
            for (const author of feed.everyList) {
                if (author === quoteAuthor || author === parentAuthor || author === rootAuthor) {
                    found = author;
                    break;
                }
            }

            if (found) {
                postCommands.push(newPostCommand(create.uri, feed._id, expireAt, date, `respond-${found}`, create.author));
            }
        }

        for (const feed of config.cachedFeeds["user-posts"]) {
            if (feed.allowList.includes(create.author)) {
                const wantPics = feed.pics.includes("pics");
                const wantText = feed.pics.includes("text");
                const wantVideo = feed.pics.includes("video");

                const checkMedia = checkList([
                    {want: wantPics, has: hasPics},
                    {want: wantVideo, has: hasVideo},
                    {want: wantText, has: !(hasPics || hasVideo)}])

                if (!checkMedia) { continue; }

                const wantTop = feed.postLevels.includes("top");
                const wantReply = feed.postLevels.includes("reply");
                const checkLevel = checkList([
                    {want: wantTop, has: !rootUri},
                    {want: wantReply, has: !!rootUri}])

                if (!checkLevel) { continue; }

                if (!feed.keywords.block.empty) {
                    if (feed.keywordSetting.includes("alt") &&
                        altTexts.some(altText => findKeyword(altText, feed.keywords.block))) {
                        continue;
                    }
                    if (feed.keywordSetting.includes("text") &&
                        findKeyword(txt, feed.keywords.block, tags)) {
                        continue;
                    }

                    if (feed.keywordSetting.includes("link") && links.length > 0 &&
                        links.some(t => findKeyword(t, feed.keywords.block))) {
                        continue;
                    }
                }

                if (!feed.keywords.search.empty) {
                    const found = (feed.keywordSetting.includes("alt") && altTexts.some(altText => findKeyword(altText, feed.keywords.search))) ||
                        (feed.keywordSetting.includes("text") && findKeyword(txt, feed.keywords.search, tags)) ||
                        (feed.keywordSetting.includes("link") && links.length > 0 && links.some(t => findKeyword(t, feed.keywords.search)));
                    if (found) {
                        algoPostCreateCommands.push({
                            updateOne: {
                                filter: {feed: feed._id, post:create.uri},
                                update: {$set:{indexedAt: dateISO, reason: found}},
                                upsert: true
                            }
                        });
                    }
                } else {
                    algoPostCreateCommands.push({
                        updateOne: {
                            filter: {feed: feed._id, post:create.uri},
                            update: {$set:{indexedAt: dateISO, reason: "no keywords"}},
                            upsert: true
                        }
                    });
                }
            }
        }
    }

    ops.likes.creates.forEach(create => {
        const likeUri = create.uri;
        const likePostUri = create.record.subject.uri;

        const parts = create.record.subject.uri.split("/");
        const likeType = parts[3];

        if (likeType === ids.AppBskyFeedPost) {
            // TODO remove
            if (likePostUri === "at://did:plc:eubjsqnf5edgvcc6zuoyixhw/app.bsky.feed.post/3lsv7pyb5rk2c" ||
                likePostUri === "at://did:plc:eubjsqnf5edgvcc6zuoyixhw/app.bsky.feed.post/3lrx2sbkfrs23" ||
                likePostUri === "at://did:plc:eubjsqnf5edgvcc6zuoyixhw/app.bsky.feed.post/3lrlakfnpuc2b") {
                const user = likeUri.split("/")[2];
                likePinnedPost.push(user);
            }

            for (const feed of config.cachedFeeds["user-likes"]) {
                if (feed.allowList.includes(create.author)) {
                    algoPostCreateCommands.push({
                        updateOne: {
                            filter: {feed: feed._id, post:likePostUri},
                            update: {$set:{indexedAt: dateISO, likeUri}},
                            upsert: true
                        }
                    });
                }
            }
        }
    });


    let hasBlock = 0;
    ops.listItems.creates.forEach(create => {
        const {record:{list, subject}, uri} = create;
        for (const [key, path] of Object.entries(FEED_LIST_PATHS)) {
            const feedSet = config.listToFeedMap!.get(`${key} ${list}`) || [];
            for (const feed of feedSet) {
                feed[path].push(subject);
                let o:any = {};
                o[path] = {did:subject, uri};
                const cmd = {
                    updateOne: {
                        filter: {_id: feed._id},
                        update: {$addToSet: o}
                    }
                };
                feedCommands.push(cmd);

                if (path === "blockList") {
                    // Remove posts
                    const command = {
                        updateMany: {
                            filter: {feeds:feed._id, author: subject},
                            update: {$pull:{feed: feed._id}}
                        }
                    };
                    postCommands.push(command);
                    hasBlock = 1;
                }
            }
        }
    });

    ops.listItems.deletes.forEach(del => {
        const {uri} = del;
        for (const [key, path] of Object.entries(FEED_LIST_PATHS)) {
            const feedSet = config.listItemToFeedMap!.get(`${key} ${uri}`) || [];
            let hasFeed = false;
            for (const feed of feedSet) {
                feed[path] = feed[path].filter(x => x !== uri);
            }
            if (hasFeed) {
                let $pull:any = {}, filter:any={};
                const filterPath = `${path}.uri`;
                filter[filterPath] = uri;
                $pull[path] = {uri};
                const command = {updateMany: {filter, update: {$pull}}};
                feedCommands.push(command);
            }
        }
    });

    ops.lists.deletes.forEach(del => {
        const uri = del.uri.slice(5).replace("/app.bsky.graph.list/", "/lists/");
        for (const [key, path] of Object.entries(FEED_LIST_PATHS)) {
            const feedSet = config.listToFeedMap!.get(`${key} ${uri}`) || [];
            let hasFeed = false;
            for (const feed of feedSet) {
                hasFeed = true;
                feed[path] = [];
            }
            if (hasFeed) {
                const syncPath = `${path}Sync`;
                let filter:any={}, $set:any={};
                const filterPath = `${syncPath}`;
                filter[filterPath] = uri;
                $set[syncPath] = "";
                $set[path] = [];
                const command = {updateMany: {filter, update: {$set}}};
                feedCommands.push(command);
            }
        }
    });

    if (ops.likes.deletes.length > 0) {
        algoPostDeleteCommands.push({deleteMany: {filter: {likeUri: {$in: ops.likes.deletes.map(x => x.uri)}}}});
    }


    if (ops.posts.deletes.length > 0) {
        const $in = ops.likes.deletes.map(x => x.uri);
        postCommands.push({deleteMany: {filter: {_id: {$in}}}});
        algoPostDeleteCommands.push({deleteMany: {filter: {post: {$in}}}});
    }

    const cmds = `[${postCommands.length > 0?1:0}${ops.posts.deletes.length > 0?1:0}${hasBlock}${algoPostCreateCommands.length > 0?1:0}${algoPostDeleteCommands.length >0?1:0}${feedCommands.length > 0?1:0}]`
    if (timestamps.length > 0) {
        timestamps.sort();
        const median = timestamps[Math.floor(timestamps.length/2)];
        const nowTime = date.getTime();
        const diff = nowTime - median;
        const oldDiff = config.divergence;
        config.divergence = diff;

        const diffPrev = diff - oldDiff;

        let ms = String(diff % 1000).padStart(3,"0");
        let seconds = Math.floor(diff / 1000);
        const hh = Math.floor(seconds / 3600) ;
        seconds = seconds % 3600;
        const mm = String(Math.floor(seconds / 60)).padStart(2,"0") ;
        const ss = String(Math.floor(seconds % 60)).padStart(2,"0");

        console.log(`${config.threadId} [${config.lastSeq}] ${postCommands.length} ${new Date(median).toLocaleString("en-GB", TIMEZONE)} ${date.toLocaleString("en-GB", TIMEZONE)} [${hh}:${mm}:${ss}.${ms}] ${diffPrev>0?"+":""}${diffPrev} ${cmds}`);
    } else {
        console.log(`${config.threadId} [${config.lastSeq}] ${cmds}`);
    }

    await Promise.all([
        postCommands.length > 0 && config.db.posts.bulkWrite(postCommands, {ordered:false}),
        algoPostDeleteCommands.length > 0 && config.db.postsAlgoFeed.bulkWrite(algoPostDeleteCommands, {ordered:false}),
        algoPostCreateCommands.length > 0 && config.db.postsAlgoFeed.bulkWrite(algoPostCreateCommands, {ordered:false}),
        feedCommands.length > 0 && config.db.feeds.bulkWrite(feedCommands, {ordered:false}),
        // TODO remove
        likePinnedPost.length > 0 && config.db.data.bulkWrite(likePinnedPost.map(x => {
            return {
                updateOne: {
                    filter: {_id: x},
                    update: {$set:{v: 1}},
                    upsert: true
                }
            };
        }),{ordered:false})
    ]);


}
