import {prepKeywords} from "../util/textAndKeywords";
import {workerConfig} from "../worker/worker";

const listsToDids = (l) => {
    let list = l || [];
    return list.map(x => x.did);
}

export function processFeedData (feedData:any[], config: workerConfig) {
    const listToFeed:Map<string, any[]> = new Map();
    const listItemToFeed:Map<string, any[]> = new Map();
    config.cachedFeeds = feedData.reduce((acc, feed) => {
        const {everyListSync, blockListSync, allowListSync, viewersSync, keywords, keywordsQuote, mode, everyListBlockKeyword} = feed;
        if (mode === "posts") {
            return acc;
        }
        feed.keywords = prepKeywords(keywords || []);
        feed.keywordsQuote = prepKeywords(keywordsQuote || []);
        feed.everyListBlockKeyword = prepKeywords(everyListBlockKeyword || []);

        const addToMap = (type, syncId, list:any[]=[]) => {
            let key = `${type} ${syncId}`;
            let items = listToFeed.get(key) || [];
            items.push(feed);
            listToFeed.set(key, items);

            for (const listItem of list) {
                const {uri} = listItem;
                key = `${type} ${uri}`;
                items = listItemToFeed.get(key) || [];
                items.push(feed);
                listItemToFeed.set(key, items);
            }
        }

        if (everyListSync) { addToMap("e", everyListSync, feed.everyList); }
        if (blockListSync) { addToMap("b", blockListSync, feed.blockList); }
        if (allowListSync) { addToMap("a", allowListSync, feed.allowList); }

        feed.allowList = listsToDids(feed.allowList);
        feed.everyList = listsToDids(feed.everyList);
        feed.blockList = listsToDids(feed.blockList);

        acc[mode].push(feed);
        return acc;
    }, {live:[], responses:[],"user-likes":[], "user-posts":[]});

    config.listToFeedMap = listToFeed;
    config.listItemToFeedMap = listItemToFeed;
}