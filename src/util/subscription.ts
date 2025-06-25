import {BlobRef} from "@atproto/lexicon";
import { ids, lexicons } from '../lexicon/lexicons'
import { Record as PostRecord } from '../lexicon/types/app/bsky/feed/post'
import { Record as RepostRecord } from '../lexicon/types/app/bsky/feed/repost'
import { Record as LikeRecord } from '../lexicon/types/app/bsky/feed/like'
import { Record as FollowRecord } from '../lexicon/types/app/bsky/graph/follow'
import { Record as ListItemRecord } from "../lexicon/types/app/bsky/graph/listitem"
import { Record as ListRecord} from "../lexicon/types/app/bsky/graph/list"
import { Record as FeedRecord} from "../lexicon/types/app/bsky/feed/generator"
import {
    Commit,
} from '../lexicon/types/com/atproto/sync/subscribeRepos'
import { cborToLexRecord, readCar } from '@atproto/repo'


export const appendOpsByType = async (evt: Commit, opsByType:OperationsByType) => {
    const car = await readCar(evt.blocks);

    const nowTs = new Date().getTime();

    for (const op of evt.ops) {
        const uri = `at://${evt.repo}/${op.path}`
        const parts = op.path.split("/");
        const collection = parts[0];
        if (op.action === 'create') {
            if (!op.cid) continue
            const recordBytes = car.blocks.get(op.cid)
            if (!recordBytes) continue
            let record = cborToLexRecord(recordBytes);
            const create = { uri, cid: "", author: evt.repo }
            if (collection === ids.AppBskyFeedPost && isPost(record)) {
                const tid = parts.at(-1) || "";
                if (tid.length !== 13 || !/^[234567abcdefghijklmnopqrstuvwxyz]*$/.test(tid)) {
                    continue;
                }

                const thenTs = new Date(record.createdAt).getTime();
                const diffTs = nowTs - thenTs;
                if(diffTs > 43200000 // 12 hours past
                    || diffTs < -600000 // 10 min future
                ) {
                    continue;
                }

                let embed:any = record.embed;
                if (!!embed) {
                    const embedType = embed["$type"];
                    switch (embedType) {
                        case "app.bsky.embed.recordWithMedia": {
                            const quoteUri = embed.record?.record?.uri;

                            const hasImages = !!embed.media?.images;
                            const hasExternal = !!embed.external;
                            const hasVideo = !!embed.media?.video;

                            const imageAlt = embed.media?.images?.map(x => {return {alt: x.alt}});
                            const externalUri = embed.external?.uri;
                            const videoAlt = embed.media?.video?.alt;
                            embed = {$type: embedType};
                            if (quoteUri) { embed.record = { record: { uri: quoteUri } }; }
                            if (hasImages) { embed.media = { images : imageAlt}; }
                            if (hasExternal) { embed.external = {uri: externalUri}; }
                            if (hasVideo) { embed.media = {video: {alt:videoAlt}}; }
                            break;
                        }
                        case "app.bsky.embed.images": {
                            embed = {$type: embedType, images: embed.images?.map(x => {return {alt: x.alt}}) || [] }; break;
                        }
                        case "app.bsky.embed.video": {
                            embed = {$type: embedType, video: { alt: embed.video?.alt }}; break;
                        }
                        case "app.bsky.embed.record": {
                            embed = {$type: embedType, record: {uri: embed.record?.uri}}; break;
                        }
                        case "app.bsky.embed.external": {
                            embed = {$type: embedType, external: {uri: embed.external?.uri}}; break;
                        }
                    }
                }

                record = {
                    $type: record.$type,
                    text: record.text,
                    entities: record.entities,
                    facets: record.facets,
                    reply: record.reply,
                    embed,
                    createdAt: record.createdAt,
                    langs: record.langs,
                    labels: record.labels,
                };
                // @ts-ignore
                opsByType.posts.creates.push({ record, ...create });
            } else if (collection === ids.AppBskyFeedRepost && isRepost(record)) {
                opsByType.reposts.creates.push({ record, ...create });
            } else if (collection === ids.AppBskyFeedLike && isLike(record)) {
                opsByType.likes.creates.push({ record, ...create });
            } else if (collection === ids.AppBskyGraphListitem && isListItem(record)) {
                opsByType.listItems.creates.push({record, ...create});
            }
        } else if (op.action === 'delete') {
            let arr;
            switch (collection) {
                case ids.AppBskyFeedPost: {arr = opsByType.posts;break;}
                case ids.AppBskyFeedRepost: {arr = opsByType.reposts;break;}
                case ids.AppBskyFeedLike: {arr = opsByType.likes;break;}
                case ids.AppBskyGraphListitem: {arr = opsByType.listItems;break;}
                case ids.AppBskyGraphList: {arr = opsByType.lists; break;}
            }
            if (arr) {
                arr.deletes.push({ uri, author: evt.repo });
            }
        }
    }
}

export type OperationsByType = {
    posts: Operations<PostRecord>
    reposts: Operations<RepostRecord>
    likes: Operations<LikeRecord>
    follows: Operations<FollowRecord>
    listItems: Operations<ListItemRecord>
    lists: Operations<ListRecord>
    feeds: Operations<FeedRecord>
}


export type Operations<T = Record<string, unknown>> = {
    creates: CreateOp<T>[]
    deletes: DeleteOp[]
    updates?: UpdateOp<T>[]
}

export type UpdateOp<T> = {
    uri: string,
    author: string,
    record: T
}

export type CreateOp<T> = {
    uri: string
    cid: string
    author: string
    record: T
}

export type DeleteOp = {
    uri: string
    author: string
}

export const isPost = (obj: unknown): obj is PostRecord => {
    return isType(obj, ids.AppBskyFeedPost);
}

export const isRepost = (obj: unknown): obj is RepostRecord => {
    return isType(obj, ids.AppBskyFeedRepost);
}

export const isLike = (obj: unknown): obj is LikeRecord => {
    return isType(obj, ids.AppBskyFeedLike);
}

export const isFollow = (obj: unknown): obj is FollowRecord => {
    return isType(obj, ids.AppBskyGraphFollow);
}

export const isListItem = (obj: unknown): obj is ListItemRecord => {
    return isType(obj, ids.AppBskyGraphListitem);
}

export const isFeedGenerator = (obj: unknown): obj is FeedRecord => {
    return isType(obj, ids.AppBskyFeedGenerator);
}

const isType = (obj: unknown, nsid: string) => {
    try {
        lexicons.assertValidRecord(nsid, fixBlobRefs(obj))
        return true
    } catch (err) {
        return false
    }
}

// @TODO right now record validation fails on BlobRefs
// simply because multiple packages have their own copy
// of the BlobRef class, causing instanceof checks to fail.
// This is a temporary solution.
const fixBlobRefs = (obj: unknown): unknown => {
    if (Array.isArray(obj)) {
        return obj.map(fixBlobRefs)
    }
    if (obj && typeof obj === 'object') {
        if (obj.constructor.name === 'BlobRef') {
            const blob = obj as BlobRef
            return new BlobRef(blob.ref, blob.mimeType, blob.size, blob.original)
        }
        return Object.entries(obj).reduce((acc, [key, val]) => {
            return Object.assign(acc, { [key]: fixBlobRefs(val) })
        }, {} as Record<string, unknown>)
    }
    return obj
}
