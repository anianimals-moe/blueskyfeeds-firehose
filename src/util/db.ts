import {MongoClient, Db, Collection,} from "mongodb"
import {TIMEZONE} from "../util/time";

export class Database {
    db: Db
    feeds: Collection<any>
    posts: Collection<any>
    data: Collection<any>
    postsAlgoFeed: Collection<any>

    constructor(db) {
        this.db = db;
        this.feeds = db.collection("feeds");
        this.posts = db.collection("posts");
        this.data = db.collection("data");
        this.postsAlgoFeed = db.collection("postsAlgoFeed");
    }
}

const WAIT_DURATION = [
    30*1000,
    60000,
    3*60000,
    7*60000,
    18*60000,
    30*60000
]

export const connectToDatabase = async (note) => {
    console.log(note, "connecting to db");
    const URI_VAR = process.env.DB_URI;
    const PW_VAR = process.env.DB_PASSWORD;

    if (!URI_VAR || !PW_VAR) {
        throw new Error('Please define the environment variables inside .env');
    }

    const uri = PW_VAR === "null" ? URI_VAR : URI_VAR.replace("@", `${encodeURIComponent(PW_VAR)}@`);

    for (let i=0;i<WAIT_DURATION.length;i++) {
        try {
            const client = await MongoClient.connect(uri);
            return new Database(client.db('blueskyfeeds')) ;
        } catch (error) {
            const {message, code, name} = error;
            if (["MongoServerError", "MongoError", "MongoNetworkError"].includes(name)) {
                const waitDuration = WAIT_DURATION[i];
                console.log("DB error", name, code, message, new Date().toLocaleString("en-GB", TIMEZONE));
                console.log("DB waiting", waitDuration, "ms");
                await new Promise(resolve => setTimeout(resolve, waitDuration));
                console.log("DB retrying");
            } else {
                throw error;
            }
        }
    }

    throw "DB MAX RETRIES";
};

