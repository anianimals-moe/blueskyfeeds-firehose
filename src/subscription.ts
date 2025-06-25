import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import {Subscription} from "@atproto/xrpc-server";
import { ids, lexicons } from './lexicon/lexicons'
import {appendOpsByType, OperationsByType} from "./util/subscription";
import {configType} from "./index";
import {TIMEZONE} from "./util/time";
import {HEARTBEAT_MS, SEQ_TO_COLLATE} from "./util/defaults";


export class FirehoseSubscription {
  sub: Subscription<RepoEvent>
  config: configType
  opsByType: OperationsByType
  timeout?: ReturnType<typeof setTimeout>
  prev: { seq: number, time: number }
  last: { seq: number, time: number }
  subscriptionReconnectDelay: 3000

  constructor(public service: string, config: any) {
    this.config = config;
    this.resetOps();
    this.prev = {seq:NaN, time:NaN};
    this.last = {seq:NaN, time:NaN};

    this.sub = new Subscription({
      service,
      signal: config.controller.signal, // To handle aborts!
      method: ids.ComAtprotoSyncSubscribeRepos,
      getParams: async () => {
        const {cursor} = config;
        const toSend = cursor > 0? {cursor} : {};
        console.log(this.config.threadId, "using cursor again", toSend);
        return toSend;
      },
      heartbeatIntervalMs: HEARTBEAT_MS,
      validate: (value: unknown) => {
        try {
          return lexicons.assertValidXrpcMessage<RepoEvent>(ids.ComAtprotoSyncSubscribeRepos, value);
        } catch (err) {
          switch (err.message) {
            case 'Message must have the property "prev"': {
              (value as any).prev = null;
              return value as RepoEvent;
            }
            case 'Message must have the property "blocks"': { break; }
            default: { console.error('repo subscription skipped invalid message', err); }
          }
        }
      },
    });
  }

  resetOps() {
    this.opsByType = {
      posts: { creates: [], deletes: [] },
      reposts: { creates: [], deletes: [] },
      likes: { creates: [], deletes: [] },
      follows: { creates: [], deletes: [] },
      listItems: {creates:[], deletes: []},
      lists: {creates:[], deletes:[]},
      feeds: {creates:[], deletes:[], updates:[]}
    };
  }

  longTimeout () {
    console.log("Waiting for 1 minute!")
    setTimeout(() => {
      console.log(this.config.threadId, "Long wait passed");
      this.run();
    }, 60*1000);
  }

  async run() {
    console.log(this.config.threadId, "listening");
    let first = true;
    let firstPush = true;
    try {
      for await (const evt of this.sub) {
        try {
          if (isCommit(evt)) {
            if (first) {console.log(this.config.threadId, "got", evt.seq); first = false;}
            await appendOpsByType(evt, this.opsByType);
          } else {
            continue;
          }

        } catch (err) {
          if (err.message === "Could not decode varint") {} else {
            console.error('repo subscription could not handle message', err)
          }
        }
        const lastSeq = evt.seq as number;

        if (lastSeq % 1000 === 0) {
          console.log("  ", this.config.threadId, evt.seq);
        }


        if (lastSeq % SEQ_TO_COLLATE === 0) {
          const opsByType = this.opsByType;
          this.resetOps();

          if (this.config.locked) {
            console.log(this.config.threadId, "blocked");
            await new Promise(resolve => this.config.gate.once('open', resolve));
            console.log(this.config.threadId, "unblocked");
          }

          if (this.timeout) {
            clearTimeout(this.timeout);
            this.timeout = undefined;
          }

          let nowTime =  new Date().getTime();
          this.config.lastSubmit = nowTime;
          this.config.worker!.postMessage({opsByType, lastSeq, ts: nowTime, firstPush});
          this.config.locked = true;

          this.timeout = setTimeout( () => {
            this.config.controller.abort("timeout");
          }, 15000);

          const now = new Date();
          nowTime = now.getTime();
          this.prev = this.last;
          const blocks = this.last.seq < 0? NaN : lastSeq - this.last.seq;
          const diffTime = this.last.time < 0? NaN : nowTime - this.last.time;
          console.log(`${this.config.threadId} ${lastSeq} ${now.toLocaleString("en-GB", TIMEZONE)} ${diffTime}/${blocks} = ${(diffTime/blocks).toFixed(3)}`);
          this.last = { seq: lastSeq, time: nowTime };
          firstPush = false;
        }
      }
    } catch (err) {
      this.config.controller = new AbortController();
      this.sub.opts.signal = this.config.controller.signal;
      console.error(this.config.threadId, new Date().toLocaleString("en-GB", TIMEZONE), 'repo subscription errored', err);

      if (err === "complete") {}
      else if (err.code === 503) {
        console.log("Error Path A");
        this.longTimeout();
      } else if (err === "Unexpected server response: 503") {
        console.log("Error Path B");
        this.longTimeout();
      } else if (err === 503) {
        console.log("Error Path C");
        this.longTimeout();
      } else if (err === "503") {
        console.log("Error Path D");
        this.longTimeout();
      } else if (err.error === "Unexpected server response: 503") {
        console.log("Error Path E");
        this.longTimeout();
      } else if (err.error === "Unexpected server response") {
        console.log("Error Path F");
        this.longTimeout();
      } else if (err.error === "503") {
        console.log("Error Path G");
        this.longTimeout();
      } else {
        setTimeout(() => {
          console.log(this.config.threadId, "restarted!");
          this.run();
        }, this.subscriptionReconnectDelay);
      }


      this.config.slowCount = 0;
      this.config.lastSubmit = -1;
      if (this.timeout) {
        clearTimeout(this.timeout);
        this.timeout = undefined;
      }
      this.resetOps();
      this.config.locked = false;
      this.last = this.prev;
    }
  }
}
