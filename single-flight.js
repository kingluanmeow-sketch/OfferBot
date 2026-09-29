"use strict";

// A subscriber owns its cancellation, not somebody else's request. The last
// subscriber leaving aborts the underlying HTTP request and its limiter wait.
class SingleFlight {
  constructor() { this.pending = new Map(); this.joined = 0; }
  run(key, fn, signal) {
    if (signal?.aborted) return Promise.reject(Object.assign(new Error("Cancelled"), {name:"AbortError"}));
    let entry=this.pending.get(key);
    if (!entry) {
      entry={controller:new AbortController(),users:0,settled:false};
      this.pending.set(key,entry);
      entry.promise=Promise.resolve().then(()=>fn(entry.controller.signal)).finally(()=>{
        entry.settled=true; if(this.pending.get(key)===entry)this.pending.delete(key);
      });
    } else this.joined++;
    entry.users++;
    return new Promise((resolve,reject)=>{
      let done=false;
      const finish=(fn,value)=>{if(done)return;done=true;signal?.removeEventListener("abort",abort);entry.users--;
        if(!entry.users&&!entry.settled){entry.controller.abort();if(this.pending.get(key)===entry)this.pending.delete(key);}
        fn(value);
      };
      const abort=()=>finish(reject,Object.assign(new Error("Cancelled"),{name:"AbortError"}));
      signal?.addEventListener("abort",abort,{once:true});
      entry.promise.then(v=>finish(resolve,v),e=>finish(reject,e));
    });
  }
}
module.exports={SingleFlight};
