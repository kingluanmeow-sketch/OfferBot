"use strict";
const mono=()=>Number(process.hrtime.bigint())/1e6;
const SPANS=Object.freeze([
['frame_received','stream_decoded','local','frame→decode'],['stream_decoded','stream_mapped','local','decode→map'],['stream_mapped','book_updated','local','map→book'],
['event_received','book_updated','local','event→book'],['book_updated','decision_ready','local','book→decision'],
['decision_ready','intent_updated','local','decision→intent'],['intent_updated','quota_requested','scheduler','intent→quota'],
['event_received','sign_finished','local','sự kiện → ký xong (TOÀN BỘ phần cục bộ)'],
['order_queue_enter','order_queue_exit','scheduler','order queue'],['validation_started','validation_finished','local','validation'],
['quota_requested','quota_granted','broker','quota wait'],['build_started','build_finished','local','build'],['sign_started','sign_finished','local','sign'],
['quota_granted','http_started','local','quota grant→HTTP start'],
['http_started','socket_assigned','transport','socket queue'],['http_started','first_byte','external','HTTP first byte'],['http_started','http_finished','external','HTTP total'],
['event_received','http_started','total','event→HTTP'],['event_received','committed','total','event→accepted'],['http_started','stream_confirmation','confirmation','HTTP→stream confirmation']]);
/** An open latency trace older than this belongs to an intent that ended without a submit. */
const OPEN_TRACE_TTL_MS=10*60*1000;
const STAGES=Object.freeze([...new Set(SPANS.flatMap(s=>s.slice(0,2)))]);
class Trace{
 constructor(id,meta={}){this.id=id;this.meta=meta;this.marks=new Map();this.startedAt=mono();this.failed=false;this.reason='';}
 mark(stage,at=mono()){if(Number.isFinite(at)&&!this.marks.has(stage))this.marks.set(stage,at);return this;}
 span(a,b){return this.marks.has(a)&&this.marks.has(b)?this.marks.get(b)-this.marks.get(a):null;}
 fail(reason){this.failed=true;this.reason=String(reason);return this;}
 line(){return `${this.meta.chain||'?'} #${this.meta.tokenId||'?'} ${this.failed?this.reason:''} `+SPANS.map(([a,b,,s])=>`${s}=${this.span(a,b)}`).join(' · ');}
}
function quantiles(list){if(!list.length)return {n:0};const s=[...list].sort((a,b)=>a-b),at=p=>s[Math.max(0,Math.ceil(p*s.length)-1)];return {n:s.length,p50:at(.5),p90:at(.9),p95:at(.95),p99:at(.99),min:s[0],max:s.at(-1),mean:s.reduce((a,b)=>a+b,0)/s.length};}
class Metrics{
 constructor({sampleCap=2000,keepTraces=50}={}){this.sampleCap=sampleCap;this.keepTraces=keepTraces;this.samples=new Map();this.open=new Map();this.recent=[];this.pending=new Map();this.early=new Map();this.counts={committed:0,failed:0,dropped:0,confirmed:0,confirmationTimeout:0,invalidSpans:0};}
 start(id,meta){const t=new Trace(id,meta);this.open.set(id,t);if(this.open.size>this.sampleCap){this.open.delete(this.open.keys().next().value);this.count('dropped');}return t;}
 get(id){return this.open.get(id)||null;}
 mark(id,stage,at=mono()){return this.open.get(id)?.mark(stage,at);}
 sample(key,ms){if(!Number.isFinite(ms)||ms<0){this.count('invalidSpans');return;}const a=this.samples.get(key)||[];a.push(ms);if(a.length>this.sampleCap)a.shift();this.samples.set(key,a);}
 finish(id,hash=''){const t=this.open.get(id);if(!t)return null;this.open.delete(id);for(const[a,b]of SPANS){const ms=t.span(a,b);if(ms!==null)this.sample(`${a}→${b}`,ms);}this.recent.push(t);if(this.recent.length>this.keepTraces)this.recent.shift();this.count(t.failed?'failed':t.marks.has('committed')?'committed':'dropped');if(hash&&t.marks.has('committed')){hash=hash.toLowerCase();this.pending.set(hash,t);const seen=this.early.get(hash);if(seen!==undefined)this.confirm(hash,seen);}this.expire();return t;}
 confirm(hash,at=mono()){const key=String(hash||'').toLowerCase();if(!key)return;const t=this.pending.get(key);if(t){t.mark('stream_confirmation',at);const ms=t.span('http_started','stream_confirmation');if(ms!==null)this.sample('http_started→stream_confirmation',ms);this.pending.delete(key);this.early.delete(key);this.count('confirmed');}else{this.early.set(key,at);if(this.early.size>this.sampleCap)this.early.delete(this.early.keys().next().value);}}
 expire(now=mono()){for(const[h,t]of this.pending)if(now-t.startedAt>120000){this.pending.delete(h);this.count('confirmationTimeout');}for(const[h,at]of this.early)if(now-at>120000)this.early.delete(h);
  // 1.25.12: a trace whose intent ended without a submit (cleared, superseded,
  // reset by a wallet switch, an early return) must not live until the cap.
  // Map order = start order, so the scan stops at the first young trace.
  for(const[id,t]of this.open){if(now-t.startedAt<=OPEN_TRACE_TTL_MS)break;this.open.delete(id);this.count('staleOpen');}}
 /** Drop every open trace (engine reset): none of them can finish any more. */
 discardOpen(){const n=this.open.size;this.open.clear();if(n)this.count('dropped',n);return n;}
 count(name,n=1){this.counts[name]=(this.counts[name]||0)+n;}
 report(){this.expire();const spans={};for(const[a,b,k,label]of SPANS){const s=this.samples.get(`${a}→${b}`);if(s?.length)(spans[k]||={})[label]=quantiles(s);}return {spans,counts:{...this.counts},openTraces:this.open.size,pendingConfirmations:this.pending.size};}
 violations(){return this.counts.invalidSpans?[`invalid spans=${this.counts.invalidSpans}`]:[];}
 reset(){this.samples.clear();this.open.clear();this.recent=[];this.pending.clear();this.early.clear();for(const k of Object.keys(this.counts))this.counts[k]=0;}
}
module.exports={Metrics,Trace,STAGES,SPANS,quantiles,mono};
