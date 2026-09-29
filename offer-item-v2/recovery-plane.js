"use strict";
const TIMED_OUT=Symbol('recovery-timeout');
class RecoveryPlane {
  /**
   * PRIORITIES (1.25.0) — P0 is the realtime Stream/SEND path and never enters
   * this plane at all. Jobs here are:
   *   1  cold authority: first read of a row that is gated (cannot send yet)
   *   2  confirmed targeted recovery: gap, contradiction, trait scope, renew,
   *      reconnect re-check, ambiguous-POST reconcile
   *   3  non-urgent: own reconciliation, collection seeds with no gated row
   * Priority <=1 bypasses the background bucket (bounded by `workers` and by
   * the per-key ReadDispatcher downstream). 2 and 3 use the bucket AND yield
   * to realtime pressure (a SEND queued/in flight, a recent write 429): a P2
   * job waits at most PRESSURE_WAIT_P2_MS, a P3 job at most PRESSURE_WAIT_P3_MS,
   * so recovery is delayed by realtime, never starved by it.
   */
  constructor({workers=6,readsPerSecond=4,onLog,pressure}={}) {
    this.workers=Math.max(2,workers);this.readsPerSecond=Math.max(1,readsPerSecond);this.onLog=onLog||(()=>{});
    this.pressure=typeof pressure==='function'?pressure:()=>false;
    this.queue=[];this.running=0;this.background=0;this.generation=0;this.controller=new AbortController();
    this.tokens=this.readsPerSecond;this.lastRefill=RecoveryPlane.mono();this.stopped=false;this.timer=null;this.active=new Set();
    this.stats={queued:0,done:0,dropped:0,staleResults:0,errors:0,timedOut:0,maxDepth:0,yieldedToRealtime:0,byPriority:{1:0,2:0,3:0}};
  }
  static mono(){return Number(process.hrtime.bigint())/1e6;}
  static get JOB_DEADLINE_MS(){return 120000;}
  static get TIMED_OUT(){return TIMED_OUT;}
  static get PRESSURE_WAIT_P2_MS(){return 2000;}
  static get PRESSURE_WAIT_P3_MS(){return 15000;}
  refill(now=RecoveryPlane.mono()){this.tokens=Math.min(this.readsPerSecond,this.tokens+Math.max(0,now-this.lastRefill)/1000*this.readsPerSecond);this.lastRefill=now;}
  hasPending(pred){return this.queue.some(pred);}
  push(job){if(this.stopped)return false;this.queue.push({...job,priority:job.priority??2,generation:this.generation,queuedAt:RecoveryPlane.mono()});this.stats.queued++;this.stats.maxDepth=Math.max(this.stats.maxDepth,this.queue.length);this.pump();return true;}
  pump(){
    clearTimeout(this.timer);this.timer=null;if(this.stopped)return;
    this.queue.sort((a,b)=>a.priority-b.priority||a.queuedAt-b.queuedAt);
    while(this.running<this.workers&&this.queue.length){
      this.refill();
      const now=RecoveryPlane.mono();
      let busy=false;try{busy=this.pressure()===true;}catch{busy=false;}
      const i=this.queue.findIndex(j=>{
        if(j.priority<=1)return true;
        if(this.background>=this.workers-1||this.tokens<1)return false;
        if(!busy)return true;
        const aged=now-j.queuedAt>=(j.priority>=3?RecoveryPlane.PRESSURE_WAIT_P3_MS:RecoveryPlane.PRESSURE_WAIT_P2_MS);
        if(!aged)this.stats.yieldedToRealtime++;
        return aged;
      });
      if(i<0)break;
      const job=this.queue.splice(i,1)[0];if(job.generation!==this.generation){this.stats.dropped++;this.dropped(job);continue;}
      if(job.priority>1){this.tokens--;this.background++;}const pk=Math.min(3,Math.max(1,job.priority|0));this.stats.byPriority[pk]=(this.stats.byPriority[pk]||0)+1;this.running++;this.active.add(job);
      this.runOne(job).catch(e=>this.onLog(String(e.message))).finally(()=>{this.active.delete(job);this.running--;if(job.priority>1)this.background--;this.pump();});
    }
    if(this.queue.length){this.timer=setTimeout(()=>this.pump(),this.queue.some(j=>j.priority<=1)?50:250);this.timer.unref?.();}
  }
  async runOne(job){
    const generation=job.generation,ctrl=new AbortController();job.controller=ctrl;
    const parent=this.controller.signal,abort=()=>ctrl.abort();parent.addEventListener('abort',abort,{once:true});
    if(parent.aborted)ctrl.abort();
    let timer,abortHandler;
    try {
      const result=await Promise.race([
        Promise.resolve().then(()=>{if(ctrl.signal.aborted)throw Object.assign(new Error('Cancelled'),{name:'AbortError'});return job.run({signal:ctrl.signal,generation});}),
        new Promise(resolve=>{abortHandler=()=>{this.dropped(job);resolve(TIMED_OUT);};ctrl.signal.addEventListener('abort',abortHandler,{once:true});if(ctrl.signal.aborted)abortHandler();timer=setTimeout(()=>{this.stats.timedOut++;ctrl.abort();},RecoveryPlane.JOB_DEADLINE_MS);timer.unref?.();})
      ]);
      if(result===TIMED_OUT)return;
      // A transport can finish just as a reconnect invalidates its generation.
      // It still owns engine lifecycle state, so take the normal drop path.
      if(generation!==this.generation||ctrl.signal.aborted){this.stats.staleResults++;this.dropped(job);return;}
      this.stats.done++;job.onResult?.(result,generation);
    } catch(error){if(ctrl.signal.aborted){this.stats.dropped++;this.dropped(job);}else{this.stats.errors++;this.onLog(`[RECOVERY] ${job.kind}: ${error.message}`);}}
    finally{clearTimeout(timer);parent.removeEventListener('abort',abort);ctrl.signal.removeEventListener('abort',abortHandler);}
  }
  dropped(job){if(job.dropped)return;job.dropped=true;try{job.onDrop?.();}catch(error){this.onLog(error.message);}}
  invalidate(why='reset'){this.generation++;for(const j of this.queue)this.dropped(j);this.stats.dropped+=this.queue.length;this.queue=[];this.controller.abort();this.controller=new AbortController();clearTimeout(this.timer);this.timer=null;return this.generation;}
  stop(){this.stopped=true;this.invalidate('stop');}
  resume(){this.stopped=false;this.pump();}
  census(){return {...this.stats,queued:this.queue.length,running:this.running,generation:this.generation,workers:this.workers,readsPerSecond:this.readsPerSecond};}
}
// Giữ cả default export cũ và named export mới. Một số tool kiểm tra/plug-in
// nội bộ require trực tiếp constructor, trong khi engine dùng destructuring.
module.exports=RecoveryPlane;
module.exports.RecoveryPlane=RecoveryPlane;
