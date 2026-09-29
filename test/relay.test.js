// Integration test against the REAL server.js (not a mock).
// Run: node test/relay.test.js
const {spawn}=require('child_process');const WebSocket=require('ws');const path=require('path');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
let pass=0,fail=0;const ok=(c,l)=>{c?pass++:fail++;console.log((c?'PASS ':'FAIL ')+l)};
const CH='rzc_testchannel01',RT='relay_admin_tok',LT='listen_only_tok';
function boot(port,env){const p=spawn('node',['server.js'],{cwd:path.join(__dirname,'..'),env:{...process.env,PORT:String(port),...env}});
 kids.push(p);let log='';p.stdout.on('data',d=>log+=d);p.stderr.on('data',d=>log+=d);p.log=()=>log;return p}
function conn(port,q){return new Promise((res)=>{const w=new WebSocket(`ws://127.0.0.1:${port}/ws?${q}`);w.msgs=[];w.code=null;w.opened=false;let done=false;
 const fin=()=>{if(!done){done=true;res(w)}};
 w.on('message',d=>w.msgs.push(JSON.parse(d)));
 w.on('close',c=>{w.code=c;w.opened=false;fin()});                 // closed by server (e.g. 4001) => unusable
 w.on('open',()=>{setTimeout(()=>{if(w.code===null&&w.readyState===1){w.opened=true}fin()},250)});
 w.on('error',()=>{})})}
const send=(w,o)=>w.send(JSON.stringify(o));
const got=(w,t)=>w.msgs.filter(m=>m.t===t);
const kids=[];process.on('exit',()=>kids.forEach(k=>{try{k.kill()}catch(e){}}));
(async()=>{
 // ───────── A) legacy mode: no tokens set, must behave exactly as before ─────────
 let s=boot(9301,{});await sleep(700);
 let a=await conn(9301,`ch=${CH}`),b=await conn(9301,`ch=${CH}`);await sleep(150);
 send(a,{t:'donation',d:{rec:{id:'X1',name:'n',amount:60,q:{name:'Nick',uid:'123456789'}}}});await sleep(150);
 const d1=got(b,'donation')[0];
 ok(!!d1,'legacy: donation relayed to peer');
 ok(d1&&d1.d.rec.q&&d1.d.rec.q.uid==='123456789'&&d1.d.rec.q.name==='Nick','legacy: q{name,uid} survives sanitize (needed by queue bridge)');
 send(a,{t:'resolved',d:{id:'X1',status:'approved',amount:60}});await sleep(150);
 ok(got(b,'resolved').length===1,'legacy: resolved relayed');
 send(a,{t:'alert',d:{name:'n',amount:60,message:'m'}});await sleep(150);
 ok(got(b,'alert').length===1,'legacy: alert relayed');
 a.close();b.close();s.kill();await sleep(200);

 // ───────── B) RELAY_TOKEN + LISTEN_TOKEN set ─────────
 s=boot(9302,{RELAY_TOKEN:RT,LISTEN_TOKEN:LT});await sleep(700);
 const adm=await conn(9302,`ch=${CH}&tok=${RT}`);
 const lis=await conn(9302,`ch=${CH}&tok=${LT}`);
 const pub=await conn(9302,`ch=${CH}`);
 const bad=await conn(9302,`ch=${CH}&tok=WRONG`);
 await sleep(200);
 ok(lis.readyState===1,'listener token accepted (bridge can connect)');
 ok(adm.readyState===1,'admin token accepted');
 ok(pub.readyState===1,'no token: donor page still connects (public scope)');
 ok(bad.readyState!==1&&bad.code===4001,'wrong token rejected');
 // donor (public) submits donation; admin + listener both see it
 send(pub,{t:'donation',d:{rec:{id:'Y1',name:'donor',amount:60,q:{name:'Nick',uid:'123456789'}}}});await sleep(200);
 ok(got(adm,'donation').length===1,'public donation reaches admin');
 ok(got(lis,'donation').length===1,'public donation reaches listener');
 ok(got(lis,'donation')[0].d.rec.status==='pending','status forced to pending');
 // public cannot forge approval / alert
 send(pub,{t:'resolved',d:{id:'Y1',status:'approved'}});send(pub,{t:'alert',d:{name:'FAKE',amount:9999,message:'x'}});await sleep(200);
 ok(got(adm,'resolved').length===0&&got(lis,'resolved').length===0,'public cannot forge resolved');
 ok(got(adm,'alert').length===0&&got(lis,'alert').length===0,'public cannot forge alert');
 // listener is READ-ONLY: cannot send anything at all
 send(lis,{t:'alert',d:{name:'FAKE',amount:9999,message:'x'}});
 send(lis,{t:'resolved',d:{id:'Y1',status:'approved'}});
 send(lis,{t:'donation',d:{rec:{id:'Z9',name:'evil',amount:1}}});
 send(lis,{t:'cfg',d:{pp:'0000000000'}});await sleep(250);
 ok(got(adm,'alert').length===0,'listener cannot send alert');
 ok(got(adm,'resolved').length===0,'listener cannot send resolved');
 ok(got(adm,'donation').length===1,'listener cannot inject donation');
 ok(got(adm,'cfg').length===0&&got(pub,'cfg').length===0,'listener cannot change cfg');
 // real admin approves: listener must receive resolved (+ q for the bridge)
 send(adm,{t:'resolved',d:{id:'Y1',status:'approved',amount:60}});await sleep(200);
 const r=got(lis,'resolved')[0];
 ok(!!r&&r.d.id==='Y1'&&r.d.status==='approved','admin resolved reaches listener (bridge)');
 ok(r&&r.d.q&&r.d.q.uid==='123456789','resolved carries q from stored donation (bridge needs it)');
 ok(r&&typeof r._at==='number','resolved has _at (bridge replay guard)');
 send(adm,{t:'alert',d:{name:'donor',amount:60,message:'hi'}});await sleep(200);
 ok(got(pub,'alert').length===1,'admin alert reaches overlay');
 // listener must NOT receive slip images
 send(pub,{t:'donation',d:{rec:{id:'Y2',name:'d2',amount:100,slip:true},slip:'data:image/jpeg;base64,'+'A'.repeat(500)}});await sleep(200);
 const l2=got(lis,'donation').find(m=>m.d.rec.id==='Y2');
 ok(l2&&!l2.d.slip,'listener never receives slip image');
 ok(got(adm,'donation').find(m=>m.d.rec.id==='Y2').d.slip,'admin still receives slip image');
 // regression: a 2nd donor re-using a pending donation id must NOT overwrite the first one
 send(pub,{t:'donation',d:{rec:{id:'Z1',name:'Real',amount:500,q:{name:'RealFF',uid:'111111111'}}}});await sleep(150);
 const pub2=await conn(9302,`ch=${CH}`);
 send(pub2,{t:'donation',d:{rec:{id:'Z1',name:'Evil',amount:1,q:{name:'EvilFF',uid:'222222222'}}}});await sleep(150);
 ok(got(adm,'donation').filter(m=>m.d.rec.id==='Z1').length===1,'duplicate pending id is not relayed again');
 send(adm,{t:'resolved',d:{id:'Z1',status:'approved'}});await sleep(200);
 const rz=got(lis,'resolved').find(m=>m.d.id==='Z1');
 ok(rz&&rz.d.amount===500&&rz.d.q&&rz.d.q.uid==='111111111','id collision: approval keeps the ORIGINAL donor amount + UID');
 pub2.close();
 ok(!/uncaught/i.test(s.log()),'no uncaught exceptions');
 [adm,lis,pub].forEach(w=>w.close());s.kill();await sleep(200);

 // ───────── C) LISTEN_TOKEN without RELAY_TOKEN: must not weaken anything ─────────
 s=boot(9303,{LISTEN_TOKEN:LT});await sleep(700);
 const l3=await conn(9303,`ch=${CH}&tok=${LT}`),p3=await conn(9303,`ch=${CH}`);await sleep(150);
 send(l3,{t:'alert',d:{name:'FAKE',amount:1,message:'x'}});await sleep(150);
 ok(got(p3,'alert').length===0,'listener read-only even when RELAY_TOKEN unset');
 [l3,p3].forEach(w=>w.close());s.kill();
 console.log(`\n${pass} passed, ${fail} failed`);process.exit(fail?1:0);
})();
