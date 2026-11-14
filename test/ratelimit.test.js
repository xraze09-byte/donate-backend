// Server-side anti-abuse for public donors, against the REAL server.js. Run: node test/ratelimit.test.js
const {spawn}=require('child_process');const WebSocket=require('ws');const path=require('path');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
let pass=0,fail=0;const ok=(c,l)=>{c?pass++:fail++;console.log((c?'PASS ':'FAIL ')+l)};
const CH='rzc_ratelimit01',RT='relay_admin_tok';const kids=[];process.on('exit',()=>kids.forEach(k=>{try{k.kill()}catch(e){}}));
function boot(port,env){const p=spawn('node',['server.js'],{cwd:path.join(__dirname,'..'),env:{...process.env,PORT:String(port),...env}});kids.push(p);return p}
function conn(port,q,hdr){return new Promise(res=>{const w=new WebSocket(`ws://127.0.0.1:${port}/ws?${q}`,{headers:hdr||{}});w.msgs=[];w.code=null;
 w.on('message',d=>w.msgs.push(JSON.parse(d)));w.on('close',c=>{w.code=c});w.on('open',()=>setTimeout(()=>res(w),200));w.on('error',()=>res(w))})}
const don=(id)=>({t:'donation',d:{rec:{id,name:'n',amount:60}}});
const got=(w,t)=>w.msgs.filter(m=>m.t===t);
(async()=>{
 // A) per-IP window survives reconnects
 let s=boot(9411,{RELAY_TOKEN:RT,DONATE_PER_IP_MAX:3,DONATE_PER_IP_WINDOW_S:60,DONATE_MIN_GAP_MS:0});await sleep(700);
 const adm=await conn(9411,`ch=${CH}&tok=${RT}`);
 for(let i=0;i<3;i++){const d=await conn(9411,`ch=${CH}`);d.send(JSON.stringify(don('A'+i)));await sleep(120);d.close()}
 await sleep(150);ok(got(adm,'donation').length===3,'3 donations from one IP (3 separate reconnects) relayed');
 const d4=await conn(9411,`ch=${CH}`);d4.send(JSON.stringify(don('A9')));await sleep(200);
 ok(got(adm,'donation').length===3,'4th donation from same IP is dropped even on a fresh connection (bucket reset bypass closed)');
 // other IP (rightmost XFF) still OK
 const o=await conn(9411,`ch=${CH}`,{'x-forwarded-for':'1.1.1.1, 9.9.9.9'});o.send(JSON.stringify(don('B1')));await sleep(200);
 ok(got(adm,'donation').length===4,'a different client IP is unaffected');
 // spoofed leftmost XFF must not give a fresh bucket: 9.9.9.9 is the proxy-appended (rightmost) IP and already used 1 slot via B1;
 // exhaust it, then a request that only changes the leftmost entry must still be dropped
 const before=got(adm,'donation').length;
 for(let i=0;i<2;i++){const x=await conn(9411,`ch=${CH}`,{'x-forwarded-for':'1.2.3.'+i+', 9.9.9.9'});x.send(JSON.stringify(don('S'+i)));await sleep(150);}
 const spoof=await conn(9411,`ch=${CH}`,{'x-forwarded-for':'66.66.66.66, 9.9.9.9'});spoof.send(JSON.stringify(don('S9')));await sleep(200);
 ok(got(adm,'donation').length===before+2,'rightmost XFF is the identity: changing only the leftmost entry does not reset the limit');
 // admin token never throttled
 for(let i=0;i<10;i++){adm.send(JSON.stringify({t:'resolved',d:{id:'A'+(i%3),status:'rejected'}}));}
 await sleep(200);ok(adm.readyState===1,'admin connection is not throttled/closed');
 s.kill();await sleep(300);

 // B) min gap between donations
 s=boot(9412,{RELAY_TOKEN:RT,DONATE_MIN_GAP_MS:1500,DONATE_PER_IP_MAX:0});await sleep(700);
 const a2=await conn(9412,`ch=${CH}&tok=${RT}`);const dd=await conn(9412,`ch=${CH}`);
 dd.send(JSON.stringify(don('G1')));dd.send(JSON.stringify(don('G2')));await sleep(250);
 ok(got(a2,'donation').length===1,'two donations <1.5s apart: second dropped');
 await sleep(1500);dd.send(JSON.stringify(don('G3')));await sleep(250);
 ok(got(a2,'donation').length===2,'after the gap a donation is accepted again');
 s.kill();await sleep(300);

 // C) pending cap per room
 s=boot(9413,{RELAY_TOKEN:RT,DONATE_PENDING_CAP:3,DONATE_MIN_GAP_MS:0,DONATE_PER_IP_MAX:0});await sleep(700);
 const a3=await conn(9413,`ch=${CH}&tok=${RT}`);const p3=await conn(9413,`ch=${CH}`);
 for(let i=0;i<6;i++){p3.send(JSON.stringify(don('P'+i)));await sleep(120)}
 await sleep(200);ok(got(a3,'donation').length===3,'pending backlog capped at 3 per room (flood cannot grow memory)');
 a3.send(JSON.stringify({t:'resolved',d:{id:'P0',status:'rejected'}}));await sleep(200);
 p3.send(JSON.stringify(don('P9')));await sleep(200);
 ok(got(a3,'donation').length===4,'after one is resolved, a new donation fits again');
 s.kill();await sleep(300);

 // D) simultaneous public sockets per IP
 s=boot(9414,{PUBLIC_CONN_PER_IP:3,RELAY_TOKEN:RT});await sleep(700);
 const cs=[];for(let i=0;i<5;i++)cs.push(await conn(9414,`ch=${CH}`));
 ok(cs.filter(c=>c.readyState===1).length===3,'only 3 simultaneous donor sockets per IP (others closed with 4009)');
 ok(cs.filter(c=>c.code===4009).length===2,'excess sockets closed with code 4009');
 cs[0].close();await sleep(300);const again=await conn(9414,`ch=${CH}`);
 ok(again.readyState===1,'slot is freed when a socket closes');
 const admOk=await conn(9414,`ch=${CH}&tok=${RT}`);ok(admOk.readyState===1,'admin connection unaffected by donor socket cap');
 s.kill();await sleep(300);

 // E) defaults do not hurt a normal donor
 s=boot(9415,{RELAY_TOKEN:RT});await sleep(700);
 const a5=await conn(9415,`ch=${CH}&tok=${RT}`);const n5=await conn(9415,`ch=${CH}`);
 n5.send(JSON.stringify(don('N1')));await sleep(250);ok(got(a5,'donation').length===1,'default config: a normal donation is relayed');
 s.kill();
 console.log(`\n${pass} passed, ${fail} failed`);process.exit(fail?1:0);
})();
