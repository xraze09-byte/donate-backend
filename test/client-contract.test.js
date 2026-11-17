// The browser module (client/donate-security.js) must produce proofs the real server accepts.
// Run: node test/client-contract.test.js
const {spawn}=require('child_process');const WebSocket=require('ws');const path=require('path');const fs=require('fs');const vm=require('vm');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
let pass=0,fail=0;const ok=(c,l)=>{c?pass++:fail++;console.log((c?'PASS ':'FAIL ')+l)};
const CH='rzc_contract01',RT='relay_admin_tok',PORT=9521,API=`http://127.0.0.1:${PORT}`;
const kids=[];process.on('exit',()=>kids.forEach(k=>{try{k.kill()}catch(e){}}));
// load the real client file in a sandbox that has Node's WebCrypto + fetch, like a browser
const ctx={globalThis:null,TextEncoder,btoa,AbortController,Promise,Math,JSON,Object,Array,Uint8Array,Number,Date,setTimeout,clearTimeout,fetch,crypto:require('crypto').webcrypto,console};
ctx.globalThis=ctx;ctx.window=undefined;vm.createContext(ctx);
vm.runInContext(fs.readFileSync(path.join(__dirname,'..','client','donate-security.js'),'utf8'),ctx);
const DS=ctx.DonateSecurity;
const conn=(q)=>new Promise(res=>{const w=new WebSocket(`ws://127.0.0.1:${PORT}/ws?${q}`);w.msgs=[];w.on('message',d=>w.msgs.push(JSON.parse(d)));w.on('open',()=>setTimeout(()=>res(w),200));w.on('error',()=>res(w))});
(async()=>{
 const p=spawn('node',['server.js'],{cwd:path.join(__dirname,'..'),env:{...process.env,PORT:String(PORT),RELAY_TOKEN:RT,CSRF_MODE:'1',POW_BITS:'12',STRICT_DONATION:'1',DONATE_MIN_GAP_MS:'0',DONATE_PER_IP_MAX:'0',CSRF_SECRET:'contract-secret'}});kids.push(p);await sleep(800);
 const adm=await conn(`ch=${CH}&tok=${RT}`),don=await conn(`ch=${CH}`);
 const bits=await DS.difficulty(API);ok(bits===12,'client reads the advertised difficulty from /health (got '+bits+')');
 const mk=(id,o)=>({id,name:'สมชาย ใจดี',message:'ขอบคุณที่ไลฟ์ครับ 🎉',amount:99.5,ts:Date.now(),status:'pending',slip:true,sha:'',ref:'',...o});
 // 1) thai text + emoji + decimal amount, no slip
 let rec=mk('CTR000001');let sec=await DS.prepare(API,rec,undefined,bits);
 ok(!!sec.csrf&&!!sec.pow,'client produced a csrf token and a proof-of-work');
 don.send(JSON.stringify({t:'donation',d:{rec,...sec}}));await sleep(300);
 ok(adm.msgs.filter(m=>m.t==='donation').length===1,'server accepts the client\'s proof for Thai text + emoji + 99.5 amount (canonical JSON agrees byte-for-byte)');
 // 2) with a slip image
 rec=mk('CTR000002');const slip='data:image/png;base64,'+Buffer.alloc(40000,7).toString('base64');sec=await DS.prepare(API,rec,slip,bits);
 don.send(JSON.stringify({t:'donation',d:{rec,slip,...sec}}));await sleep(400);
 ok(adm.msgs.filter(m=>m.t==='donation').length===2,'server accepts the client\'s proof when a slip image is attached');
 // 3) a proof made for a different record is rejected
 const a=mk('CTR000003'),b=mk('CTR000004',{amount:5000});const sa=await DS.prepare(API,a,undefined,bits);
 don.send(JSON.stringify({t:'donation',d:{rec:b,...sa}}));await sleep(300);
 ok(adm.msgs.filter(m=>m.t==='donation').length===2,'a proof/token prepared for donation A cannot be used to send donation B');
 // 4) graceful when server is unreachable: page must not crash
 const dead=await DS.prepare('http://127.0.0.1:1',mk('CTR000005'),undefined,0);
 ok(dead&&typeof dead==='object'&&!dead.csrf&&!dead.pow,'unreachable server: prepare() resolves to {} instead of throwing (donate button keeps working)');
 p.kill();await sleep(200);
 console.log(`\n${pass} passed, ${fail} failed`);process.exit(fail?1:0);
})();
