// Relay passes sig/nonce/ts through on alert/cfg/resolved (admin only), drops malformed ones, and leaves unsigned output unchanged.
// Run: node test/sigpass.test.js   (spawns the real server.js)
const {spawn}=require('child_process');const WebSocket=require('ws');const path=require('path');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
let pass=0,fail=0;const ok=(c,l)=>{c?pass++:fail++;console.log((c?'PASS ':'FAIL ')+l)};
const CH='rzc_sigtest0001',RT='relay_admin_tok';
const kids=[];process.on('exit',()=>kids.forEach(k=>{try{k.kill()}catch(e){}}));
function boot(port,env){const p=spawn('node',['server.js'],{cwd:path.join(__dirname,'..'),env:{...process.env,PORT:String(port),DONATE_MIN_GAP_MS:'0',...env}});kids.push(p);p.stdout.on('data',()=>{});p.stderr.on('data',()=>{});return p}
function conn(port,q){return new Promise(res=>{const w=new WebSocket(`ws://127.0.0.1:${port}/ws?${q}`);w.msgs=[];w.code=null;let done=false;const fin=()=>{if(!done){done=true;res(w)}};
 w.on('message',d=>w.msgs.push(JSON.parse(d)));w.on('close',c=>{w.code=c;fin()});w.on('open',()=>setTimeout(fin,250));w.on('error',()=>{})})}
const send=(w,o)=>w.send(JSON.stringify(o));const got=(w,t)=>w.msgs.filter(m=>m.t===t);
const SIG='abcDEF0123456789_-abcDEF0123456789_-abcDEF01',NONCE='nonce_ABC-123',TS=1760000000000;
(async()=>{
 boot(9311,{RELAY_TOKEN:RT});await sleep(800);
 const adm=await conn(9311,`ch=${CH}&tok=${RT}`),pub=await conn(9311,`ch=${CH}`),peer=await conn(9311,`ch=${CH}&tok=${RT}`);await sleep(150);
 // signed alert
 send(adm,{t:'alert',d:{name:'Ann',amount:50,message:'hi',sig:SIG,nonce:NONCE,ts:TS,evil:'x'}});await sleep(200);
 let a=got(peer,'alert').at(-1);
 ok(a&&a.d.sig===SIG&&a.d.nonce===NONCE&&a.d.ts===TS,'signed alert: sig/nonce/ts forwarded unchanged');
 ok(a&&!('evil' in a.d)&&a.d.name==='Ann'&&a.d.amount===50&&a.d.message==='hi','signed alert: other fields still whitelisted');
 // unsigned alert: identical to the old shape
 send(adm,{t:'alert',d:{name:'Bob',amount:20,message:'yo'}});await sleep(200);
 a=got(peer,'alert').at(-1);
 ok(a&&JSON.stringify(Object.keys(a.d).sort())===JSON.stringify(['amount','message','name']),'unsigned alert: output keys unchanged (name, amount, message)');
 // malformed / half-signed
 send(adm,{t:'alert',d:{name:'C',amount:5,message:'',sig:'<script>',nonce:NONCE,ts:TS}});await sleep(200);
 a=got(peer,'alert').at(-1);ok(a&&!('sig' in a.d)&&!('nonce' in a.d)&&!('ts' in a.d),'bad sig characters: all signature fields dropped');
 send(adm,{t:'alert',d:{name:'D',amount:5,message:'',sig:SIG}});await sleep(200);
 a=got(peer,'alert').at(-1);ok(a&&!('sig' in a.d),'sig without nonce/ts: dropped (half-signed never forwarded)');
 send(adm,{t:'alert',d:{name:'E',amount:5,message:'',sig:SIG,nonce:NONCE,ts:'x'.repeat(5)}});await sleep(200);
 a=got(peer,'alert').at(-1);ok(a&&!('sig' in a.d),'non-numeric ts: dropped');
 send(adm,{t:'alert',d:{name:'F',amount:5,message:'',sig:'a'.repeat(600),nonce:NONCE,ts:TS}});await sleep(200);
 a=got(peer,'alert').at(-1);ok(a&&!('sig' in a.d),'oversized sig: dropped');
 // resolved + cfg
 send(adm,{t:'resolved',d:{id:'D1',status:'approved',sig:SIG,nonce:NONCE,ts:TS}});await sleep(200);
 let r=got(peer,'resolved').at(-1);ok(r&&r.d.id==='D1'&&r.d.sig===SIG&&r.d.nonce===NONCE&&r.d.ts===TS,'resolved: signature forwarded');
 send(adm,{t:'resolved',d:{id:'D2',status:'rejected'}});await sleep(200);
 r=got(peer,'resolved').at(-1);ok(r&&JSON.stringify(Object.keys(r.d).sort())===JSON.stringify(['id','status']),'unsigned resolved: output keys unchanged');
 send(adm,{t:'cfg',d:{pp:'0812345678',payee:'P',goal:1000,min:10,reqSlip:true,sig:SIG,nonce:NONCE,ts:TS}});await sleep(200);
 let c=got(peer,'cfg').at(-1);ok(c&&c.d.sig===SIG&&c.d.goal===1000&&c.d.pp==='0812345678','cfg: signature forwarded, fields intact');
 send(adm,{t:'cfg',d:{pp:'1',payee:'P',goal:5,min:1}});await sleep(200);
 c=got(peer,'cfg').at(-1);ok(c&&!('sig' in c.d),'unsigned cfg: no signature fields added');
 // a public (donor) client still cannot send admin-only types, signed or not
 const before=got(peer,'alert').length;
 send(pub,{t:'alert',d:{name:'Fake',amount:9999,message:'x',sig:SIG,nonce:NONCE,ts:TS}});await sleep(250);
 ok(got(peer,'alert').length===before,'public client cannot inject alerts (even with sig fields)');
 console.log('\n'+pass+' passed, '+fail+' failed');process.exit(fail?1:0);
})();
