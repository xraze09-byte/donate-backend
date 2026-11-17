// Opt-in hardening (security.js) against the REAL server.js. Run: node test/security.test.js
const {spawn}=require('child_process');const WebSocket=require('ws');const path=require('path');const crypto=require('crypto');
const SEC=require('../security');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
let pass=0,fail=0;const ok=(c,l)=>{c?pass++:fail++;console.log((c?'PASS ':'FAIL ')+l)};
const CH='rzc_security01',RT='relay_admin_tok';const kids=[];process.on('exit',()=>kids.forEach(k=>{try{k.kill()}catch(e){}}));
function boot(port,env){const p=spawn('node',['server.js'],{cwd:path.join(__dirname,'..'),env:{...process.env,PORT:String(port),DONATE_MIN_GAP_MS:'0',DONATE_PER_IP_MAX:'0',RELAY_TOKEN:RT,CSRF_SECRET:'test-secret-'+port,...env}});kids.push(p);return p}
function conn(port,q,hdr){return new Promise(res=>{const w=new WebSocket(`ws://127.0.0.1:${port}/ws?${q}`,{headers:hdr||{}});w.msgs=[];w.code=null;
 w.on('message',d=>w.msgs.push(JSON.parse(d)));w.on('close',c=>{w.code=c});w.on('open',()=>setTimeout(()=>res(w),200));w.on('error',()=>res(w))})}
const got=(w,t)=>w.msgs.filter(m=>m.t===t);
const get=async(port,p,h)=>{const r=await fetch(`http://127.0.0.1:${port}${p}`,{headers:h||{}});let j=null;try{j=await r.json()}catch(e){}return {status:r.status,json:j,headers:r.headers}};
// a donation the way the donate page builds it
const rec=(id,over)=>({id,name:'Tester',message:'hello',amount:60,ts:Date.now(),...over});
// solve the same proof-of-work the browser must solve (bound to rec + slip hash)
function solve(bound,bits){for(let n=0;n<5e6;n++){const h=crypto.createHash('sha256').update(SEC.powPayload(bound)+'|'+n).digest();if(SEC.leadingZeroBits(h)>=bits)return {n,bits}}throw new Error('no pow')}
const SIGNED={sig:'A'.repeat(86),nonce:'N'.repeat(16),ts:Date.now()};

(async()=>{
 // ---------- unit: pure functions ----------
 ok(SEC.canon({b:2,a:1,sig:'x',tok:'y',csrf:'z',pow:{n:1},u:undefined})==='{"a":1,"b":2}','canon sorts keys, drops sig/tok/csrf/pow/undefined');
 ok(SEC.canon({a:[{z:1,y:2}]})==='{"a":[{"y":2,"z":1}]}','canon recurses into arrays and objects');
 const t1=SEC.issueCsrf('1.2.3.4');
 ok(SEC.checkCsrf(t1,'1.2.3.4'),'csrf token verifies for the IP it was issued to');
 ok(!SEC.checkCsrf(t1,'5.6.7.8'),'csrf token is bound to the IP (other IP rejected)');
 ok(!SEC.checkCsrf(t1.slice(0,-3)+'AAA','1.2.3.4'),'tampered csrf token rejected');
 ok(!SEC.checkCsrf('','1.2.3.4')&&!SEC.checkCsrf(null,'1.2.3.4')&&!SEC.checkCsrf('a.b','1.2.3.4'),'empty / null / malformed csrf rejected');
 const bound={id:'POW000001',amount:60};const pw=solve(bound,10);
 ok(SEC.checkPow(bound,pw,10),'a solved proof-of-work verifies');
 ok(!SEC.checkPow({...bound,amount:61},pw,10),'proof-of-work is bound to the content (changing the amount invalidates it)');
 ok(!SEC.checkPow(bound,{n:pw.n,bits:4},10),'a pow that claims fewer bits than required is rejected');
 ok(!SEC.checkPow(bound,{n:-1,bits:10},10)&&!SEC.checkPow(bound,null,10),'negative nonce / missing pow rejected');
 ok(SEC.strictDonation(rec('OKID0001'),{min:1,max:1e6})===null,'a normal donation passes strict validation');
 ok(SEC.strictDonation(rec('OKID0002',{amount:0.001}),{})==='decimals','3-decimal amount rejected');
 ok(SEC.strictDonation(rec('OKID0003',{message:'visit http://evil.example'}),{})==='link','link in message rejected');
 ok(SEC.strictDonation(rec('OKID0004',{name:'a\u202Eb'}),{})==='control-chars','bidi-override character in name rejected');
 ok(SEC.strictDonation(rec('OKID0005',{ts:Date.now()-3600e3}),{})==='ts','stale timestamp (1h old) rejected');
 ok(SEC.strictDonation(rec('x'),{})==='id','too-short id rejected');
 ok(SEC.strictDonation(rec('OKID0006',{amount:5}),{min:10})==='min','below configured minimum rejected');

 // ---------- A) everything OFF (the default): no behaviour change ----------
 let s=boot(9501,{});await sleep(700);
 let adm=await conn(9501,`ch=${CH}&tok=${RT}`);let don=await conn(9501,`ch=${CH}`);
 don.send(JSON.stringify({t:'donation',d:{rec:rec('OFF000001')}}));await sleep(250);
 ok(got(adm,'donation').length===1,'DEFAULT: anonymous donor with NO csrf/pow still gets through (live site keeps working)');
 adm.send(JSON.stringify({t:'alert',d:{name:'x',amount:5,message:'m'}}));await sleep(200);
 ok(got(don,'alert').length===1,'DEFAULT: unsigned alert from admin is still relayed');
 const h=await get(9501,'/health');
 ok(h.json&&h.json.sec&&h.json.sec.csrf==='off'&&h.json.sec.signed==='off'&&h.json.sec.pow==='off','/health reports every feature as off');
 const c=await get(9501,'/csrf',{origin:'https://x.example'});
 ok(c.status===200&&typeof c.json.token==='string'&&c.json.token.split('.').length===3,'GET /csrf returns a token even while CSRF_MODE is off');
 ok(c.headers.get('access-control-allow-origin')==='https://x.example'||c.headers.get('access-control-allow-origin')==='*','GET /csrf sends a CORS header so the donate page can read it');
 s.kill();await sleep(300);

 // ---------- B) CSRF enforced ----------
 s=boot(9502,{CSRF_MODE:'1'});await sleep(700);
 adm=await conn(9502,`ch=${CH}&tok=${RT}`);don=await conn(9502,`ch=${CH}`);
 don.send(JSON.stringify({t:'donation',d:{rec:rec('CSRF00001')}}));await sleep(250);
 ok(got(adm,'donation').length===0,'CSRF on: donation with no token is dropped');
 don.send(JSON.stringify({t:'donation',d:{rec:rec('CSRF00002'),csrf:'1.2.3'}}));await sleep(250);
 ok(got(adm,'donation').length===0,'CSRF on: donation with a garbage token is dropped');
 const tk=(await get(9502,'/csrf')).json.token;
 don.send(JSON.stringify({t:'donation',d:{rec:rec('CSRF00003'),csrf:tk}}));await sleep(250);
 ok(got(adm,'donation').length===1,'CSRF on: donation with a token from /csrf is relayed');
 ok(!JSON.stringify(got(adm,'donation')[0]).includes(tk),'the csrf token is never relayed to the admin');
 adm.send(JSON.stringify({t:'resolved',d:{id:'CSRF00003',status:'rejected'}}));await sleep(150);
 ok(adm.readyState===1,'CSRF on: admin (token holder) is never asked for a csrf token');
 s.kill();await sleep(300);

 // ---------- C) CSRF in log-only mode ----------
 s=boot(9503,{CSRF_MODE:'log'});await sleep(700);
 adm=await conn(9503,`ch=${CH}&tok=${RT}`);don=await conn(9503,`ch=${CH}`);
 don.send(JSON.stringify({t:'donation',d:{rec:rec('LOG000001')}}));await sleep(250);
 ok(got(adm,'donation').length===1,'CSRF log mode: token-less donation is STILL relayed');
 const hl=await get(9503,'/health');
 ok(hl.json.sec.csrf==='log'&&hl.json.sec.hits.csrf>=1,'CSRF log mode: the would-be rejection is counted on /health');
 s.kill();await sleep(300);

 // ---------- D) proof-of-work ----------
 s=boot(9504,{POW_BITS:'10'});await sleep(700);
 adm=await conn(9504,`ch=${CH}&tok=${RT}`);don=await conn(9504,`ch=${CH}`);
 don.send(JSON.stringify({t:'donation',d:{rec:rec('POW000001')}}));await sleep(250);
 ok(got(adm,'donation').length===0,'PoW on: donation with no proof is dropped');
 const r2=rec('POW000002');
 don.send(JSON.stringify({t:'donation',d:{rec:r2,pow:solve(r2,10)}}));await sleep(250);
 ok(got(adm,'donation').length===1,'PoW on: donation with a valid proof is relayed');
 const r3=rec('POW000003');const p3=solve(r3,10);
 don.send(JSON.stringify({t:'donation',d:{rec:{...r3,amount:9999},pow:p3}}));await sleep(250);
 ok(got(adm,'donation').length===1,'PoW on: proof reused on an edited amount is dropped');
 const r4=rec('POW000004');const slip='data:image/png;base64,'+Buffer.from('slipbytes').toString('base64');
 don.send(JSON.stringify({t:'donation',d:{rec:r4,slip,pow:solve({...r4,slip},10)}}));await sleep(250);
 ok(got(adm,'donation').length===2,'PoW on: proof that covers a slip image verifies');
 s.kill();await sleep(300);

 // ---------- E) strict validation ----------
 s=boot(9505,{STRICT_DONATION:'1'});await sleep(700);
 adm=await conn(9505,`ch=${CH}&tok=${RT}`);don=await conn(9505,`ch=${CH}`);
 don.send(JSON.stringify({t:'donation',d:{rec:rec('STR000001',{message:'see www.evil.example'})}}));await sleep(250);
 ok(got(adm,'donation').length===0,'strict on: link in message is dropped');
 don.send(JSON.stringify({t:'donation',d:{rec:rec('STR000002')}}));await sleep(250);
 ok(got(adm,'donation').length===1,'strict on: a normal donation is relayed');
 s.kill();await sleep(300);

 // ---------- F) signed-shape for admin messages ----------
 s=boot(9506,{REQUIRE_SIGNED:'1'});await sleep(700);
 adm=await conn(9506,`ch=${CH}&tok=${RT}`);don=await conn(9506,`ch=${CH}`);
 adm.send(JSON.stringify({t:'alert',d:{name:'x',amount:5,message:'unsigned'}}));await sleep(250);
 ok(got(don,'alert').length===0,'signed on: unsigned alert is dropped');
 adm.send(JSON.stringify({t:'alert',d:{name:'x',amount:5,message:'signed',...SIGNED}}));await sleep(250);
 ok(got(don,'alert').length===1,'signed on: signed-shaped alert is relayed');
 ok(got(don,'alert')[0].d.sig===SIGNED.sig&&got(don,'alert')[0].d.nonce===SIGNED.nonce&&got(don,'alert')[0].d.ts===SIGNED.ts,'signed on: sig/nonce/ts reach the donor byte-for-byte unchanged');
 adm.send(JSON.stringify({t:'cfg',d:{pp:'0812345678',min:10}}));await sleep(250);
 ok(got(don,'cfg').length===0,'signed on: unsigned cfg is dropped (cannot swap the PromptPay number)');
 adm.send(JSON.stringify({t:'resolved',d:{id:'RES000001',status:'approved'}}));await sleep(250);
 ok(got(don,'resolved').length===0,'signed on: unsigned resolved is dropped');
 don.send(JSON.stringify({t:'donation',d:{rec:rec('SIG000001')}}));await sleep(250);
 ok(got(adm,'donation').length===1,'signed on: donations (not admin messages) are unaffected');
 s.kill();await sleep(300);

 // ---------- G) everything on together ----------
 s=boot(9507,{CSRF_MODE:'1',POW_BITS:'8',STRICT_DONATION:'1',REQUIRE_SIGNED:'1'});await sleep(700);
 adm=await conn(9507,`ch=${CH}&tok=${RT}`);don=await conn(9507,`ch=${CH}`);
 const tk2=(await get(9507,'/csrf')).json.token;const r5=rec('ALL000001');
 don.send(JSON.stringify({t:'donation',d:{rec:r5,csrf:tk2,pow:solve(r5,8)}}));await sleep(300);
 ok(got(adm,'donation').length===1,'ALL ON: a correct donation (csrf + pow + clean fields) goes through');
 const hh=await get(9507,'/health');
 ok(hh.json.sec.csrf==='on'&&hh.json.sec.pow==='8:on'&&hh.json.sec.strict==='on'&&hh.json.sec.signed==='on','ALL ON: /health reflects the configuration');
 s.kill();await sleep(300);

 console.log(`\n${pass} passed, ${fail} failed`);process.exit(fail?1:0);
})();
