// mangaGrabber.js — Mangalib + Mangabuff downloader with CDP cookies + Node HTTP fetch
// Run:
//   node mangaGrabber.js --linksJson="C:\\path\\links.json" --channelFolder="C:\\YT\\MyChannel" --debug --rpm=10
//
// JSON format:
// { "mangaLinks": ["https://mangabuff.ru/manga/<slug>", "https://mangalib.org/ru/manga/..."], "used": [] }

const CDP   = require('chrome-remote-interface');
const fs    = require('fs');
const path  = require('path');
const http  = require('http');
const https = require('https');

const CDP_PORT    = Number(process.env.CDP_PORT || 9333);
const CDP_HOST    = process.env.CDP_HOST || '127.0.0.1';
const NAV_TIMEOUT = 20000;
const POLL_MS     = 200;

let DEBUG      = false;
let STEP_DELAY = 0;

// -------- rate limiter --------
class RateLimiter {
  constructor({rpm=10, minIntervalMs=null}={}) {
    this.minIntervalMs = Number(minIntervalMs ?? Math.max(1, Math.floor(60000/Math.max(1,rpm))));
    this._last = 0;
  }
  async hit(label='') {
    const now = Date.now();
    const wait = this._last ? this.minIntervalMs - (now - this._last) : 0;
    if (wait > 0) {
      if (DEBUG) console.log(`[debug] ⏳ rate-limit ${label} wait ${wait}ms`);
      await new Promise(r=>setTimeout(r, wait));
    }
    this._last = Date.now();
  }
}

const argsRaw = process.argv.slice(2);
function parseArgs(argv){
  const out={_pos:[]};
  for (let i=0;i<argv.length;i++){
    const t=argv[i];
    if (t.startsWith('--')){
      const [k,vRaw]=t.slice(2).split('=');
      const v = vRaw!=null ? vRaw.replace(/^["']|["']$/g,'')
            : (i+1<argv.length && !argv[i+1].startsWith('--') ? argv[++i].replace(/^["']|["']$/g,'') : 'true');
      out[k]=v;
    } else out._pos.push(t.replace(/^["']|["']$/g,''));
  }
  return out;
}
const args          = parseArgs(argsRaw);
const linksJsonPath = args.linksJson || args.l || args._pos[0];
const channelFolder = args.channelFolder || args.c || args._pos[1];
DEBUG               = String(args.debug||'false').toLowerCase()==='true';
STEP_DELAY          = Number.isFinite(Number(args.stepDelay)) ? Number(args.stepDelay) : 0;
const limitLinks    = args.limit ? Number(args.limit) : null;
const preferredLang = (args.lang || '').trim().toLowerCase();

const rpm           = args.rpm ? Number(args.rpm) : 10;
const minIntervalMs = args.minIntervalMs ? Number(args.minIntervalMs) : null;
const limiter       = new RateLimiter({rpm, minIntervalMs});

// smart load settings
const SMART_MAX_MS        = Number(args.smartMaxMs || 7000);
const SMART_STABLE_TICKS  = Number(args.smartStableTicks || 2);
const SMART_MIN_LOOPS     = Number(args.smartMinLoops || 1);
const SMART_SCROLL_STEPS  = Number(args.smartScrollSteps || 8);
const SMART_WINDOW_STEPS  = Number(args.smartWindowSteps || 5);
const SMART_STEP_MS       = Number(args.smartStepMs || 110);

const NAV_EXTRA_MS        = Number(args.navExtraMs || 350);
const NAV_TICKS           = Number(args.navTicks || 2);

const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));
const log   = (...a)=>console.log(...a);
const dbg   = (...a)=>{ if (DEBUG) console.log('[debug]', ...a); };

// -------- fs utils --------
function ensureDir(p){ fs.mkdirSync(p,{recursive:true}); }
function writeJSON(file,obj){ ensureDir(path.dirname(file)); fs.writeFileSync(file, JSON.stringify(obj,null,2), 'utf-8'); }
function sanitizeName(s){
  s=(s||'manga').toString().replace(/[<>:"/\\|?*\x00-\x1F]/g,' ').replace(/\s{2,}/g,' ').trim().replace(/[\. ]+$/,'');
  return s || 'manga';
}
function uniqStrings(list){
  const out=[]; const seen=new Set();
  for (const raw of list||[]){
    const s=(raw||'').toString().trim();
    if (!s) continue;
    const key=s.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key); out.push(s);
  }
  return out;
}
function toUnifiedMeta({names=[],descriptions=[],keywords=[],mainUrl=''}) {
  return {
    names: uniqStrings(names),
    descriptions: uniqStrings(descriptions),
    keywords: uniqStrings(keywords),
    mainUrl: mainUrl || ''
  };
}
function isHttpUrl(s){ return /^https?:\/\//i.test(s||''); }
const DEFAULT_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
function httpGetText(url, extraHeaders={}){
  const lib = url.startsWith('https') ? https : http;
  return new Promise((resolve,reject)=>{
    lib.get(url,{headers:{'User-Agent':DEFAULT_UA, ...extraHeaders}},res=>{
      if (res.statusCode>=300 && res.statusCode<400 && res.headers.location){
        return resolve(httpGetText(new URL(res.headers.location,url).href, extraHeaders));
      }
      if (res.statusCode!==200) return reject(new Error('HTTP '+res.statusCode));
      const chunks=[]; res.on('data',d=>chunks.push(d)); res.on('end',()=>resolve(Buffer.concat(chunks).toString('utf-8')));
    }).on('error',reject);
  });
}
async function httpGetJson(url){
  const txt = await httpGetText(url);
  return JSON.parse(txt);
}
async function loadLinksState(src){
  const txt = isHttpUrl(src) ? await httpGetText(src) : fs.readFileSync(src,'utf-8');
  const j   = JSON.parse(txt);
  return { jpath:src, state:{ mangaLinks:Array.isArray(j.mangaLinks)?j.mangaLinks.slice():[], used:Array.isArray(j.used)?j.used.slice():[], _raw:j } };
}
function saveLinksStateLocal(jpath,state){
  if (isHttpUrl(jpath)) return;
  const obj = state._raw || {};
  obj.mangaLinks = state.mangaLinks;
  obj.used       = state.used;
  writeJSON(jpath, obj);
}

// -------- CDP --------
async function openTab(url='about:blank'){
  const browser=await CDP({host:CDP_HOST,port:CDP_PORT});
  const {Target}=browser;
  await Target.setDiscoverTargets({discover:true});
  const {targetId}=await Target.createTarget({url});
  const tab=await CDP({host:CDP_HOST,port:CDP_PORT,target:targetId});
  const {Page,DOM,Runtime,Network}=tab;
  await Promise.all([Page.enable(),DOM.enable(),Runtime.enable(),Network.enable()]);
  await Network.setUserAgentOverride({userAgent:
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'});
  await Page.bringToFront();
  if (DEBUG){
    tab.Runtime.consoleAPICalled(({type,args})=>{
      try{ const parts=args.map(a=>a.value ?? a.description ?? '').filter(Boolean);
        console.log(`[page:${type}]`,...parts);}catch{}
    });
  }
  return {browser,tab};
}

async function goto(Page, Runtime, url, timeout = NAV_TIMEOUT){
  await limiter.hit('goto');
  dbg('▶ navigate:', url);
  await Page.navigate({url});
  const t0 = Date.now();
  while (Date.now() - t0 < timeout){
    const {result} = await Runtime.evaluate({expression:'document.readyState', returnByValue:true});
    if (result?.value === 'complete' || result?.value === 'interactive') break;
    await sleep(POLL_MS);
  }
  await sleep(NAV_EXTRA_MS);
  for (let i=0;i<NAV_TICKS;i++){
    await Runtime.evaluate({expression:'0', awaitPromise:true});
    await sleep(90);
  }
}

async function waitForSelector(Runtime,sel,timeout=NAV_TIMEOUT,poll=POLL_MS){
  const t0=Date.now();
  while (Date.now()-t0<timeout){
    const {result}=await Runtime.evaluate({expression:`!!document.querySelector(${JSON.stringify(sel)})`,returnByValue:true,awaitPromise:true});
    if (result?.value) return true;
    await sleep(poll);
  }
  return false;
}

async function evalInPage(Runtime,expr,returnByValue=true){
  const { result, exceptionDetails } = await Runtime.evaluate({ expression: expr, returnByValue, awaitPromise:true });
  if (exceptionDetails){
    const msg = exceptionDetails.text ||
      (exceptionDetails.exception && (exceptionDetails.exception.description || exceptionDetails.exception.value)) ||
      'Eval error';
    throw new Error(msg);
  }
  return result?.value;
}

// -------- site helpers --------
const MANGALIB_HOSTS = ['mangalib.org','mangalib.me','mixlib.me'];
const MANGADEX_HOSTS = ['mangadex.org','api.mangadex.org'];
const MANGABUFF_HOSTS= ['mangabuff.ru'];
const WEBTOONS_HOSTS = ['webtoons.com'];
const normHost = h => String(h||'').trim().toLowerCase().replace(/^www\./,'').replace(/\.+$/,'');
const dexLangOrder = (()=>{
  const list = [];
  if (preferredLang) list.push(preferredLang);
  list.push('en');
  list.push('');
  return Array.from(new Set(list));
})();
function looksLikeWebtoonsImg(url){
  return /webtoon-phinf\.pstatic\.net/i.test(url||'') && /\.(jpe?g|png|webp)(\?|$)/i.test(url||'');
}

function pickLangText(obj, langs=dexLangOrder){
  if (!obj || typeof obj !== 'object') return '';
  for (const l of langs){
    if (!l) continue;
    if (Object.prototype.hasOwnProperty.call(obj,l) && obj[l]) return obj[l];
  }
  const firstKey = Object.keys(obj)[0];
  return firstKey ? obj[firstKey] : '';
}
function pickAltTitle(altTitles, langs=dexLangOrder){
  if (!Array.isArray(altTitles)) return '';
  for (const l of langs){
    if (!l) continue;
    const found = altTitles.find(t => t && typeof t === 'object' && Object.prototype.hasOwnProperty.call(t,l) && t[l]);
    if (found) return found[l];
  }
  if (!altTitles.length) return '';
  const first = altTitles.find(t => t && typeof t === 'object');
  if (!first) return '';
  const fk = Object.keys(first)[0];
  return fk ? first[fk] : '';
}
function langListForLog(){
  const lst = dexLangOrder.map(l=>l || 'any');
  return Array.from(new Set(lst)).join(',');
}

// -------- mangalib meta (как было) --------
async function getPageSlug(Runtime){
  return await evalInPage(Runtime,`
    (function(){
      try{
        const u=new URL(location.href);
        const seg=u.pathname.split('/').filter(Boolean);
        const i=seg.indexOf('manga');
        if (i>=0 && i+1<seg.length) return seg[i+1];
      }catch(e){}
      return '';
    })()
  `);
}
async function scrapeMangalibMeta(Runtime){
  const names=await evalInPage(Runtime,`
    (function(){function t(n){return (n&&(n.textContent||'').trim())||'';}
      const h1=document.querySelector('h1 span, h1'); const h2=document.querySelector('h2');
      return {ruName:t(h1),enName:t(h2)}; })()
  `);
  const more=await evalInPage(Runtime,`
    (function(){function txt(n){return (n&&(n.textContent||'').replace(/\\s+/g,' ').trim())||'';}
      const s=document.querySelector('.section-body')||document;
      const desc=txt(s.querySelector('.text-collapse .up_ai, .text-collapse, .up_ai'));
      const chips=[...s.querySelectorAll('[data-type]')];
      const kw=[]; let restriction='';
      for (const c of chips){const typ=(c.getAttribute('data-type')||'').trim(); const v=txt(c);
        if(!v)continue; if(typ==='restriction')restriction=v; else kw.push(v);}
      return {ruDescription:desc, ruKeywords:kw, restriction}; })()
  `);
  return {
    ruName:names?.ruName||'',
    enName:names?.enName||'',
    ruDescription:more?.ruDescription||'',
    enDescription:'',
    ruKeywords:more?.ruKeywords||[],
    enKeywords:[],
    restriction:more?.restriction||''
  };
}

// -------- collect chapters (Mangalib) --------
async function collectMangalibChapters(Page, Runtime){
  log('  ▶ открываю вкладку "Главы"...');

  // force section=chapters
  const forced=await evalInPage(Runtime,`
    (function(){
      try{const u=new URL(location.href);
        if ((u.searchParams.get('section')||'').toLowerCase()!=='chapters'){
          u.searchParams.set('section','chapters'); return u.href; }
      }catch(e){} return null; })()
  `);
  if (forced) await goto(Page,Runtime,forced);

  // click tab for SPA
  await evalInPage(Runtime,`
    (function(){
      function txt(n){return (n&&(n.textContent||'').toLowerCase())||'';}
      const node=[...document.querySelectorAll('.tabs *')].find(n=>txt(n).includes('главы')||txt(n).includes('chapters'));
      if (node) (node.closest('button,[role=button],a,[tabindex]')||node).click();
      return true;
    })()
  `);

  async function smartLoad(maxMs){
    const start=Date.now(); let last=-1, stable=0, loops=0;
    async function count(){
      const {result}=await Runtime.evaluate({expression:`document.querySelectorAll('a[href*="/read/"]').length`,returnByValue:true});
      return Number(result?.value)||0;
    }
    async function pump(){
      await evalInPage(Runtime,`
        (async function(){
          const scroller =
            document.querySelector('.vue-recycle-scroller__item-wrapper')?.parentElement ||
            document.querySelector('[class*="recycle"][class*="scroller"]') || null;
          if (scroller){
            for (let i=0;i<${SMART_SCROLL_STEPS};i++){
              scroller.scrollBy(0, scroller.clientHeight);
              await new Promise(r=>setTimeout(r, ${SMART_STEP_MS}));
            }
          }
          const root=document.scrollingElement||document.documentElement;
          for (let i=0;i<${SMART_WINDOW_STEPS};i++){
            root.scrollTo(0, root.scrollHeight);
            await new Promise(r=>setTimeout(r, ${SMART_STEP_MS}));
          }
          return true;
        })()
      `);
    }
    do{
      await pump(); const c=await count(); dbg('  [smartLoad] /read/ count =',c); loops++;
      if (c>last){ last=c; stable=0;} else stable++;
      if (loops>=SMART_MIN_LOOPS && stable>=SMART_STABLE_TICKS) break;
      if (Date.now()-start>maxMs) break;
      await sleep(120);
    }while(true);
    return last;
  }
  const totalLinks = await smartLoad(SMART_MAX_MS);
  if (DEBUG) dbg('  итоговое число /read/ ссылок:', totalLinks);

  const raw=await evalInPage(Runtime,`
    (function(){
      function text(n){return (n&&(n.textContent||'').replace(/\\s+/g,' ').trim())||'';}
      function abs(h){try{return new URL(h,location.origin).href;}catch{return h;}}
      const as=[...document.querySelectorAll('a[href*="/read/"]')];
      const seen=new Set(); const out=[];
      for (const a of as){
        const href=abs(a.getAttribute('href')||''); if (!/\\/read\\//.test(href)) continue;
        if (seen.has(href)) continue; seen.add(href);
        let dateText=''; let p=a.parentElement;
        for (let i=0;i<3 && p; i++, p=p.parentElement){
          const dt=p.querySelector('.aft_h9, [data-tooltip="upload-info"]'); if (dt){ const s=text(dt); if (s){dateText=s; break;} }
        }
        out.push({href, label:text(a), dateText});
      }
      return out;
    })()
  `);
  dbg('  raw anchors (any /read/):', raw?.length);

  function chapterFromHref(h){
    const m = h.match(/\/c(\d+(?:\.\d+)?)(?:\D|$)/i);
    return m ? parseFloat(m[1]) : null;
  }
  function chapterFromLabel(s){
    const m=(s||'').match(/глава\s*([\d\.]+)/i)||(s||'').match(/chapter\s*([\d\.]+)/i);
    return m?parseFloat(m[1]):null;
  }
  function ruDate(s){
    const m=(s||'').match(/(\d{2})\.(\d{2})\.(\d{4})/);
    return m?new Date(+m[3],+m[2]-1,+m[1]).getTime():null;
  }

  const by=new Map();
  for (const r of (raw||[])){
    const ch = chapterFromHref(r.href) ?? chapterFromLabel(r.label);
    if (ch==null) continue;
    const ts = ruDate(r.dateText) ?? Number.MAX_SAFE_INTEGER;
    const prev=by.get(ch);
    if (!prev || ts<prev.ts) by.set(ch,{ch, href:r.href, label:r.label||`Глава ${ch}`, ts, dateText:r.dateText});
  }
  const chapters=[...by.values()].sort((a,b)=>a.ch-b.ch);
  log(`  ✓ найдено глав: ${chapters.length}`);
  if (DEBUG) chapters.slice(0,10).forEach(it=>dbg('   →',it.ch,it.dateText,it.href));
  return chapters;
}

// -------- image downloading (Node, with cookies) --------
function extFromMimeOrUrl(mime,url){
  const m=(mime||'').toLowerCase();
  if (m.includes('png')) return '.png';
  if (m.includes('jpeg')||m.includes('jpg')) return '.jpg';
  if (m.includes('webp')) return '.webp';
  if (m.includes('gif'))  return '.gif';
  if (m.includes('svg'))  return '.svg';
  const um=(url||'').toLowerCase().match(/\.(png|jpe?g|webp|gif|svg|jfif)(\?|$)/);
  return um?('.'+um[1].toLowerCase().replace('jpeg','jpg')):'.png';
}
function existingImagesInfo(dir){
  if (!fs.existsSync(dir)) return {count:0, maxIndex:0};
  const files = fs.readdirSync(dir).filter(f=>/^\d{3}\.(png|jpe?g|webp|gif)$/i.test(f));
  const nums = files.map(f=>parseInt(f.split('.')[0],10)).filter(n=>Number.isFinite(n));
  const maxIndex = nums.length ? Math.max(...nums) : 0;
  return {count:files.length, maxIndex};
}
function looksLikePageCdn(url){
  return /(?:mixlib|mangalib|img\d?\.)/i.test(url||'') && /\.(png|jpe?g|webp)(?:\?|$)/i.test(url||'');
}
function looksLikeBuffCdn(url){
  return /(mangabuff\.ru|^https?:\/\/c\d+\.)/i.test(url||'') && /\.(png|jpe?g|webp)(?:\?|$)/i.test(url||'');
}

async function getCookieHeader(Network, urlStr){
  try{
    const {cookies} = await Network.getCookies({urls:[urlStr]});
    if (!cookies || !cookies.length) return '';
    return cookies.map(c=>`${c.name}=${c.value}`).join('; ');
  }catch{ return ''; }
}
function fetchBufferWithHeaders(url, headers){
  const lib = url.startsWith('https') ? https : http;
  return new Promise((resolve,reject)=>{
    const req = lib.request(url, {method:'GET', headers}, res=>{
      if (res.statusCode>=300 && res.statusCode<400 && res.headers.location){
        const red = new URL(res.headers.location, url).href;
        return resolve(fetchBufferWithHeaders(red, headers));
      }
      if (res.statusCode!==200){
        return reject(new Error('HTTP '+res.statusCode));
      }
      const chunks=[]; res.on('data',d=>chunks.push(d));
      res.on('end',()=>resolve({buf:Buffer.concat(chunks), mime:res.headers['content-type']||''}));
    });
    req.on('error',reject); req.end();
  });
}

// --- dropdown-first per-page downloader (Mangalib) ---
async function downloadViaDropdown(Page, Runtime, Network, chapterUrl, saveDir){
  const meta = await evalInPage(Runtime,`
    (function(){
      const sel =
        document.querySelector('footer select.form-input__field') ||
        document.querySelector('label select.form-input__field')  ||
        document.querySelector('select.form-input__field');
      if (!sel) return {has:false,total:0};
      const total = sel.options.length || 0;
      const values = [...sel.options].map(o => Number(o.value) || 0).filter(Boolean);
      return {has:true,total,values};
    })()
  `);
  if (!meta?.has || !meta.total) return {saved:0, reason:'no-dropdown'};

  dbg('    [dropdown] pages total =', meta.total);

  let saved=0; let idx=existingImagesInfo(saveDir).maxIndex;
  for (const p of meta.values){
    await evalInPage(Runtime,`
      (async function(pageNum){
        const sel =
          document.querySelector('footer select.form-input__field') ||
          document.querySelector('label select.form-input__field')  ||
          document.querySelector('select.form-input__field');
        if (sel){
          sel.value = String(pageNum);
          sel.dispatchEvent(new Event('change', {bubbles:true}));
        }
        await new Promise(r=>setTimeout(r, 200));
        const tgt = document.querySelector('[data-page="'+pageNum+'"]');
        if (tgt){ tgt.scrollIntoView({block:'center'}); }
        return true;
      })(${JSON.stringify(p)})
    `);

    const ok = await waitForSelector(Runtime, `[data-page="${p}"] img`, 12000, 160);
    if (!ok){ log(`      ⚠️ p=${p}: img не появился`); continue; }

    const imgUrl = await evalInPage(Runtime,`
      (function(pageNum){
        const img = document.querySelector('[data-page="'+pageNum+'"] img');
        return img ? (img.currentSrc || img.src || '') : '';
      })(${JSON.stringify(p)})
    `);
    if (!looksLikePageCdn(imgUrl)){ log(`      ⚠️ p=${p}: bad url (${imgUrl||'none'})`); continue; }

    await limiter.hit('fetch-img');
    const cookie = await getCookieHeader(Network, imgUrl);
    const headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': chapterUrl,
      'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
      'Accept-Language': 'ru,en;q=0.9',
      ...(cookie ? {'Cookie': cookie} : {})
    };

    try{
      const {buf, mime} = await fetchBufferWithHeaders(imgUrl, headers);
      const ext  = extFromMimeOrUrl(mime, imgUrl);
      if (ext==='.gif' || ext==='.svg'){ dbg('      skip loader', imgUrl); continue; }
      idx++; const file = path.join(saveDir, String(idx).padStart(3,'0') + ext);
      fs.writeFileSync(file, buf);
      saved++; log(`      ✓ сохранено: ${path.basename(file)}`);
      await sleep(STEP_DELAY);
    }catch(e){ log(`      ⚠️ p=${p}: ${e.message||e}`); }
  }

  return {saved};
}

// --- long-strip fallback (Mangalib) ---
async function downloadLongStrip(Page, Runtime, Network, chapterUrl, saveDir){
  const urls = await evalInPage(Runtime,`
    (async function(){
      const wrap=document.querySelector('.vc_be')||document.body;
      const pg = [...wrap.querySelectorAll('[data-page]')];
      const out=[];
      for (const p of pg){
        p.scrollIntoView({block:'center'}); await new Promise(r=>setTimeout(r,220));
        const img = p.querySelector('img[class*="acj_mr"], img');
        if (img){
          const src = img.currentSrc || img.src || '';
          const n   = Number(p.getAttribute('data-page')||'0')||0;
          if (src) out.push({n,url:src});
        }
      }
      return out.sort((a,b)=>a.n-b.n);
    })()
  `);

  if (!urls || !urls.length) return {saved:0, reason:'no-urls-long-strip'};

  let saved=0, idx=0;
  for (const {n, url} of urls){
    if (!looksLikePageCdn(url)) { dbg('      skip non-page img', url); continue; }
    await limiter.hit('fetch-img');

    const cookie = await getCookieHeader(Network, url);
    const headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': chapterUrl,
      'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
      'Accept-Language': 'ru,en;q=0.9',
      ...(cookie ? {'Cookie': cookie} : {})
    };

    try{
      const {buf, mime} = await fetchBufferWithHeaders(url, headers);
      const ext  = extFromMimeOrUrl(mime, url);
      if (ext==='.gif' || ext==='.svg'){ dbg('      skip loader', url); continue; }
      idx++; const file = path.join(saveDir, String(idx).padStart(3,'0') + ext);
      fs.writeFileSync(file, buf);
      saved++; log(`      ✓ сохранено: ${path.basename(file)}`);
      await sleep(STEP_DELAY);
    }catch(e){ log(`      ⚠️ p=${n}: ${e.message||e}`); }
  }
  return {saved};
}

// --- ?p= fallback (Mangalib) ---
async function downloadByParamPaging(Page, Runtime, Network, baseHref, saveDir){
  let saved=0, misses=0; const MAX_MISSES=2; const HARD_LIMIT=800;

  for (let p=1; p<=HARD_LIMIT; p++){
    const url=new URL(baseHref, 'https://mangalib.org/'); url.searchParams.set('p', String(p));
    dbg('      → goto page', p, url.href);
    await goto(Page, Runtime, url.href);

    const ready = await waitForSelector(Runtime, `.vc_be [data-page] img, [data-page="${p}"] img`, 15000);
    if (!ready){
      log(`      ⚠️ p=${p}: img не найден`);
      if (++misses>=MAX_MISSES){ log('      ⏹ останов по двум подряд промахам'); break; }
      continue;
    }

    const imgUrl = await evalInPage(Runtime,`
      (function(n){
        const exact = document.querySelector('[data-page="'+n+'"] img');
        const cand  = exact ? (exact.currentSrc||exact.src) : '';
        if (cand) return cand;
        const any = document.querySelector('.vc_be [data-page] img');
        return any ? (any.currentSrc||any.src) : '';
      })(${JSON.stringify(p)})
    `);

    if (!looksLikePageCdn(imgUrl)){
      log(`      ⚠️ p=${p}: bad url (${imgUrl||'none'})`);
      if (++misses>=MAX_MISSES){ log('      ⏹ останов по двум подряд промахам'); break; }
      continue;
    }

    await limiter.hit('fetch-img');
    const cookie = await getCookieHeader(Network, imgUrl);
    const headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': url.href,
      'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
      'Accept-Language': 'ru,en;q=0.9',
      ...(cookie ? {'Cookie': cookie} : {})
    };

    try{
      const {buf, mime} = await fetchBufferWithHeaders(imgUrl, headers);
      const ext  = extFromMimeOrUrl(mime, imgUrl);
      if (ext==='.gif' || ext==='.svg'){ dbg('      skip loader', imgUrl); continue; }
      const file = path.join(saveDir, String(p).padStart(3,'0') + ext);
      fs.writeFileSync(file, buf);
      saved++;
      log(`      ✓ сохранено: ${path.basename(file)}`);
      await sleep(STEP_DELAY);
      misses=0;
    }catch(e){
      log(`      ⚠️ p=${p}: ${e.message||e}`);
      if (++misses>=MAX_MISSES){ log('      ⏹ останов по двум подряд промахам'); break; }
    }
  }

  return {saved};
}

async function downloadMangalibChapter(Page, Runtime, Network, chapterHref, saveDir){
  const base = new URL(chapterHref, 'https://mangalib.org/');
  await goto(Page, Runtime, base.href);
  await sleep(STEP_DELAY);

  await waitForSelector(Runtime, '.vc_be, [data-page], select.form-input__field', 15000);

  log('    ▶ скачивание страниц (dropdown → long-strip → ?p=)');
  ensureDir(saveDir);

  const viaSelect = await downloadViaDropdown(Page, Runtime, Network, base.href, saveDir);
  if (viaSelect.saved>0){ log(`    ✓ глав готово: сохранено страниц = ${viaSelect.saved}`); return; }

  const viaStrip = await downloadLongStrip(Page, Runtime, Network, base.href, saveDir);
  if (viaStrip.saved>0){ log(`    ✓ глав готово: сохранено страниц = ${viaStrip.saved}`); return; }

  log('    ↪️ перехожу к ?p=');
  const res2 = await downloadByParamPaging(Page, Runtime, Network, base.href, saveDir);
  if (res2.saved===0){
    log('    ⚠️ ни одной страницы не скачано — возможно, логин/18+/защита.');
  } else {
    log(`    ✓ глав готово: сохранено страниц = ${res2.saved}`);
  }
}

// -------- MANGABUFF: meta & chapters --------

// meta со страницы манги
async function scrapeMangabuffMeta(Runtime){
  return await evalInPage(Runtime, `
    (function(){
      function txt(n){return (n&&(n.textContent||'').replace(/\\s+/g,' ').trim())||'';}
      // ВАЖНО: формируем абсолютные ссылки относительно location.href, чтобы не потерять /manga
      function abs(h){ try { return new URL(h, location.href).href; } catch(e){ return h||''; } }

      const root = document.querySelector('.manga__middle-wrapper') || document;

      // Сбор имён и раскладка по языкам
      const nameH1 = txt(root.querySelector('.manga__names h1.manga__name'));
      const altSpans = [...(root.querySelectorAll('.manga__names h3.manga__name-alt span')||[])];
      const rawNames = [nameH1, ...altSpans.map(s=>txt(s))].filter(Boolean);

      const isRU = s => /[А-Яа-яЁё]/.test(s);
      const isJP = s => /[\\u3040-\\u30ff\\u3400-\\u4dbf\\u4e00-\\u9fff]/.test(s);
      const isEN = s => /[A-Za-z]/.test(s);

      let ruName = '', enName = '', jpName = '';
      if (isRU(nameH1)) ruName = nameH1;
      else if (isJP(nameH1)) jpName = nameH1;
      else if (isEN(nameH1)) enName = nameH1;
      for (const n of rawNames){
        if (!ruName && isRU(n)) { ruName = n; continue; }
        if (!enName && isEN(n)) { enName = n; continue; }
        if (!jpName && isJP(n)) { jpName = n; continue; }
      }

      const rvEl = root.querySelector('[itemprop="ratingValue"]');
      const rcEl = root.querySelector('[itemprop="ratingCount"]');
      const ratingValue = rvEl ? Number(rvEl.getAttribute('content')||'') || null : null;
      const ratingCount = rcEl ? Number(rcEl.getAttribute('content')||'') || null : null;
      const viewsText   = txt(root.querySelector('.manga__views'));
      const views = viewsText ? Number(viewsText.replace(/[^\\d]/g,''))||null : null;

      const descNode = document.querySelector('.manga__description');
      const desc = txt(descNode);

      const tagNodes = [...(document.querySelectorAll('.tags .tags__item')||[])];
      const tags = tagNodes.map(n=>txt(n)).filter(Boolean).filter(t=>!/^\\+$/.test(t));

      // read button — строим относительно location.href
      const readBtn = document.querySelector('a.read-btn, .button.read-btn, a.button--primary.read-btn');
      const hrefRaw = readBtn ? (readBtn.getAttribute('href')||'') : '';
      let firstChapterUrl = hrefRaw ? abs(hrefRaw) : '';
      // safety net: если внезапно без /manga, а мы на /manga/<slug>, допишем
      try {
        if (/^https?:\\/\\/[^/]+\\/(?!manga\\/)/i.test(firstChapterUrl) && location.pathname.startsWith('/manga/')) {
          const fixed = '/manga/' + hrefRaw.replace(/^\\/?/,'').replace(/^manga\\//,'');
          firstChapterUrl = new URL(fixed, location.origin).href;
        }
      } catch(e){}

      return {
        ruName, enName, jpName,
        ruDescription: desc,
        ruKeywords: tags,
        ratingValue, ratingCount, views,
        firstChapterUrl
      };
    })()
  `);
}

// распарсить vol/ch из URL
function parseBuffVolChap(urlStr){
  try{
    const u = new URL(urlStr);
    // /manga/<slug>/<vol>/<chap>
    const parts = u.pathname.split('/').filter(Boolean);
    const i = parts.indexOf('manga');
    if (i>=0 && i+3<parts.length){
      return { vol: parts[i+2], chap: parts[i+3] };
    }
    const m = u.pathname.match(/\/manga\/[^/]+\/(\d+)\/([\d.]+)/i);
    if (m) return { vol:m[1], chap:m[2] };
  }catch{}
  return { vol:'1', chap:'1' };
}

// умная прокрутка, чтобы догрузились data-src
async function smartLoadBuff(Runtime, maxMs=SMART_MAX_MS){
  const start=Date.now(); let lastH=0, stable=0, loops=0;
  async function pump(){
    await evalInPage(Runtime, `
      (async function(){
        const root = document.scrollingElement || document.documentElement;
        for (let i=0;i<${SMART_WINDOW_STEPS};i++){
          root.scrollTo(0, root.scrollHeight);
          await new Promise(r=>setTimeout(r, ${SMART_STEP_MS}));
        }
        return true;
      })()
    `);
  }
  async function height(){
    const {result}=await Runtime.evaluate({expression:`document.documentElement.scrollHeight`,returnByValue:true});
    return Number(result?.value)||0;
  }
  do{
    await pump(); const h=await height(); loops++;
    if (h>lastH){ lastH=h; stable=0; } else stable++;
    if (loops>=SMART_MIN_LOOPS && stable>=SMART_STABLE_TICKS) break;
    if (Date.now()-start>maxMs) break;
    await sleep(120);
  }while(true);
}

// собрать URL страниц из .reader__pages
async function collectBuffPageUrls(Runtime){
  return await evalInPage(Runtime, `
    (function(){
      function abs(h){ try { return new URL(h, location.href).href; } catch(e){ return h||''; } }
      const items = [...(document.querySelectorAll('.reader__pages .reader__item[data-page]')||[])];
      const out = [];
      for (const it of items){
        const n = Number(it.getAttribute('data-page')||'0')||0;
        const img = it.querySelector('img');
        if (!img) continue;
        const s = img.currentSrc || img.getAttribute('src') || img.getAttribute('data-src') || '';
        if (!s) continue;
        out.push({ n, url: abs(s) });
      }
      out.sort((a,b)=>a.n-b.n);
      return out;
    })()
  `);
}

async function getBuffNextHref(Runtime){
  return await evalInPage(Runtime, `
    (function(){
      function abs(h){ try { return new URL(h, location.href).href; } catch(e){ return h||''; } }
      const wrap = document.querySelector('.reader__controls-wrapper');
      if (!wrap) return '';
      const links = [...(wrap.querySelectorAll('a.navigate-button[href]')||[])];
      if (!links.length) return '';
      const last = links[links.length-1];
      const href = last.getAttribute('href')||'';
      if (!href || href === '#') return '';
      return abs(href);
    })()
  `);
}

// -------- WEBTOONS: meta, chapters, download --------
async function scrapeWebtoonsMeta(Runtime){
  return await evalInPage(Runtime, `
    (function(){
      function txt(n){return (n&&(n.textContent||'').replace(/\\s+/g,' ').trim())||'';}
      function abs(h){ try { return new URL(h, location.href).href; } catch(e){ return h||''; } }
      const title = txt(document.querySelector('h1.subj'));
      const description = txt(document.querySelector('p.summary'));
      const genre = txt(document.querySelector('h2.genre'));
      return { title, description, tags: genre ? [genre] : [], mainUrl: abs(location.href) };
    })()
  `);
}

async function collectWebtoonsEpisodes(Page, Runtime, mainUrl){
  const seenPages = new Set();
  const queue = [mainUrl];
  const byNo = new Map();

  async function scrapePage(url){
    await goto(Page, Runtime, url);
    await waitForSelector(Runtime, '#_listUl li._episodeItem a[href], ul#_listUl li a[href]', 15000);
    return await evalInPage(Runtime, `
      (function(){
        function abs(h){ try { return new URL(h, location.href).href; } catch(e){ return h||''; } }
        const pageLinks = [...(document.querySelectorAll('.paginate a[href]')||[])]
          .map(a=>a.getAttribute('href')||'')
          .filter(h=>h && h!== '#' && !/^javascript:/i.test(h))
          .map(abs)
          .filter(Boolean);
        const eps = [];
        const items=[...(document.querySelectorAll('#_listUl li._episodeItem')||[])];
        for (const li of items){
          const a = li.querySelector('a[href]');
          if (!a) continue;
          const href = abs(a.getAttribute('href')||'');
          const label = (a.querySelector('.subj span')||a.querySelector('.subj')||{}).textContent || '';
          const dateText = (a.querySelector('.date')||{}).textContent || '';
          const no = Number(li.getAttribute('data-episode-no')||'') || null;
          if (!href) continue;
          eps.push({href, label: label.replace(/\\s+/g,' ').trim(), dateText: dateText.trim(), no});
        }
        return {pageLinks, episodes:eps};
      })()
    `);
  }

  while (queue.length){
    const url = queue.shift();
    if (seenPages.has(url)) continue;
    seenPages.add(url);
    const res = await scrapePage(url);
    for (const p of res.pageLinks||[]){
      if (p && !seenPages.has(p)) queue.push(p);
    }
    for (const ep of res.episodes||[]){
      const key = ep.no || ep.href;
      if (byNo.has(key)) continue;
      byNo.set(key, ep);
    }
  }

  const list = [...byNo.values()];
  list.sort((a,b)=>{
    if (a.no!=null && b.no!=null) return a.no - b.no;
    return (a.href||'').localeCompare(b.href||'');
  });
  return list;
}

async function smartLoadWebtoons(Runtime, maxMs=SMART_MAX_MS){
  const start=Date.now(); let lastH=0, stable=0;
  while (Date.now()-start < maxMs){
    await evalInPage(Runtime, `
      (async function(){
        const root=document.scrollingElement||document.documentElement;
        for (let i=0;i<${SMART_WINDOW_STEPS};i++){
          root.scrollTo(0, root.scrollHeight);
          await new Promise(r=>setTimeout(r, ${SMART_STEP_MS}));
        }
        return true;
      })()
    `);
    const h = await evalInPage(Runtime, 'document.documentElement.scrollHeight', true);
    if (h>lastH){ lastH=h; stable=0; } else stable++;
    if (stable>=SMART_STABLE_TICKS) break;
    await sleep(100);
  }
}

async function collectWebtoonsImages(Runtime){
  await smartLoadWebtoons(Runtime);
  const urls = await evalInPage(Runtime, `
    (function(){
      function abs(h){ try { return new URL(h, location.href).href; } catch(e){ return h||''; } }
      const imgs=[...(document.querySelectorAll('#_imageList img, .viewer_lst img, img._images')||[])];
      const out=[];
      for (const img of imgs){
        const s = img.getAttribute('data-url') || img.currentSrc || img.src || '';
        if (!s) continue;
        out.push(abs(s));
      }
      return out;
    })()
  `);
  const seen=new Set(); const uniq=[];
  for (const u of urls||[]){
    if (!u) continue;
    if (seen.has(u)) continue;
    seen.add(u); uniq.push(u);
  }
  return uniq;
}

async function downloadWebtoonsEpisode(Page, Runtime, Network, episode, saveDir){
  await goto(Page, Runtime, episode.href);
  await waitForSelector(Runtime, '#_imageList img, .viewer_lst img, img._images', 15000);
  ensureDir(saveDir);
  const srcs = await collectWebtoonsImages(Runtime);
  let saved=existingImagesInfo(saveDir).count;
  for (let i=0;i<srcs.length;i++){
    const src = srcs[i];
    if (!looksLikeWebtoonsImg(src)) continue;
    await limiter.hit('fetch-img');
    const cookie = await getCookieHeader(Network, src);
    const headers = {
      'User-Agent': DEFAULT_UA,
      'Referer': episode.href,
      'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
      'Accept-Language': 'ru,en;q=0.9',
      ...(cookie ? {'Cookie': cookie} : {})
    };
    try{
      const {buf, mime} = await fetchBufferWithHeaders(src, headers);
      const ext = extFromMimeOrUrl(mime, src);
      const file = path.join(saveDir, String(i+1).padStart(3,'0') + ext);
      fs.writeFileSync(file, buf);
      saved++;
      log(`      saved ${path.basename(file)}`);
      await sleep(STEP_DELAY);
    }catch(e){
      log(`      fail page ${i+1}: ${e.message||e}`);
    }
  }
  return {saved};
}

async function processWebtoons(Page, Runtime, Network, link, channelFolderAbs){
  log(`\n=== WEBTOONS: ${link}`);
  await goto(Page, Runtime, link);

  const meta = await scrapeWebtoonsMeta(Runtime);
  const folderName = sanitizeName(meta?.title || 'manga');
  const outFolder  = path.join(channelFolderAbs, 'manga', folderName);
  ensureDir(outFolder);
  const unified = toUnifiedMeta({
    names:[meta?.title],
    descriptions:[meta?.description],
    keywords:[...(meta?.tags||[])],
    mainUrl: link
  });
  writeJSON(path.join(outFolder,'mangaMeta.json'), { mangaMeta: unified });
  log(`  meta saved: ${path.join(outFolder,'mangaMeta.json')}`);

  const episodes = await collectWebtoonsEpisodes(Page, Runtime, link);
  if (!episodes.length){
    log('  no episodes found on Webtoons');
    return true;
  }

  for (const ep of episodes){
    const label = ep.label || (`episode-${ep.no||''}`).trim();
    log(`  -> ${label} (${ep.href})`);
    const dirName = (ep.no!=null ? String(ep.no) : label || 'episode').replace(/[\\/:*?"<>|]/g,'_').replace(/\s+/g,'_');
    const chDir = path.join(outFolder, dirName);
    await downloadWebtoonsEpisode(Page, Runtime, Network, ep, chDir);
  }
  return true;
}

async function downloadMangabuffChapter(Page, Runtime, Network, chapterUrl, saveDir){
  await goto(Page, Runtime, chapterUrl);
  await waitForSelector(Runtime, '.reader__pages', 15000);

  log('    ▶ скачивание страниц (Buff long-strip)');
  ensureDir(saveDir);

  await smartLoadBuff(Runtime);

  const urls = await collectBuffPageUrls(Runtime);
  if (!urls || !urls.length){
    log('    ⚠️ не нашли страниц в .reader__pages — возможно, разметка изменилась или требуется логин/18+');
    return {saved:0};
  }

  let saved=0, idx=0;
  for (const {n, url} of urls){
    if (!looksLikeBuffCdn(url)) { dbg('      skip non-buff img', url); continue; }
    // БЕЗ паузы и лимитера для Mangabuff (по твоей просьбе):
    // await limiter.hit('fetch-img');

    const cookie = await getCookieHeader(Network, url);
    const headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Referer': chapterUrl,
      'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
      'Accept-Language': 'ru,en;q=0.9',
      ...(cookie ? {'Cookie': cookie} : {})
    };
    try{
      const {buf, mime} = await fetchBufferWithHeaders(url, headers);
      const ext  = extFromMimeOrUrl(mime, url);
      if (ext==='.gif' || ext==='.svg'){ dbg('      skip loader', url); continue; }
      idx++; const file = path.join(saveDir, String(idx).padStart(3,'0') + ext);
      fs.writeFileSync(file, buf);
      saved++; log(`      ✓ сохранено: ${path.basename(file)}`);
      // Без паузы между картинками на Mangabuff:
      // await sleep(STEP_DELAY);
    }catch(e){ log(`      ⚠️ p=${n}: ${e.message||e}`); }
  }

  log(`    ✓ глав готово: сохранено страниц = ${saved}`);
  return {saved};
}

async function processMangabuff(Page, Runtime, Network, link, channelFolderAbs){
  log(`\n=== MANGABUFF: ${link}`);
  await goto(Page, Runtime, link);

  // метаданные
  const meta = await scrapeMangabuffMeta(Runtime);
  // Проверка firstChapterUrl
  if (!meta || !meta.firstChapterUrl) {
    log('  ⚠️ не нашли ссылку "читать" (read-btn) — возможно, нет прав/кнопки или изменилась разметка.');
  }

  const folderName = sanitizeName(meta?.ruName || meta?.enName || meta?.jpName || 'manga');
  const outFolder  = path.join(channelFolderAbs, 'manga', folderName);
  ensureDir(outFolder);
  const unified = toUnifiedMeta({
    names:[meta?.ruName, meta?.enName, meta?.jpName],
    descriptions:[meta?.ruDescription],
    keywords:[...(meta?.ruKeywords||[])],
    mainUrl: link
  });
  writeJSON(path.join(outFolder,'mangaMeta.json'), { mangaMeta: unified });
  log(`  ✓ meta записан: ${path.join(outFolder,'mangaMeta.json')}`);

  if (!meta?.firstChapterUrl) return true;

  // идём по главам последовательно по стрелке "вперёд"
  const seen = new Set();
  let current = meta.firstChapterUrl;
  let chaptersDone = 0;

  while (current && !seen.has(current)){
    seen.add(current);
    const {vol, chap} = parseBuffVolChap(current);
    log(`  → том ${vol} глава ${chap} (${current})`);
    const chDir = path.join(outFolder, String(vol), String(chap).replace(/\./g,'_'));
    ensureDir(chDir);

    await downloadMangabuffChapter(Page, Runtime, Network, current, chDir);

    // найти ссылку на следующую главу
    const nextHref = await getBuffNextHref(Runtime);
    if (!nextHref){ log('  ⏹ следующая глава не найдена — стоп'); break; }
    current = nextHref;
    chaptersDone++;
    // Между главами можем оставить небольшую паузу (если не нужно — закомментируй):
    // await sleep(Math.max(STEP_DELAY, 100));
  }

  log(`  ✓ Mangabuff завершён. Скачано глав: ${chaptersDone || seen.size}`);
  return true;
}




// -------- MANGADEX: meta, chapters, download --------
const PAGE_EXTS = ['.png','.jpg','.jpeg','.webp','.gif'];
function pageFilePath(dir, idx, ext){
  const pad = String(idx).padStart(3,'0');
  for (const e of PAGE_EXTS){
    const f = path.join(dir, pad + e.replace('jpeg','jpg'));
    if (fs.existsSync(f)) return {file:f, exists:true, ext:e.replace('jpeg','jpg')};
  }
  const e = (ext||'.png').replace('jpeg','jpg');
  return {file: path.join(dir, pad + e), exists:false, ext:e};
}
async function fetchBlobBytes(Runtime, url){
  const expr = `(async()=>{const r=await fetch(${JSON.stringify(url)}); if(!r.ok) throw new Error('HTTP '+r.status); const b=await r.arrayBuffer(); return Array.from(new Uint8Array(b));})()`;
  const { result, exceptionDetails } = await Runtime.evaluate({ expression: expr, returnByValue:true, awaitPromise:true });
  if (exceptionDetails) throw new Error(exceptionDetails.text || 'blob fetch error');
  return Buffer.from(result.value);
}
async function scrapeDexMetaDom(Runtime){
  return await evalInPage(Runtime, `
    (function(){
      function txt(n){return (n&&(n.textContent||'').replace(/\\s+/g,' ').trim())||'';}
      const mainTitle = txt(document.querySelector('.title p, .reader--header-title, h1'));
      const altTitle = txt(document.querySelector('.title [title], .alt-title span'));
      const desc = txt(document.querySelector('[style*="grid-area: synopsis"] p, [style*="grid-area: synopsis"] div p'));
      const tags = [...document.querySelectorAll('[href*="/tag/"] span, [locale] a.tag span')].map(e=>txt(e)).filter(Boolean);
      return { mainTitle, altTitle, description: desc, tags };
    })()
  `);
}
async function clickDexRead(Page, Runtime){
  const clicked = await evalInPage(Runtime, `
    (function(){
      const btns = [...document.querySelectorAll('button, a')];
      // строгие кандидаты: иконка книги или ссылка на /chapter/, исключаем историю
      const candidates = btns.filter(n=>{
        const txt=(n.textContent||'').trim().toLowerCase();
        const href=(n.getAttribute('href')||'').toLowerCase();
        const hasIcon = !!n.querySelector('svg.feather-book-open');
        if (href.includes('/my/history')) return false;
        if (href.includes('/chapter/')) return true;
        if (hasIcon) return true;
        return ['read','читать','start reading'].some(k=>txt===k || txt.startsWith(k));
      });
      const target = candidates[0];
      if (target){ (target.closest('a,button')||target).click(); return true; }
      return false;
    })()
  `);
  if (clicked){ await sleep(500); await waitForSelector(Runtime, '.md-modal__box, .md--reader-pages, .reader--header', 12000); }
}
async function chooseDexGroupByLang(Runtime){
  const prefList = JSON.stringify(dexLangOrder || []);
  const script = `
    (function(){
      const entries=[...document.querySelectorAll('.md-modal__box .chapter-grid')].map(ch=>{
        const img=ch.querySelector('img[title]');
        const lang=(img&&img.getAttribute('title')||'').toLowerCase();
        const href=(ch.querySelector('a[href]')||{}).href||'';
        return {lang,href,node:ch};
      }).filter(e=>e.href);
      const pref=${prefList};
      for (const l of pref){
        if (!l) continue;
        const f=entries.find(e=>e.lang.startsWith(l));
        if (f){ (f.node.querySelector('a[href]')||f.node).click(); return true; }
      }
      if (entries[0]){ (entries[0].node.querySelector('a[href]')||entries[0].node).click(); return true; }
      return false;
    })()
  `;
  const choice = await evalInPage(Runtime, script);
  if (choice){ await waitForSelector(Runtime, '.md--reader-pages, .reader--header', 12000); }
}
async function ensureDexMenuOpen(Runtime){
  const open = await evalInPage(Runtime, `!!document.querySelector('.reader--menu.open')`);
  if (!open){
    await evalInPage(Runtime, `
      (function(){ const btn=document.querySelector('.reader--meta.menu'); if(btn) btn.click(); return true; })()
    `);
    await sleep(400);
  }
}
async function collectDexChapterLinks(Runtime){
  await ensureDexMenuOpen(Runtime);
  await evalInPage(Runtime, `
    (function(){
      const trigger=document.querySelector('#chapter-selector .feather-chevron-down, #chapter-selector [class*="chevron"], #chapter-selector .relative');
      if (trigger) trigger.click();
      return true;
    })()
  `);
  await sleep(300);
  const res = await evalInPage(Runtime, `
    (function(){
      try{
        const out=[];
        const items=[...document.querySelectorAll('#chapter-selector ul li[data-value]')];
        for (const li of items){
          const id = li.getAttribute('data-value')||'';
          const label=(li.textContent||'').replace(/\\s+/g,' ').trim();
          let href='';
          const a=li.querySelector('a[href]');
          if (a) href=a.href; else if (id) href=new URL('/chapter/'+id, location.origin).href;
          out.push({id, label, href});
        }
        return {ok:true, data:out};
      }catch(e){ return {ok:false, error:(e&&e.message)||String(e)}; }
    })()
  `);
  if (!res || res.ok!==true){ throw new Error('collect-chapters: ' + ((res && res.error) || 'unknown')); }
  return res.data || [];
}

async function smartLoadDexImages(Runtime){
  const steps = SMART_WINDOW_STEPS;
  const delay = SMART_STEP_MS;
  const start=Date.now(); let lastH=0, stable=0;
  while (Date.now()-start < SMART_MAX_MS){
    await evalInPage(Runtime, `(async function(){
      const root=document.scrollingElement||document.documentElement;
      for (let i=0;i<${steps};i++){
        root.scrollTo(0, root.scrollHeight);
        await new Promise(r=>setTimeout(r, ${delay}));
      }
      return true;
    })()`);
    const h = await evalInPage(Runtime, 'document.documentElement.scrollHeight', true);
    if (h>lastH){ lastH=h; stable=0; } else stable++;
    if (stable>=SMART_STABLE_TICKS) break;
    await sleep(100);
  }
}
async function ensureDexImagesLoaded(Runtime){
  for (let attempt=0; attempt<12; attempt++){
    const remaining = await evalInPage(Runtime, `
      (function(){
        const items=[...document.querySelectorAll('.unloaded')];
        for (const it of items){
          try{
            it.scrollIntoView({block:'center'});
            const btn = it.querySelector('button, span');
            if (btn) btn.dispatchEvent(new Event('click', {bubbles:true}));
          }catch(e){}
        }
        return items.length;
      })()
    `);
    if (!remaining) return true;
    await sleep(600);
  }
  return false;
}
async function collectDexImageSrcs(Runtime){
  await smartLoadDexImages(Runtime);
  await ensureDexImagesLoaded(Runtime);
  return await evalInPage(Runtime, `
    (function(){
      const imgs=[...document.querySelectorAll('.md--reader-pages img')];
      return imgs.map(img=>img.currentSrc||img.src||'').filter(Boolean);
    })()
  `);
}
async function saveDexChapterDom(Page, Runtime, Network, chapter, saveDir){
  await goto(Page, Runtime, chapter.href);
  await waitForSelector(Runtime, '.md--reader-pages img', 15000);
  ensureDir(saveDir);
  const progressFile = path.join(saveDir,'progress.json');
  const srcs = await collectDexImageSrcs(Runtime);
  let savedCount = existingImagesInfo(saveDir).count;
  for (let i=0;i<srcs.length;i++){
    const src = srcs[i];
    const pageNum = i+1;
    const target = pageFilePath(saveDir, pageNum, path.extname(src)||'.jpg');
    if (target.exists){
      savedCount = Math.min(savedCount+1, srcs.length);
      try{ writeJSON(progressFile, {saved:savedCount, total:srcs.length, chapterId: chapter.id||''}); }catch{}
      continue;
    }
    try{
      let buf, mime='';
      if (src.startsWith('blob:')){
        buf = await fetchBlobBytes(Runtime, src);
      } else {
        await limiter.hit('fetch-img');
        const cookie = await getCookieHeader(Network, src);
        const headers = {
          'User-Agent': DEFAULT_UA,
          'Referer': chapter.href,
          'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
          'Accept-Language': 'ru,en;q=0.9',
          ...(cookie ? {'Cookie': cookie} : {})
        };
        const res = await fetchBufferWithHeaders(src, headers);
        buf = res.buf; mime = res.mime||'';
      }
      const ext = extFromMimeOrUrl(mime, src);
      const file = target.file.endsWith(ext) ? target.file : path.join(saveDir, String(pageNum).padStart(3,'0') + ext);
      fs.writeFileSync(file, buf);
      savedCount = Math.min(savedCount+1, srcs.length);
      log(`      saved ${path.basename(file)}`);
      try{ writeJSON(progressFile, {saved:savedCount, total:srcs.length, chapterId: chapter.id||''}); }catch{}
      await sleep(STEP_DELAY);
    }catch(e){ log(`      fail page ${pageNum}: ${e.message||e}`); }
  }
  return {saved:savedCount};
}
async function processMangaDex(Page, Runtime, Network, link, channelFolderAbs){
  let phase = 'meta';
  try{
    // сначала мета на странице тайтла (до ухода в reader)
    await goto(Page, Runtime, link);
    const metaTitle = await scrapeDexMetaDom(Runtime);
    const folderName = sanitizeName(metaTitle.mainTitle || metaTitle.altTitle || 'manga');
    const outFolder  = path.join(channelFolderAbs, 'manga', folderName);
    ensureDir(outFolder);
    const unified = toUnifiedMeta({
      names:[metaTitle.mainTitle, metaTitle.altTitle],
      descriptions:[metaTitle.description],
      keywords:[...(metaTitle.tags||[])],
      mainUrl: link
    });
    writeJSON(path.join(outFolder,'mangaMeta.json'), { mangaMeta: unified });
    log(`  meta saved: ${path.join(outFolder,'mangaMeta.json')}`);

    // переход в reader
    phase = 'open-reader';
    await clickDexRead(Page, Runtime);
    if (await waitForSelector(Runtime, '.md-modal__box', 2000)){
      await chooseDexGroupByLang(Runtime);
    }
    await waitForSelector(Runtime, '.md--reader-pages img, #chapter-selector', 15000);

    // главы
    phase = 'chapters';
    const chapters = await collectDexChapterLinks(Runtime);
    if (!chapters.length){ log('  no chapters found via DOM'); return true; }
    const ordered = chapters.slice().reverse();
    for (const ch of ordered){
      const chLabel = ch.label || ch.id || 'chapter';
      log(`  -> ${chLabel} (${ch.href})`);
      const chDir = path.join(outFolder, '0', String(chLabel).replace(/\s+/g,'_').replace(/\./g,'_'));
      await saveDexChapterDom(Page, Runtime, Network, ch, chDir);
    }
    return true;
  }catch(e){
    log(`  [dex-dom] fail at phase=${phase}: ${e.message||e}`);
    throw e;
  }
}

// -------- router --------
// -------- router --------
// -------- router --------
async function handleLink(tab, link, channelFolderAbs){
  const {Page,Runtime,Network} = tab;
  try{
    const u=new URL(link);
    const host=normHost(u.hostname);
    const isLib = MANGALIB_HOSTS.some(h=>host.endsWith(h));
    const isDex = MANGADEX_HOSTS.some(h=>host.endsWith(h));
    const isBuf = MANGABUFF_HOSTS.some(h=>host.endsWith(h));
    const isWeb = WEBTOONS_HOSTS.some(h=>host.endsWith(h));

    if (isLib){
      log(`\n=== MANGALIB: ${link}`);
      await goto(Page,Runtime,link);
      const meta=await scrapeMangalibMeta(Runtime);
      const slug=await getPageSlug(Runtime);
      const folderName=sanitizeName(meta.ruName || meta.enName || slug || 'manga');
      const outFolder=path.join(channelFolderAbs,'manga',folderName);
      ensureDir(outFolder);
      const unified = toUnifiedMeta({
        names:[meta.ruName, meta.enName, slug],
        descriptions:[meta.ruDescription, meta.enDescription],
        keywords:[...(meta.ruKeywords||[]), ...(meta.enKeywords||[])],
        mainUrl: link
      });
      writeJSON(path.join(outFolder,'mangaMeta.json'),{mangaMeta:unified});
      log(`  ✓ meta записан: ${path.join(outFolder,'mangaMeta.json')}`);

      const chapters=await collectMangalibChapters(Page,Runtime);
      if (!chapters.length){
        log('  ⚠️ Главы не найдены (0). Возможно, нужен логин/18+ или иная разметка.');
        return true;
      }

      for (const ch of chapters){
        log(`  → ${ch.label || ('Глава '+ch.ch)} (${ch.href})`);
        const chDir=path.join(outFolder,String(ch.ch).replace(/\./g,'_'));
        await downloadMangalibChapter(Page,Runtime,Network,ch.href,chDir);
      }
      return true;
    }

    if (isBuf){
      return await processMangabuff(Page, Runtime, Network, link, channelFolderAbs);
    }

    if (isDex){
      log(`\n=== MANGADEX: ${link}`);
      await processMangaDex(Page,Runtime,Network,link,channelFolderAbs);
      return true;
    }

    if (isWeb){
      return await processWebtoons(Page, Runtime, Network, link, channelFolderAbs);
    }

    log('??? Неизвестный сайт:', host, link);
    return false;

  }catch(e){
    console.error('  ❌ Ошибка в handleLink:', e?.message || e);
    return false;
  }
}

// -------- main --------
let tab=null, browser=null;
(async()=>{
  if (!linksJsonPath || !channelFolder){
    console.error('Usage:\n  node mangaGrabber.js --linksJson="C:\\\\path\\\\links.json" --channelFolder="C:\\\\YT\\\\MyChannel" [--debug] [--rpm=10] [--stepDelay=0]');
    process.exit(1);
  }

  const {jpath,state}=await loadLinksState(linksJsonPath);
  if (!state.mangaLinks.length){ log('Нет ссылок в mangaLinks — выхожу.'); process.exit(0); }

  const channelFolderAbs=path.resolve(channelFolder);
  ensureDir(channelFolderAbs);

  ({browser,tab}=await openTab('about:blank'));
  const {Page,Runtime}=tab;

  try{
    const queue=state.mangaLinks.slice(0, limitLinks || state.mangaLinks.length);
    for (const link of queue){
      const ok=await handleLink(tab, link, channelFolderAbs);
      if (ok){
        const i=state.mangaLinks.indexOf(link); if (i>=0) state.mangaLinks.splice(i,1);
        if (!state.used.includes(link)) state.used.push(link);
        saveLinksStateLocal(jpath,state);
        log('  ✓ Прогресс сохранён в linksJson');
      } else {
        log('  ↪️ Оставляю ссылку в списке (ошибка, повтор позже)');
      }
    }
    log('\nDONE.');
  }catch(e){
    console.error('❌ Глобальная ошибка:', e?.message || e);
    process.exitCode=1;
  }finally{
    try{ await tab.close(); }catch{}
    try{ await browser.close(); }catch{}
  }
})();

