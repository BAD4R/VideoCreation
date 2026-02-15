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
let NAV_TIMEOUT_MS = 20000;
const POLL_MS     = 200;

let DEBUG      = false;
let STEP_DELAY = 0;
const runtimeInputMap = new WeakMap();
const runtimeClientMap = new WeakMap();

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

const navTimeoutMs = Number(args.navTimeoutMs);
if (Number.isFinite(navTimeoutMs)) NAV_TIMEOUT_MS = navTimeoutMs;
const mangaInitWaitMs  = Number(args.mangaInitWaitMs);
const mangaPageWaitMs  = Number(args.mangaPageWaitMs);
const mangaParamWaitMs = Number(args.mangaParamWaitMs);
const MANGA_INIT_WAIT_MS  = Number.isFinite(mangaInitWaitMs) ? mangaInitWaitMs : 15000;
const MANGA_PAGE_WAIT_MS  = Number.isFinite(mangaPageWaitMs) ? mangaPageWaitMs : 12000;
const MANGA_PARAM_WAIT_MS = Number.isFinite(mangaParamWaitMs) ? mangaParamWaitMs : 15000;
const imgConcurrency = Number(args.imgConcurrency);
const IMG_CONCURRENCY = Number.isFinite(imgConcurrency) ? Math.max(1, Math.floor(imgConcurrency)) : 1;
const chapterConcurrency = Number(args.chapterConcurrency);
const CHAPTER_CONCURRENCY = Number.isFinite(chapterConcurrency) ? Math.max(1, Math.floor(chapterConcurrency)) : 1;

// smart load settings
const SMART_MAX_MS        = Number(args.smartMaxMs || 7000);
const SMART_STABLE_TICKS  = Number(args.smartStableTicks || 2);
const SMART_MIN_LOOPS     = Number(args.smartMinLoops || 1);
const SMART_SCROLL_STEPS  = Number(args.smartScrollSteps || 8);
const SMART_WINDOW_STEPS  = Number(args.smartWindowSteps || 5);
const SMART_STEP_MS       = Number(args.smartStepMs || 110);
const WEBTOONS_WAIT_MAX_MS = Number(args.webtoonsWaitMaxMs || SMART_MAX_MS);
const WEBTOONS_WAIT_STABLE_TICKS = Number(args.webtoonsWaitStableTicks || SMART_STABLE_TICKS);
const WEBTOONS_WAIT_POLL_MS = Number(args.webtoonsWaitPollMs || 250);
const WEBTOONS_CAPTURE = args.webtoonsCapture == null ? true : !isFalseish(args.webtoonsCapture);
const WEBTOONS_CAPTURE_MAX_MS = Number(args.webtoonsCaptureMaxMs || Math.max(WEBTOONS_WAIT_MAX_MS, 30000));
const WEBTOONS_CAPTURE_POLL_MS = Number(args.webtoonsCapturePollMs || WEBTOONS_WAIT_POLL_MS);
const CAPTURE_DOWNLOADS = args.captureDownloads == null ? true : !isFalseish(args.captureDownloads);
const CAPTURE_MAX_MS = Number(args.captureMaxMs || Math.max(SMART_MAX_MS, 20000));
const CAPTURE_POLL_MS = Number(args.capturePollMs || 250);

const NAV_EXTRA_MS        = Number(args.navExtraMs || 350);
const NAV_TICKS           = Number(args.navTicks || 2);

function isFalseish(v){
  const s = String(v||'').trim().toLowerCase();
  return s === 'false' || s === '0' || s === 'no';
}
const SURVEY_WAIT = args.surveyWait == null ? true : !isFalseish(args.surveyWait);
const SURVEY_WAIT_POLL_MS = Number(args.surveyWaitPollMs || 1500);
const SURVEY_CLICK_POLL_MS = Number(args.surveyClickPollMs || 1000);
const SURVEY_CHECK_PATTERNS = [
  /verify you are human/i,
  /verify you are human by completing the action below/i,
  /verifying you are human/i,
  /this may take a few seconds/i
];
const IMG_WAIT = args.imgWait == null ? true : !isFalseish(args.imgWait);
const IMG_WAIT_POLL_MS = Number(args.imgWaitPollMs || 1500);
const IMG_WAIT_MAX_MS = Number(args.imgWaitMaxMs || 0);
const REQUIRE_COMPLETE_DOWNLOADS = args.requireCompleteDownloads == null ? true : !isFalseish(args.requireCompleteDownloads);
const DOWNLOAD_ROUNDS = Number.isFinite(Number(args.downloadRounds))
  ? Math.max(0, Math.floor(Number(args.downloadRounds)))
  : 3;
const DOWNLOAD_ROUND_DELAY_MS = Number.isFinite(Number(args.downloadRoundDelayMs))
  ? Math.max(0, Math.floor(Number(args.downloadRoundDelayMs)))
  : 1200;
const FETCH_TIMEOUT_MS = Number.isFinite(Number(args.fetchTimeoutMs))
  ? Math.max(1000, Math.floor(Number(args.fetchTimeoutMs)))
  : 30000;
const FETCH_RETRIES = Number.isFinite(Number(args.fetchRetries))
  ? Math.max(0, Math.floor(Number(args.fetchRetries)))
  : 2;
const FETCH_RETRY_DELAY_MS = Number.isFinite(Number(args.fetchRetryDelayMs))
  ? Math.max(0, Math.floor(Number(args.fetchRetryDelayMs)))
  : 800;

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
  if (tab && tab.Input) runtimeInputMap.set(Runtime, tab.Input);
  runtimeClientMap.set(Runtime, tab);
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

async function goto(Page, Runtime, url, timeout = NAV_TIMEOUT_MS){
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
  await waitForSurveyCheck(Runtime, url);
}

async function waitForSelector(Runtime,sel,timeout=NAV_TIMEOUT_MS,poll=POLL_MS){
  const t0=Date.now();
  while (Date.now()-t0<timeout){
    const {result}=await Runtime.evaluate({expression:`!!document.querySelector(${JSON.stringify(sel)})`,returnByValue:true,awaitPromise:true});
    if (result?.value) return true;
    await sleep(poll);
  }
  return false;
}

async function waitForImageReady(Runtime, sel, timeoutMs){
  const timeout = Number.isFinite(Number(timeoutMs)) ? Number(timeoutMs) : NAV_TIMEOUT_MS;
  const expr = `
    (function(){
      const sel = ${JSON.stringify(sel)};
      const timeout = ${Number(timeout)};
      return new Promise(resolve=>{
        let done = false;
        let timer = null;
        let poll = null;
        let mo = null;
        const start = Date.now();

        function cleanup(){
          if (timer) clearTimeout(timer);
          if (poll) clearInterval(poll);
          if (mo) mo.disconnect();
        }
        function finish(ok){
          if (done) return;
          done = true;
          cleanup();
          resolve(!!ok);
        }
        function isReady(){
          const img = document.querySelector(sel);
          if (!img) return false;
          const src = img.currentSrc || img.src || '';
          if (!src) return false;
          return !!(img.complete && img.naturalWidth > 0);
        }
        function tick(){
          if (isReady()) return finish(true);
          if (timeout && (Date.now() - start) >= timeout) return finish(false);
        }

        mo = new MutationObserver(tick);
        mo.observe(document.documentElement, {subtree:true, childList:true, attributes:true, attributeFilter:['src','srcset','data-src']});
        poll = setInterval(tick, 100);
        if (timeout) timer = setTimeout(()=>finish(false), timeout);
        tick();
      });
    })()
  `;
  return await evalInPage(Runtime, expr, true);
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

async function waitForSurveyCheck(Runtime, label=''){
  if (!SURVEY_WAIT) return false;
  let warned = false;
  let lastClickAt = 0;
  const Input = runtimeInputMap.get(Runtime);
  while (true){
    let info;
    try{
      info = await evalInPage(Runtime, `
        (function(){
          function isVisible(el){
            if (!el) return false;
            const style = window.getComputedStyle(el);
            if (!style || style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0;
          }
          function hasImageCandidate(){
            const selectors = [
              '.reading-content img',
              'img.wp-manga-chapter-img',
              '.reader__pages img',
              '.md--reader-pages img',
              '#_imageList img',
              '.viewer_lst img',
              'img._images'
            ];
            function pickSrc(img){
              const s = img.getAttribute('data-src') ||
                img.getAttribute('data-original') ||
                img.getAttribute('data-lazy-src') ||
                img.getAttribute('data-url') ||
                img.currentSrc || img.src || '';
              return (s||'').trim();
            }
            for (const sel of selectors){
              const imgs = document.querySelectorAll(sel);
              for (const img of imgs){
                const s = pickSrc(img);
                if (!s) continue;
                if (/\\.(png|jpe?g|webp)(\\?|$)/i.test(s)) return true;
              }
            }
            return false;
          }
          const title = (document.title || '').toLowerCase();
          const bodyText = (document.body && document.body.innerText || '').toLowerCase();
          const hasTurnstile = [...document.querySelectorAll('input[name="cf-turnstile-response"], iframe[src*="turnstile"], .cf-turnstile')]
            .some(isVisible);
          const hasCfChallenge = [...document.querySelectorAll('iframe[src*="challenges.cloudflare.com"], #cf-challenge, .cf-browser-verification')]
            .some(isVisible);
          const hasHcaptcha = [...document.querySelectorAll('iframe[src*="hcaptcha"], .h-captcha')]
            .some(isVisible);
          const hasRecaptcha = [...document.querySelectorAll('iframe[src*="recaptcha"], .g-recaptcha')]
            .some(isVisible);
          const hasReaderContent =
            hasImageCandidate() ||
            !!document.querySelector('select.single-chapter-select option') ||
            !!document.querySelector('#chapter-selector, .md--reader-pages') ||
            !!document.querySelector('.reader__pages') ||
            !!document.querySelector('.vc_be, [data-page]') ||
            !!document.querySelector('#_imageList, .viewer_lst');
          return {
            title,
            bodyText: bodyText.slice(0, 5000),
            hasWidget: !!(hasTurnstile || hasCfChallenge || hasHcaptcha || hasRecaptcha),
            hasReaderContent
          };
        })()
      `, true);
    }catch(e){
      dbg('survey-check detect error:', e.message || e);
      return warned;
    }

    const hay = `${info?.title || ''}\n${info?.bodyText || ''}`;
    const matched = !!info?.hasWidget || SURVEY_CHECK_PATTERNS.some(re=>re.test(hay));
    if (!matched || info?.hasReaderContent) return warned;

    if (!warned){
      const hint = label ? ` (${label})` : '';
      log(`  reading interest survey detected${hint}. waiting for it to clear...`);
      warned = true;
    }
    if (SURVEY_CLICK_POLL_MS > 0){
      const now = Date.now();
      if (!lastClickAt || (now - lastClickAt) >= SURVEY_CLICK_POLL_MS){
        try{
          const clickInfo = await evalInPage(Runtime, `
            (function(){
              function isVisible(el){
                if (!el) return false;
                const style = window.getComputedStyle(el);
                if (!style || style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                const r = el.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
              }
              function isChecked(el){
                if (!el) return false;
                if (typeof el.checked === 'boolean') return !!el.checked;
                const aria = (el.getAttribute('aria-checked') || '').toLowerCase();
                return aria === 'true';
              }
              function clampPoint(x, y){
                const maxX = Math.max(1, window.innerWidth || 1) - 1;
                const maxY = Math.max(1, window.innerHeight || 1) - 1;
                return {
                  x: Math.max(0, Math.min(maxX, x)),
                  y: Math.max(0, Math.min(maxY, y))
                };
              }
              function pointFor(el){
                const r = el.getBoundingClientRect();
                if (!r.width || !r.height) return null;
                return clampPoint(r.left + r.width / 2, r.top + r.height / 2);
              }
              function findCheckbox(){
                const boxes = [...document.querySelectorAll('input[type="checkbox"], [role="checkbox"]')].filter(isVisible);
                if (!boxes.length) return null;
                boxes.sort((a,b)=> (isChecked(a)?1:0) - (isChecked(b)?1:0));
                const target = boxes[0];
                let clickEl = target;
                const id = target.getAttribute('id');
                if (id){
                  const label = document.querySelector('label[for="' + id.replace(/"/g,'\\"') + '"]');
                  if (label && isVisible(label)) clickEl = label;
                }
                if (clickEl && clickEl.closest) clickEl = clickEl.closest('label,[role="checkbox"],input[type="checkbox"]') || clickEl;
                return {el: clickEl, kind:'checkbox'};
              }
              function findIframe(){
                const iframes = [...document.querySelectorAll('iframe')].filter(isVisible);
                if (!iframes.length) return null;
                const candidates = iframes.filter(f=>{
                  const src = (f.getAttribute('src') || '').toLowerCase();
                  const title = (f.getAttribute('title') || '').toLowerCase();
                  return src.includes('turnstile') ||
                    src.includes('challenges.cloudflare') ||
                    src.includes('hcaptcha') ||
                    src.includes('recaptcha') ||
                    title.includes('captcha') ||
                    title.includes('challenge');
                });
                const pick = candidates[0] || iframes[0];
                return {el: pick, kind:'iframe'};
              }
              const target = findCheckbox() || findIframe();
              if (!target || !target.el) return {didDomClick:false, clickPoint:null, kind:''};
              try{ target.el.scrollIntoView({block:'center', inline:'center'}); }catch{}
              const clickPoint = pointFor(target.el);
              let didDomClick = false;
              if (target.kind !== 'iframe'){
                try{ target.el.click(); didDomClick = true; }catch{}
              }
              return {didDomClick, clickPoint, kind: target.kind};
            })()
          `, true);
          let didClick = !!clickInfo?.didDomClick;
          if (clickInfo?.clickPoint && Input && (clickInfo?.kind === 'iframe' || !didClick)){
            const x = Number(clickInfo.clickPoint.x);
            const y = Number(clickInfo.clickPoint.y);
            if (Number.isFinite(x) && Number.isFinite(y)){
              try{
                await Input.dispatchMouseEvent({type:'mouseMoved', x, y, button:'left', clickCount:1});
                await Input.dispatchMouseEvent({type:'mousePressed', x, y, button:'left', clickCount:1, buttons:1});
                await Input.dispatchMouseEvent({type:'mouseReleased', x, y, button:'left', clickCount:1, buttons:0});
                didClick = true;
              }catch(e){
                dbg('survey-check input click error:', e.message || e);
              }
            }
          }
          if (didClick) lastClickAt = now;
        }catch(e){
          dbg('survey-check click error:', e.message || e);
          lastClickAt = now;
        }
      }
    }
    const clickMs = Number.isFinite(SURVEY_CLICK_POLL_MS) && SURVEY_CLICK_POLL_MS > 0 ? SURVEY_CLICK_POLL_MS : null;
    const loopSleepMs = clickMs ? Math.min(SURVEY_WAIT_POLL_MS, clickMs) : SURVEY_WAIT_POLL_MS;
    await sleep(loopSleepMs);
  }
}

async function waitForImages(Runtime, collectFn, label=''){
  if (!IMG_WAIT) return await collectFn(Runtime);
  let warned = false;
  const start = Date.now();
  while (true){
    const urls = await collectFn(Runtime);
    if (urls && urls.length) return urls;
    if (!warned){
      const hint = label ? ` (${label})` : '';
      log(`    waiting for images to load${hint}...`);
      warned = true;
    }
    if (IMG_WAIT_MAX_MS > 0 && (Date.now() - start) >= IMG_WAIT_MAX_MS) return urls;
    await sleep(IMG_WAIT_POLL_MS);
  }
}

// -------- site helpers --------
const MANGALIB_HOSTS = ['mangalib.org','mangalib.me','mixlib.me'];
const MANGADEX_HOSTS = ['mangadex.org','api.mangadex.org'];
const MANGABUFF_HOSTS= ['mangabuff.ru'];
const MANHWACLAN_HOSTS = ['manhwaclan.com'];
const MANHUAUS_HOSTS = ['manhuaus.com'];
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
function normalizeWebtoonsUrl(url){
  const s = (url||'').toString();
  return s.replace(/[?#].*$/, '');
}
function normalizeCaptureUrl(url){
  const s = (url||'').toString();
  return s.replace(/#.*$/, '');
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

async function getMangalibPageCount(Runtime){
  return await evalInPage(Runtime, `
    (function(){
      const sel =
        document.querySelector('footer select.form-input__field') ||
        document.querySelector('label select.form-input__field')  ||
        document.querySelector('select.form-input__field');
      if (sel){
        const values = [...sel.options].map(o => Number(o.value) || 0).filter(Boolean);
        return values.length || 0;
      }
      const nodes = [...document.querySelectorAll('[data-page]')];
      const nums = nodes.map(n => Number(n.getAttribute('data-page') || '0') || 0).filter(Boolean);
      return nums.length || 0;
    })()
  `);
}

async function collectMangalibPageUrlsFromState(Runtime){
  const res = await evalInPage(Runtime, `
    (function(){
      function isImg(s){ return /\\.(png|jpe?g|webp)(\\?|$)/i.test(s||''); }
      function abs(u, base){
        try{ return new URL(u, base || location.origin).href; }catch{ return u||''; }
      }
      function pickPages(obj){
        if (!obj || typeof obj !== 'object') return {pages:null, base:''};
        const pages = Array.isArray(obj.pages) ? obj.pages
          : Array.isArray(obj.images) ? obj.images
          : Array.isArray(obj.page) ? obj.page
          : Array.isArray(obj.items) ? obj.items
          : null;
        const base = obj.domain || obj.host || obj.baseUrl || obj.cdn || '';
        return {pages, base};
      }

      const nuxt = window.__NUXT__ || window.__nuxt__ || null;
      let pages = null;
      let base = '';

      if (nuxt && nuxt.state){
        const reader = nuxt.state.reader || nuxt.state.Reader || null;
        if (reader){
          const picked = pickPages(reader);
          if (picked.pages && picked.pages.length){
            pages = picked.pages;
            base = picked.base || base;
          }
        }
        if (!pages){
          for (const k of Object.keys(nuxt.state)){
            const picked = pickPages(nuxt.state[k]);
            if (picked.pages && picked.pages.length){
              pages = picked.pages;
              base = picked.base || base;
              break;
            }
          }
        }
      }

      if (!pages || !pages.length) return {ok:false};
      const urls = [];
      for (const item of pages){
        let u = '';
        if (typeof item === 'string') u = item;
        else if (item && typeof item === 'object'){
          u = item.url || item.src || item.path || item.u || item.s || item.image || '';
          if (!u){
            for (const v of Object.values(item)){
              if (typeof v === 'string' && isImg(v)){ u = v; break; }
            }
          }
        }
        if (!u) continue;
        urls.push(abs(u, base || location.origin));
      }

      const out = [];
      const seen = new Set();
      for (const u of urls){
        const key = (u||'').toLowerCase();
        if (!key) continue;
        if (seen.has(key)) continue;
        if (!isImg(u)) continue;
        seen.add(key);
        out.push(u);
      }
      return {ok: out.length>0, urls: out, total: pages.length, base};
    })()
  `);
  if (res && res.ok && Array.isArray(res.urls)) return res;
  return {urls:[], total:0};
}

async function downloadMangalibFromState(Runtime, Network, chapterUrl, saveDir, expectedTotal){
  const state = await collectMangalibPageUrlsFromState(Runtime);
  if (!state.urls || !state.urls.length) return {saved:0, reason:'no-state'};
  if (expectedTotal && state.urls.length !== expectedTotal){
    dbg('  [state] url count mismatch', state.urls.length, expectedTotal);
    return {saved:0, reason:'count-mismatch'};
  }
  log(`    fast: state urls = ${state.urls.length}`);
  const res = await downloadUrlList(Network, state.urls, saveDir, chapterUrl, {
    concurrency: IMG_CONCURRENCY,
    label: 'mangalib-state',
    runtime: Runtime
  });
  return res;
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
const IMG_EXTS = ['.png','.jpg','.jpeg','.webp','.gif'];
function looksLikeImageUrl(url){
  return /\.(png|jpe?g|webp|gif|svg)(\?|$)/i.test(url||'');
}
function existingImagesInfo(dir){
  if (!fs.existsSync(dir)) return {count:0, maxIndex:0};
  const files = fs.readdirSync(dir).filter(f=>/^\d{3}\.(png|jpe?g|webp|gif)$/i.test(f));
  const nums = files.map(f=>parseInt(f.split('.')[0],10)).filter(n=>Number.isFinite(n));
  const maxIndex = nums.length ? Math.max(...nums) : 0;
  return {count:files.length, maxIndex};
}
function pageFileExists(dir, idx){
  const pad = String(idx).padStart(3,'0');
  for (const ext of IMG_EXTS){
    const file = path.join(dir, pad + ext.replace('jpeg','jpg'));
    if (fs.existsSync(file)) return file;
  }
  return '';
}
function looksLikePageCdn(url){
  return /(?:mixlib|mangalib|img\d?\.)/i.test(url||'') && /\.(png|jpe?g|webp)(?:\?|$)/i.test(url||'');
}
function looksLikeBuffCdn(url){
  return /(mangabuff\.ru|^https?:\/\/c\d+\.)/i.test(url||'') && /\.(png|jpe?g|webp)(?:\?|$)/i.test(url||'');
}
async function mapConcurrent(items, concurrency, worker){
  const total = items.length;
  if (!total) return [];
  const limit = Math.max(1, Math.floor(Number(concurrency) || 1));
  let index = 0;
  const results = new Array(total);
  async function run(){
    while (true){
      const i = index++;
      if (i >= total) break;
      results[i] = await worker(items[i], i);
    }
  }
  const runners = [];
  for (let i=0;i<limit;i++) runners.push(run());
  await Promise.all(runners);
  return results;
}

async function getCookieHeader(Network, urlStr){
  try{
    const {cookies} = await Network.getCookies({urls:[urlStr]});
    if (!cookies || !cookies.length) return '';
    return cookies.map(c=>`${c.name}=${c.value}`).join('; ');
  }catch{ return ''; }
}
function isRetryableFetchError(err){
  const msg = (err && err.message) ? String(err.message) : String(err||'');
  if (err && err.name === 'AggregateError') return true;
  if (/HTTP 429|HTTP 5\d\d/.test(msg)) return true;
  return /ECONNRESET|ETIMEDOUT|EAI_AGAIN|ENOTFOUND|socket hang up|timeout/i.test(msg);
}
function fetchBufferWithHeadersOnce(url, headers, timeoutMs, redirectDepth=0){
  const lib = url.startsWith('https') ? https : http;
  return new Promise((resolve,reject)=>{
    let settled = false;
    let timer = null;
    const finish = (fn, value)=>{
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      fn(value);
    };
    const req = lib.request(url, {method:'GET', headers}, res=>{
      if (res.statusCode>=300 && res.statusCode<400 && res.headers.location){
        res.resume();
        if (redirectDepth >= 5){
          return finish(reject, new Error('HTTP redirect loop'));
        }
        const red = new URL(res.headers.location, url).href;
        return finish(resolve, fetchBufferWithHeadersOnce(red, headers, timeoutMs, redirectDepth+1));
      }
      if (res.statusCode!==200){
        res.resume();
        return finish(reject, new Error('HTTP '+res.statusCode));
      }
      const chunks=[];
      res.on('data',d=>chunks.push(d));
      res.on('end',()=>finish(resolve, {buf:Buffer.concat(chunks), mime:res.headers['content-type']||''}));
      res.on('error',err=>finish(reject, err));
    });
    timer = setTimeout(()=>{ req.destroy(new Error('timeout')); }, timeoutMs);
    req.on('error',err=>finish(reject, err));
    req.end();
  });
}
async function fetchBufferWithHeaders(url, headers, opts={}){
  const timeoutMs = Number.isFinite(Number(opts.timeoutMs)) ? Math.max(1000, Math.floor(Number(opts.timeoutMs))) : FETCH_TIMEOUT_MS;
  const retries = Number.isFinite(Number(opts.retries)) ? Math.max(0, Math.floor(Number(opts.retries))) : FETCH_RETRIES;
  const retryDelayMs = Number.isFinite(Number(opts.retryDelayMs)) ? Math.max(0, Math.floor(Number(opts.retryDelayMs))) : FETCH_RETRY_DELAY_MS;

  let attempt = 0;
  while (true){
    try{
      return await fetchBufferWithHeadersOnce(url, headers, timeoutMs);
    }catch(e){
      if (attempt >= retries || !isRetryableFetchError(e)) throw e;
      attempt++;
      await sleep(retryDelayMs * attempt);
    }
  }
}

function findMissingEntries(saveDir, entries){
  const out = [];
  for (const ent of entries){
    if (!pageFileExists(saveDir, ent.pageNum)) out.push(ent);
  }
  return out;
}

function buildEntriesFromUrls(urls, filterUrl){
  const out = [];
  const seen = new Set();
  for (const u of (urls||[])){
    const s = (u||'').toString().trim();
    if (!s) continue;
    const key = s.toLowerCase();
    if (seen.has(key)) continue;
    if (filterUrl && !filterUrl(s)) continue;
    if (!filterUrl && !looksLikeImageUrl(s)) continue;
    seen.add(key);
    out.push({url: s, pageNum: out.length + 1});
  }
  return out;
}

function buildEntriesFromExplicit(entries, filterUrl){
  const out = [];
  const seenPages = new Set();
  for (const ent of (entries||[])){
    if (!ent) continue;
    const s = (ent.url||'').toString().trim();
    if (!s) continue;
    const pageNum = Number(ent.pageNum);
    if (!Number.isFinite(pageNum) || pageNum <= 0) continue;
    if (seenPages.has(pageNum)) continue;
    if (filterUrl && !filterUrl(s)) continue;
    if (!filterUrl && !looksLikeImageUrl(s)) continue;
    seenPages.add(pageNum);
    out.push({url: s, pageNum});
  }
  return out;
}

async function forceImagePreloads(Runtime, urls){
  const list = Array.isArray(urls) ? urls.filter(Boolean) : [];
  if (!list.length) return {total:0};
  return await evalInPage(Runtime, `
    (function(urls){
      const preloads = [];
      for (const u of urls){
        if (!u) continue;
        const im = new Image();
        im.loading = 'eager';
        im.decoding = 'async';
        im.src = u;
        preloads.push(im);
      }
      window.__codexImagePreloads = preloads;
      return {total: preloads.length};
    })(${JSON.stringify(list)})
  `, true);
}

async function downloadUrlListViaCdp(Runtime, Network, urls, saveDir, referer, opts={}){
  const client = runtimeClientMap.get(Runtime);
  if (!client){
    return await downloadUrlListDirect(Network, urls, saveDir, referer, opts);
  }
  const {
    filterUrl=null,
    label='',
    requireComplete=REQUIRE_COMPLETE_DOWNLOADS,
    maxRounds=DOWNLOAD_ROUNDS,
    roundDelayMs=DOWNLOAD_ROUND_DELAY_MS,
    captureMaxMs=CAPTURE_MAX_MS,
    capturePollMs=CAPTURE_POLL_MS,
    entries=null,
    normalizeUrl=null
  } = opts || {};

  const list = entries ? buildEntriesFromExplicit(entries, filterUrl) : buildEntriesFromUrls(urls, filterUrl);
  if (!list.length) return {saved:0, total:0, reason:'no-urls', complete:true};
  ensureDir(saveDir);

  const savedPages = new Set();
  for (const ent of list){
    if (pageFileExists(saveDir, ent.pageNum)) savedPages.add(ent.pageNum);
  }

  const exactMap = new Map();
  for (const ent of list){
    const key = ent.url;
    if (!exactMap.has(key)) exactMap.set(key, []);
    exactMap.get(key).push(ent);
  }

  const normFn = typeof normalizeUrl === 'function' ? normalizeUrl : normalizeCaptureUrl;
  const normMap = new Map();
  for (const ent of list){
    const norm = normFn(ent.url);
    if (!norm) continue;
    if (!normMap.has(norm)) normMap.set(norm, ent.url);
    else normMap.set(norm, null);
  }

  function entriesForUrl(url){
    if (!url) return null;
    const exact = exactMap.get(url);
    if (exact && exact.length) return exact;
    const norm = normFn(url);
    const key = normMap.get(norm);
    if (key) return exactMap.get(key) || null;
    return null;
  }

  let savedNew = 0;
  let lastProgressAt = Date.now();
  const inFlight = new Map();

  const onResponse = params=>{
    const resp = params && params.response;
    if (!resp || !resp.url) return;
    if (params.type && params.type !== 'Image'){
      if (!resp.mimeType || !resp.mimeType.startsWith('image/')) return;
    }
    const listFor = entriesForUrl(resp.url);
    if (!listFor) return;
    if (!inFlight.has(params.requestId)){
      inFlight.set(params.requestId, {url: resp.url, mimeType: resp.mimeType || ''});
    }
  };

  const onFinished = async params=>{
    const info = inFlight.get(params.requestId);
    if (!info) return;
    inFlight.delete(params.requestId);
    const listFor = entriesForUrl(info.url);
    if (!listFor || !listFor.length) return;
    try{
      const body = await Network.getResponseBody({requestId: params.requestId});
      const buf = body && body.base64Encoded ? Buffer.from(body.body || '', 'base64')
        : Buffer.from((body && body.body) || '', 'utf8');
      if (!buf.length) return;
      const ext = extFromMimeOrUrl(info.mimeType, info.url);
      if (ext==='.gif' || ext==='.svg') return;
      for (const ent of listFor){
        if (savedPages.has(ent.pageNum)) continue;
        const file = path.join(saveDir, String(ent.pageNum).padStart(3,'0') + ext);
        if (fs.existsSync(file)){ savedPages.add(ent.pageNum); continue; }
        await fs.promises.writeFile(file, buf);
        log(`      saved ${path.basename(file)}`);
        savedPages.add(ent.pageNum);
        savedNew++;
        lastProgressAt = Date.now();
        if (STEP_DELAY) await sleep(STEP_DELAY);
      }
    }catch(e){
      dbg('capture error:', e.message || e);
    }
  };

  const onFailed = params=>{
    if (params && params.requestId) inFlight.delete(params.requestId);
  };

  client.on('Network.responseReceived', onResponse);
  client.on('Network.loadingFinished', onFinished);
  client.on('Network.loadingFailed', onFailed);

  try{
    try{ await Network.setCacheDisabled({cacheDisabled:true}); }catch{}
    const roundsLimit = maxRounds === 0 ? Infinity : Math.max(1, Math.floor(maxRounds));
    let round = 0;
    while (round < roundsLimit){
      const missing = findMissingEntries(saveDir, list);
      if (!missing.length) break;
      round++;
      if (round > 1){
        log(`      retrying ${missing.length} missing pages (round ${round}/${roundsLimit === Infinity ? 'inf' : roundsLimit})`);
      }
      await forceImagePreloads(Runtime, missing.map(m=>m.url));

      const start = Date.now();
      let lastSavedCount = savedPages.size;
      while (Date.now() - start < captureMaxMs){
        const remain = findMissingEntries(saveDir, list);
        if (!remain.length) break;
        if (savedPages.size !== lastSavedCount){
          lastSavedCount = savedPages.size;
        }
        if (Date.now() - lastProgressAt > captureMaxMs) break;
        await sleep(capturePollMs);
      }
      if (round < roundsLimit && roundDelayMs) await sleep(roundDelayMs);
    }
  } finally {
    client.removeListener('Network.responseReceived', onResponse);
    client.removeListener('Network.loadingFinished', onFinished);
    client.removeListener('Network.loadingFailed', onFailed);
    try{ await Network.setCacheDisabled({cacheDisabled:false}); }catch{}
  }

  const remaining = findMissingEntries(saveDir, list);
  const complete = remaining.length === 0;
  const saved = list.length - remaining.length;
  if (!complete && requireComplete){
    throw new Error(`download incomplete${label ? ' ('+label+')' : ''}: ${saved}/${list.length}`);
  }
  if (label) dbg(`[downloadUrlList:${label}] total=${list.length} savedNew=${savedNew} missing=${remaining.length}`);
  return {saved, total: list.length, complete, missing: remaining.length};
}

async function downloadUrlList(Network, urls, saveDir, referer, opts={}){
  const useCapture = CAPTURE_DOWNLOADS && opts && opts.runtime && !opts.forceDirect;
  if (useCapture){
    try{
      return await downloadUrlListViaCdp(opts.runtime, Network, urls, saveDir, referer, opts);
    }catch(e){
      if (opts.fallbackToDirect === false || opts.requireComplete) throw e;
      dbg('capture failed, fallback to direct:', e.message || e);
    }
  }
  return await downloadUrlListDirect(Network, urls, saveDir, referer, opts);
}

async function downloadUrlListDirect(Network, urls, saveDir, referer, opts={}){
  const {
    concurrency=1,
    filterUrl=null,
    label='',
    useLimiter=true,
    requireComplete=REQUIRE_COMPLETE_DOWNLOADS,
    maxRounds=DOWNLOAD_ROUNDS,
    roundDelayMs=DOWNLOAD_ROUND_DELAY_MS,
    entries=null
  } = opts || {};
  const list = entries ? buildEntriesFromExplicit(entries, filterUrl) : buildEntriesFromUrls(urls, filterUrl);
  if (!list.length) return {saved:0, total:0, reason:'no-urls', complete:true};
  ensureDir(saveDir);

  let savedNew = 0;
  const accept = 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8';
  const roundsLimit = maxRounds === 0 ? Infinity : Math.max(1, Math.floor(maxRounds));
  let round = 0;
  let missing = findMissingEntries(saveDir, list);

  while (missing.length && round < roundsLimit){
    round++;
    if (round > 1 && missing.length){
      log(`      retrying ${missing.length} missing pages (round ${round}/${roundsLimit === Infinity ? 'inf' : roundsLimit})`);
    }
    await mapConcurrent(missing, concurrency, async ({url, pageNum}) => {
      if (pageFileExists(saveDir, pageNum)) return;
      if (useLimiter) await limiter.hit('fetch-img');

      const cookie = await getCookieHeader(Network, url);
      const headers = {
        'User-Agent': DEFAULT_UA,
        'Referer': referer,
        'Accept': accept,
        'Accept-Language': 'ru,en;q=0.9',
        ...(cookie ? {'Cookie': cookie} : {})
      };

      try{
        const {buf, mime} = await fetchBufferWithHeaders(url, headers);
        const ext  = extFromMimeOrUrl(mime, url);
        if (ext==='.gif' || ext==='.svg'){ dbg('      skip loader', url); return; }
        const file = path.join(saveDir, String(pageNum).padStart(3,'0') + ext);
        await fs.promises.writeFile(file, buf);
        savedNew++;
        log(`      saved ${path.basename(file)}`);
        if (STEP_DELAY) await sleep(STEP_DELAY);
      }catch(e){
        log(`      fail p=${pageNum}: ${e.message||e}`);
      }
    });
    missing = findMissingEntries(saveDir, list);
    if (missing.length && round < roundsLimit && roundDelayMs){
      await sleep(roundDelayMs);
    }
  }

  const complete = missing.length === 0;
  const saved = list.length - missing.length;
  if (!complete && requireComplete){
    throw new Error(`download incomplete${label ? ' ('+label+')' : ''}: ${saved}/${list.length}`);
  }
  if (label) dbg(`[downloadUrlList:${label}] total=${list.length} savedNew=${savedNew} missing=${missing.length} rounds=${round}`);
  return {saved, total: list.length, complete, missing: missing.length, rounds: round};
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

  const entries = [];
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
        const tgt = document.querySelector('[data-page="'+pageNum+'"]');
        if (tgt){ tgt.scrollIntoView({block:'center'}); }
        return true;
      })(${JSON.stringify(p)})
    `);

    const ok = await waitForImageReady(Runtime, `[data-page="${p}"] img`, MANGA_PAGE_WAIT_MS);
    if (!ok){ log(`      ?????? p=${p}: img ???? ????????????????`); continue; }

    const imgUrl = await evalInPage(Runtime,`
      (function(pageNum){
        const img = document.querySelector('[data-page="'+pageNum+'"] img');
        return img ? (img.currentSrc || img.src || '') : '';
      })(${JSON.stringify(p)})
    `);
    if (!looksLikePageCdn(imgUrl)){ log(`      ?????? p=${p}: bad url (${imgUrl||'none'})`); continue; }

    const pageNum = Number(p) || (entries.length + 1);
    entries.push({url: imgUrl, pageNum});
  }

  if (!entries.length) return {saved:0, reason:'no-urls-dropdown'};
  const res = await downloadUrlList(Network, [], saveDir, chapterUrl, {
    concurrency: IMG_CONCURRENCY,
    filterUrl: looksLikePageCdn,
    label: 'mangalib-dropdown',
    entries,
    runtime: Runtime
  });
  return {saved: res.saved};
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

  const ordered = urls.sort((a,b)=>a.n-b.n).map(u=>u.url);
  const res = await downloadUrlList(Network, ordered, saveDir, chapterUrl, {
    concurrency: IMG_CONCURRENCY,
    filterUrl: looksLikePageCdn,
    label: 'mangalib-strip',
    runtime: Runtime
  });
  return {saved: res.saved};
}

// --- ?p= fallback (Mangalib) ---
async function downloadByParamPaging(Page, Runtime, Network, baseHref, saveDir){
  let misses=0; const MAX_MISSES=2; const HARD_LIMIT=800;
  const entries = [];
  let lastReferer = baseHref;

  for (let p=1; p<=HARD_LIMIT; p++){
    const url=new URL(baseHref, 'https://mangalib.org/'); url.searchParams.set('p', String(p));
    dbg('      ??? goto page', p, url.href);
    await goto(Page, Runtime, url.href);
    lastReferer = url.href;

    const ready = await waitForImageReady(Runtime, `.vc_be [data-page] img, [data-page="${p}"] img`, MANGA_PARAM_WAIT_MS);
    if (!ready){
      log(`      ?????? p=${p}: img ???? ????????????`);
      if (++misses>=MAX_MISSES){ log('      ??? ?????????????? ???? ???????? ???????????? ????????????????'); break; }
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
      log(`      ?????? p=${p}: bad url (${imgUrl||'none'})`);
      if (++misses>=MAX_MISSES){ log('      ??? ?????????????? ???? ???????? ???????????? ????????????????'); break; }
      continue;
    }

    entries.push({url: imgUrl, pageNum: p});
    misses=0;
  }

  if (!entries.length) return {saved:0};
  const res = await downloadUrlList(Network, [], saveDir, lastReferer || baseHref, {
    concurrency: IMG_CONCURRENCY,
    filterUrl: looksLikePageCdn,
    label: 'mangalib-param',
    entries,
    runtime: Runtime
  });
  return {saved: res.saved};
}

async function downloadMangalibChapter(Page, Runtime, Network, chapterHref, saveDir){
  const base = new URL(chapterHref, 'https://mangalib.org/');
  await goto(Page, Runtime, base.href);
  await sleep(STEP_DELAY);

  await waitForSelector(Runtime, '.vc_be, [data-page], select.form-input__field', MANGA_INIT_WAIT_MS);

  log('    ▶ скачивание страниц (dropdown → long-strip → ?p=)');
  ensureDir(saveDir);

  const expectedPages = await getMangalibPageCount(Runtime);
  const viaState = await downloadMangalibFromState(Runtime, Network, base.href, saveDir, expectedPages);
  if (viaState.saved>0){ log(`    fast saved: ${viaState.saved}`); return; }

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

async function collectMangabuffChapterLinks(Runtime, maxMs=SMART_MAX_MS){
  const start=Date.now(); let last=0, stable=0;
  async function countLinks(){
    const {result}=await Runtime.evaluate({expression:`document.querySelectorAll('a[href*="/manga/"]').length`,returnByValue:true});
    return Number(result?.value)||0;
  }
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
  do{
    await pump(); const c=await countLinks();
    if (c>last){ last=c; stable=0; } else stable++;
    if (stable>=SMART_STABLE_TICKS) break;
    if (Date.now()-start>maxMs) break;
    await sleep(120);
  }while(true);

  const res = await evalInPage(Runtime, `
    (function(){
      function abs(h){ try { return new URL(h, location.href).href; } catch(e){ return h||''; } }
      const re = /\/manga\/[^/]+\/\d+\/[\d.]+/i;
      const out = [];
      const seen = new Set();
      const as = [...document.querySelectorAll('a[href]')];
      for (const a of as){
        const href = abs(a.getAttribute('href')||'');
        if (!href || !re.test(href)) continue;
        const key = href.toLowerCase();
        if (seen.has(key)) continue;
        seen.add(key);
        out.push({href});
      }
      return out;
    })()
  `);
  return Array.isArray(res) ? res : [];
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

// -------- MANHWACLAN: meta, chapters, download --------
async function scrapeManhwaClanMeta(Runtime){
  return await evalInPage(Runtime, `
    (function(){
      function txt(n){return (n&&(n.textContent||'').replace(/\\s+/g,' ').trim())||'';}
      function abs(h){ try { return new URL(h, location.href).href; } catch(e){ return h||''; } }
      const title = txt(document.querySelector('.post-title h1, .post-title h2, h1'));
      const tagNodes = [...(document.querySelectorAll('.genres-content a, .post-content_item .genres-content a')||[])];
      const tags = tagNodes.map(n=>txt(n)).filter(Boolean);
      let desc = '';
      const descRoot = document.querySelector('.description-summary .summary__content') || document.querySelector('.description-summary');
      if (descRoot){
        const ps = [...(descRoot.querySelectorAll('p')||[])];
        if (ps.length){
          desc = ps.map(p=>txt(p)).filter(Boolean).join('\\n');
        } else {
          desc = txt(descRoot);
        }
      }
      let chapterUrl = '';
      const chapterLink = [...(document.querySelectorAll('a[href*="/chapter-"]')||[])].find(a=>{
        const h = a.getAttribute('href')||'';
        return /\\/chapter-\\d/i.test(h);
      });
      if (chapterLink) chapterUrl = abs(chapterLink.getAttribute('href')||'');
      return { title, description: desc, tags, chapterUrl };
    })()
  `);
}

async function collectManhwaClanChapters(Runtime){
  return await evalInPage(Runtime, `
    (function(){
      function txt(n){return (n&&(n.textContent||'').replace(/\\s+/g,' ').trim())||'';}
      function baseHref(){
        try{
          const u = new URL(location.href);
          const parts = u.pathname.split('/').filter(Boolean);
          const i = parts.findIndex(p => /^chapter-/.test(p));
          if (i >= 0) parts.splice(i, 1);
          u.pathname = '/' + parts.join('/') + '/';
          return u.href;
        }catch(e){ return location.href; }
      }
      const base = baseHref();
      const out = [];
      const opts = [...(document.querySelectorAll('select.single-chapter-select option')||[])];
      for (const opt of opts){
        const label = txt(opt);
        const raw = opt.getAttribute('data-redirect') || opt.getAttribute('data-href') || opt.getAttribute('value') || '';
        let href = '';
        if (raw){
          if (/^https?:\\/\\//i.test(raw)) href = raw;
          else {
            let v = raw.trim().replace(/^\\/+/, '');
            if (v && !/\\/$/.test(v)) v += '/';
            href = new URL(v, base).href;
          }
        }
        if (!href) continue;
        out.push({label, href, value: raw});
      }
      return out;
    })()
  `);
}

function parseManhwaClanChapterNum(label, href, value){
  function fromText(s){
    const m = (s||'').match(/chapter\s*([0-9]+(?:[.-][0-9]+)?)/i);
    if (!m) return null;
    return parseFloat(m[1].replace(/-/g,'.'));
  }
  function fromUrl(s){
    const m = (s||'').match(/chapter-([0-9]+(?:[.-][0-9]+)?)/i);
    if (!m) return null;
    return parseFloat(m[1].replace(/-/g,'.'));
  }
  return fromText(label) ?? fromUrl(value) ?? fromUrl(href);
}

async function smartLoadManhwaClan(Runtime, maxMs=SMART_MAX_MS){
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

async function collectManhwaClanPageUrls(Runtime){
  return await evalInPage(Runtime, `
    (function(){
      function abs(h){ try { return new URL(h, location.href).href; } catch(e){ return h||''; } }
      function pickSrc(img){
        const s = img.getAttribute('data-src') ||
          img.getAttribute('data-original') ||
          img.getAttribute('data-lazy-src') ||
          img.currentSrc || img.src || '';
        return (s||'').trim();
      }
      let imgs = [...(document.querySelectorAll('.reading-content img.wp-manga-chapter-img, img.wp-manga-chapter-img')||[])];
      if (!imgs.length) imgs = [...(document.querySelectorAll('.reading-content img')||[])];
      const out = [];
      for (const img of imgs){
        const s = pickSrc(img);
        if (!s) continue;
        out.push(abs(s));
      }
      return out;
    })()
  `);
}

async function downloadManhwaClanChapter(Page, Runtime, Network, chapter, saveDir){
  await goto(Page, Runtime, chapter.href);
  await waitForSelector(Runtime, '.reading-content img, img.wp-manga-chapter-img', 15000);
  ensureDir(saveDir);

  await smartLoadManhwaClan(Runtime);

  const urls = await waitForImages(Runtime, collectManhwaClanPageUrls, 'manhwaclan');
  if (!urls || !urls.length){
    log('    no images found in .reading-content');
    return {saved:0};
  }
  const res = await downloadUrlList(Network, urls, saveDir, chapter.href, {
    concurrency: IMG_CONCURRENCY,
    filterUrl: (u)=>/\.(png|jpe?g|webp)(\?|$)/i.test(u||''),
    label: 'manhwaclan',
    runtime: Runtime
  });
  return {saved: res.saved};
}

async function processManhwaClan(Page, Runtime, Network, link, channelFolderAbs, siteLabel='MANHWACLAN'){
  log(`\n=== ${siteLabel}: ${link}`);
  await goto(Page, Runtime, link);

  const meta = await scrapeManhwaClanMeta(Runtime);
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

  let chapterSeed = meta?.chapterUrl || '';
  if (!chapterSeed && /\/chapter-\d/i.test(link)) chapterSeed = link;
  if (!chapterSeed){
    try{
      const base = link.endsWith('/') ? link : link + '/';
      chapterSeed = new URL('chapter-1/', base).href;
    }catch{ chapterSeed = link; }
  }

  await goto(Page, Runtime, chapterSeed);
  await waitForSelector(Runtime, 'select.single-chapter-select option, .reading-content img, img.wp-manga-chapter-img', 15000);

  let rawChapters = await collectManhwaClanChapters(Runtime);
  if (!rawChapters || !rawChapters.length){
    log('  no chapter select found, using current chapter only');
    rawChapters = [{href: chapterSeed, label: 'chapter', value: ''}];
  }

  const chapters = [];
  const seen = new Set();
  for (const ch of rawChapters){
    const href = ch && ch.href ? String(ch.href) : '';
    if (!href) continue;
    const key = href.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    const label = (ch.label || '').toString().trim();
    const num = parseManhwaClanChapterNum(label, href, ch.value);
    chapters.push({href, label, num});
  }

  if (!chapters.length){
    log('  no chapters found on ManhwaClan');
    return true;
  }

  const hasNums = chapters.some(c=>Number.isFinite(c.num));
  chapters.sort((a,b)=>{
    if (hasNums){
      const an = Number.isFinite(a.num) ? a.num : Number.MAX_SAFE_INTEGER;
      const bn = Number.isFinite(b.num) ? b.num : Number.MAX_SAFE_INTEGER;
      if (an !== bn) return an - bn;
    }
    return (a.href||'').localeCompare(b.href||'');
  });

  function chapterDirName(ch){
    const base = Number.isFinite(ch.num) ? String(ch.num) : (ch.label || 'chapter');
    const safe = base.replace(/[\\/:*?"<>|]/g,'_').replace(/\s+/g,'_').replace(/\./g,'_').trim();
    return safe || 'chapter';
  }

  const chapterWorkers = Math.min(CHAPTER_CONCURRENCY, chapters.length);
  if (chapterWorkers <= 1){
    for (const ch of chapters){
      const label = ch.label || (Number.isFinite(ch.num) ? `chapter ${ch.num}` : 'chapter');
      log(`  -> ${label} (${ch.href})`);
      const chDir = path.join(outFolder, chapterDirName(ch));
      await downloadManhwaClanChapter(Page, Runtime, Network, ch, chDir);
    }
    return true;
  }

  log(`  chapters parallel: ${chapterWorkers}`);
  let index = 0;
  const extraTabs = [];
  async function worker(workerId, workerTab){
    const {Page: WPage, Runtime: WRuntime, Network: WNetwork} = workerTab;
    while (true){
      const i = index++;
      if (i >= chapters.length) break;
      const ch = chapters[i];
      const label = ch.label || (Number.isFinite(ch.num) ? `chapter ${ch.num}` : 'chapter');
      log(`  -> ${label} (${ch.href}) [w${workerId}]`);
      const chDir = path.join(outFolder, chapterDirName(ch));
      await downloadManhwaClanChapter(WPage, WRuntime, WNetwork, ch, chDir);
    }
  }

  const workers = [];
  workers.push(worker(1, tab));
  for (let w=1; w<chapterWorkers; w++){
    const extra = await openTab('about:blank');
    extraTabs.push(extra);
    workers.push(worker(w+1, extra.tab));
  }
  await Promise.all(workers);
  for (const extra of extraTabs){
    try{ await extra.tab.close(); }catch{}
    try{ await extra.browser.close(); }catch{}
  }
  return true;
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

async function smartLoadWebtoons(Runtime, maxMs=WEBTOONS_WAIT_MAX_MS){
  const start=Date.now(); let lastTotal=0, lastWithUrl=0, stable=0;
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
    const info = await evalInPage(Runtime, `
      (function(){
        function isRealUrl(s){
          if (!s) return false;
          if (/^data:|^about:blank/i.test(s)) return false;
          return /^(https?:)?\\/\\//i.test(s);
        }
        const imgs=[...(document.querySelectorAll('#_imageList img, .viewer_lst img, img._images')||[])];
        let withUrl=0;
        for (const img of imgs){
          const s = img.getAttribute('data-url') || img.currentSrc || img.src || '';
          if (isRealUrl(s)) withUrl++;
        }
        return {total: imgs.length, withUrl};
      })()
    `);
    const total = Number(info?.total) || 0;
    const withUrl = Number(info?.withUrl) || 0;
    if (total > 0 && total === lastTotal && withUrl === lastWithUrl && withUrl === total){
      stable++;
    } else {
      stable = 0;
    }
    lastTotal = total; lastWithUrl = withUrl;
    if (stable >= WEBTOONS_WAIT_STABLE_TICKS) break;
    await sleep(WEBTOONS_WAIT_POLL_MS);
  }
}

async function forceWebtoonsImageLoads(Runtime, urls){
  const list = Array.isArray(urls) ? urls.filter(Boolean) : [];
  if (!list.length) return {matched:0, total:0, extras:0};
  return await evalInPage(Runtime, `
    (function(urls){
      function pickUrl(img){
        return img.getAttribute('data-url') || img.getAttribute('data-src') || img.currentSrc || img.src || '';
      }
      const wanted = new Set(urls);
      const imgs=[...(document.querySelectorAll('#_imageList img, .viewer_lst img, img._images')||[])];
      let matched = 0;
      for (const img of imgs){
        const u = pickUrl(img);
        if (!u) continue;
        if (wanted.has(u)){
          if (!img.src || /^data:|^about:blank/i.test(img.src)) img.src = u;
          img.loading = 'eager';
          matched++;
        }
      }
      const preloads = [];
      for (const u of urls){
        if (!u) continue;
        const im = new Image();
        im.loading = 'eager';
        im.src = u;
        preloads.push(im);
      }
      window.__codexWebtoonsPreloads = preloads;
      return {matched: matched, total: urls.length, extras: Math.max(0, urls.length - matched)};
    })(${JSON.stringify(list)})
  `, true);
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

async function downloadWebtoonsImagesViaCdp(Runtime, Network, urls, saveDir, referer, opts={}){
  const client = runtimeClientMap.get(Runtime);
  if (!client){
    return await downloadUrlList(Network, urls, saveDir, referer, {
      concurrency: IMG_CONCURRENCY,
      filterUrl: looksLikeWebtoonsImg,
      label: 'webtoons',
      normalizeUrl: normalizeWebtoonsUrl,
      runtime: Runtime
    });
  }
  const {
    requireComplete=REQUIRE_COMPLETE_DOWNLOADS,
    maxRounds=DOWNLOAD_ROUNDS,
    roundDelayMs=DOWNLOAD_ROUND_DELAY_MS,
    captureMaxMs=WEBTOONS_CAPTURE_MAX_MS,
    capturePollMs=WEBTOONS_CAPTURE_POLL_MS
  } = opts || {};

  const entries = [];
  const seen = new Set();
  for (const u of (urls||[])){
    const s = (u||'').toString().trim();
    if (!s) continue;
    if (!looksLikeWebtoonsImg(s)) continue;
    const key = s.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    entries.push({url:s, pageNum: entries.length + 1});
  }
  if (!entries.length) return {saved:0, total:0, complete:true, reason:'no-urls'};

  ensureDir(saveDir);
  const savedPages = new Set();
  for (const ent of entries){
    if (pageFileExists(saveDir, ent.pageNum)) savedPages.add(ent.pageNum);
  }

  const exactMap = new Map();
  const normMap = new Map();
  function addToMap(map, key, ent){
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(ent);
  }
  for (const ent of entries){
    addToMap(exactMap, ent.url, ent);
    addToMap(normMap, normalizeWebtoonsUrl(ent.url), ent);
  }

  let savedNew = 0;
  let lastProgressAt = Date.now();
  const inFlight = new Map();

  function findEntryByUrl(url){
    if (!url) return null;
    const list = exactMap.get(url);
    if (list){
      const hit = list.find(e=>!savedPages.has(e.pageNum));
      if (hit) return hit;
    }
    const n = normalizeWebtoonsUrl(url);
    const list2 = normMap.get(n);
    if (list2){
      return list2.find(e=>!savedPages.has(e.pageNum)) || null;
    }
    return null;
  }
  function markSaved(ent){
    if (savedPages.has(ent.pageNum)) return;
    savedPages.add(ent.pageNum);
    savedNew++;
    lastProgressAt = Date.now();
  }

  const onResponse = params=>{
    const resp = params && params.response;
    if (!resp || !resp.url) return;
    if (params.type && params.type !== 'Image'){
      if (!resp.mimeType || !resp.mimeType.startsWith('image/')) return;
    }
    const ent = findEntryByUrl(resp.url);
    if (!ent) return;
    if (!inFlight.has(params.requestId)){
      inFlight.set(params.requestId, {entry: ent, url: resp.url, mimeType: resp.mimeType || ''});
    }
  };

  const onFinished = async params=>{
    const info = inFlight.get(params.requestId);
    if (!info) return;
    inFlight.delete(params.requestId);
    if (pageFileExists(saveDir, info.entry.pageNum)){
      markSaved(info.entry);
      return;
    }
    try{
      const body = await Network.getResponseBody({requestId: params.requestId});
      const buf = body && body.base64Encoded ? Buffer.from(body.body || '', 'base64')
        : Buffer.from((body && body.body) || '', 'utf8');
      if (!buf.length) return;
      const ext = extFromMimeOrUrl(info.mimeType, info.url);
      if (ext==='.gif' || ext==='.svg') return;
      const file = path.join(saveDir, String(info.entry.pageNum).padStart(3,'0') + ext);
      await fs.promises.writeFile(file, buf);
      log(`      saved ${path.basename(file)}`);
      markSaved(info.entry);
    }catch(e){
      dbg('webtoons capture error:', e.message || e);
    }
  };

  const onFailed = params=>{
    if (params && params.requestId) inFlight.delete(params.requestId);
  };

  client.on('Network.responseReceived', onResponse);
  client.on('Network.loadingFinished', onFinished);
  client.on('Network.loadingFailed', onFailed);

  try{
    try{ await Network.setCacheDisabled({cacheDisabled:true}); }catch{}
    const roundsLimit = maxRounds === 0 ? Infinity : Math.max(1, Math.floor(maxRounds));
    let round = 0;
    while (round < roundsLimit){
      const missing = findMissingEntries(saveDir, entries);
      if (!missing.length) break;
      round++;
      if (round > 1){
        log(`      retrying ${missing.length} missing pages (round ${round}/${roundsLimit === Infinity ? 'inf' : roundsLimit})`);
      }
      await forceWebtoonsImageLoads(Runtime, missing.map(m=>m.url));

      const start = Date.now();
      let lastSavedCount = savedPages.size;
      while (Date.now() - start < captureMaxMs){
        const remaining = findMissingEntries(saveDir, entries);
        if (!remaining.length) break;
        if (savedPages.size !== lastSavedCount){
          lastSavedCount = savedPages.size;
        }
        if (Date.now() - lastProgressAt > captureMaxMs) break;
        await sleep(capturePollMs);
      }
      if (round < roundsLimit && roundDelayMs) await sleep(roundDelayMs);
    }
  } finally {
    client.removeListener('Network.responseReceived', onResponse);
    client.removeListener('Network.loadingFinished', onFinished);
    client.removeListener('Network.loadingFailed', onFailed);
    try{ await Network.setCacheDisabled({cacheDisabled:false}); }catch{}
  }

  const remaining = findMissingEntries(saveDir, entries);
  const complete = remaining.length === 0;
  const saved = entries.length - remaining.length;
  if (!complete && requireComplete){
    throw new Error(`download incomplete (webtoons-cdp): ${saved}/${entries.length}`);
  }
  return {saved, total: entries.length, complete, missing: remaining.length, savedNew};
}

async function downloadWebtoonsEpisode(Page, Runtime, Network, episode, saveDir){
  await goto(Page, Runtime, episode.href);
  await waitForSelector(Runtime, '#_imageList img, .viewer_lst img, img._images', 15000);
  ensureDir(saveDir);
  const srcs = await collectWebtoonsImages(Runtime);
  let res;
  if (WEBTOONS_CAPTURE){
    try{
      res = await downloadWebtoonsImagesViaCdp(Runtime, Network, srcs, saveDir, episode.href, {
        requireComplete: REQUIRE_COMPLETE_DOWNLOADS,
        maxRounds: DOWNLOAD_ROUNDS,
        roundDelayMs: DOWNLOAD_ROUND_DELAY_MS
      });
    }catch(e){
      if (REQUIRE_COMPLETE_DOWNLOADS) throw e;
      dbg('webtoons capture failed, fallback to direct fetch:', e.message || e);
      res = await downloadUrlList(Network, srcs, saveDir, episode.href, {
        concurrency: IMG_CONCURRENCY,
        filterUrl: looksLikeWebtoonsImg,
        label: 'webtoons',
        normalizeUrl: normalizeWebtoonsUrl,
        runtime: Runtime
      });
    }
  } else {
    res = await downloadUrlList(Network, srcs, saveDir, episode.href, {
      concurrency: IMG_CONCURRENCY,
      filterUrl: looksLikeWebtoonsImg,
      label: 'webtoons',
      normalizeUrl: normalizeWebtoonsUrl,
      runtime: Runtime
    });
  }
  return {saved: res.saved};
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

  const episodeWorkers = Math.min(CHAPTER_CONCURRENCY, episodes.length);
  if (episodeWorkers <= 1){
    for (const ep of episodes){
      const label = ep.label || (`episode-${ep.no||''}`).trim();
      log(`  -> ${label} (${ep.href})`);
      const dirName = (ep.no!=null ? String(ep.no) : label || 'episode').replace(/[\/:*?"<>|]/g,'_').replace(/\s+/g,'_');
      const chDir = path.join(outFolder, dirName);
      await downloadWebtoonsEpisode(Page, Runtime, Network, ep, chDir);
    }
    return true;
  }

  log(`  episodes parallel: ${episodeWorkers}`);
  let index = 0;
  const extraTabs = [];
  async function worker(workerId, workerTab){
    const {Page: WPage, Runtime: WRuntime, Network: WNetwork} = workerTab;
    while (true){
      const i = index++;
      if (i >= episodes.length) break;
      const ep = episodes[i];
      const label = ep.label || (`episode-${ep.no||''}`).trim();
      log(`  -> ${label} (${ep.href}) [w${workerId}]`);
      const dirName = (ep.no!=null ? String(ep.no) : label || 'episode').replace(/[\/:*?"<>|]/g,'_').replace(/\s+/g,'_');
      const chDir = path.join(outFolder, dirName);
      await downloadWebtoonsEpisode(WPage, WRuntime, WNetwork, ep, chDir);
    }
  }

  const workers = [];
  workers.push(worker(1, tab));
  for (let w=1; w<episodeWorkers; w++){
    const extra = await openTab('about:blank');
    extraTabs.push(extra);
    workers.push(worker(w+1, extra.tab));
  }
  await Promise.all(workers);
  for (const extra of extraTabs){
    try{ await extra.tab.close(); }catch{}
    try{ await extra.browser.close(); }catch{}
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
  const ordered = urls.sort((a,b)=>a.n-b.n).map(u=>u.url);
  const res = await downloadUrlList(Network, ordered, saveDir, chapterUrl, {
    concurrency: IMG_CONCURRENCY,
    filterUrl: looksLikeBuffCdn,
    label: 'mangabuff',
    useLimiter: false,
    runtime: Runtime
  });

  log(`    saved pages: ${res.saved}`);
  return {saved: res.saved};
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


  const listRaw = await collectMangabuffChapterLinks(Runtime);
  if (listRaw.length){
    const chapters = [];
    const seenList = new Set();
    for (const item of listRaw){
      const href = item && item.href ? String(item.href) : '';
      if (!href) continue;
      const key = href.toLowerCase();
      if (seenList.has(key)) continue;
      seenList.add(key);
      const parsed = parseBuffVolChap(href);
      const vol = parsed.vol;
      const chap = parsed.chap;
      const volNum = Number.isFinite(parseFloat(vol)) ? parseFloat(vol) : Number.MAX_SAFE_INTEGER;
      const chapNum = Number.isFinite(parseFloat(chap)) ? parseFloat(chap) : Number.MAX_SAFE_INTEGER;
      chapters.push({href, vol, chap, volNum, chapNum});
    }
    chapters.sort((a,b)=> (a.volNum-b.volNum) || (a.chapNum-b.chapNum) || a.href.localeCompare(b.href));
    if (chapters.length){
      log(`  chapters list: ${chapters.length}`);
      const chapterWorkers = Math.min(CHAPTER_CONCURRENCY, chapters.length);
      if (chapterWorkers <= 1){
        for (const ch of chapters){
          log(`  -> vol ${ch.vol} ch ${ch.chap} (${ch.href})`);
          const chDir = path.join(outFolder, String(ch.vol), String(ch.chap).replace(/\./g,'_'));
          ensureDir(chDir);
          await downloadMangabuffChapter(Page, Runtime, Network, ch.href, chDir);
        }
        return true;
      }

      log(`  chapters parallel: ${chapterWorkers}`);
      let index = 0;
      const extraTabs = [];
      async function worker(workerId, workerTab){
        const {Page: WPage, Runtime: WRuntime, Network: WNetwork} = workerTab;
        while (true){
          const i = index++;
          if (i >= chapters.length) break;
          const ch = chapters[i];
          log(`  -> vol ${ch.vol} ch ${ch.chap} (${ch.href}) [w${workerId}]`);
          const chDir = path.join(outFolder, String(ch.vol), String(ch.chap).replace(/\./g,'_'));
          ensureDir(chDir);
          await downloadMangabuffChapter(WPage, WRuntime, WNetwork, ch.href, chDir);
        }
      }

      const workers = [];
      workers.push(worker(1, tab));
      for (let w=1; w<chapterWorkers; w++){
        const extra = await openTab('about:blank');
        extraTabs.push(extra);
        workers.push(worker(w+1, extra.tab));
      }
      await Promise.all(workers);
      for (const extra of extraTabs){
        try{ await extra.tab.close(); }catch{}
        try{ await extra.browser.close(); }catch{}
      }
      return true;
    }
  }

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
  const total = srcs.length;
  if (!total){
    const savedEmpty = existingImagesInfo(saveDir).count;
    return {saved: savedEmpty};
  }

  const blobEntries = [];
  const urlEntries = [];
  for (let i=0; i<srcs.length; i++){
    const src = srcs[i];
    const pageNum = i + 1;
    if (!src) continue;
    if (src.startsWith('blob:')) blobEntries.push({url: src, pageNum});
    else urlEntries.push({url: src, pageNum});
  }

  if (blobEntries.length){
    await mapConcurrent(blobEntries, IMG_CONCURRENCY, async (entry) => {
      const pageNum = entry.pageNum;
      const target = pageFilePath(saveDir, pageNum, path.extname(entry.url)||'.jpg');
      if (target.exists) return;
      try{
        const buf = await fetchBlobBytes(Runtime, entry.url);
        const ext = extFromMimeOrUrl('', entry.url);
        const file = target.file.endsWith(ext) ? target.file : path.join(saveDir, String(pageNum).padStart(3,'0') + ext);
        await fs.promises.writeFile(file, buf);
        log(`      saved ${path.basename(file)}`);
        if (STEP_DELAY) await sleep(STEP_DELAY);
      }catch(e){ log(`      fail page ${pageNum}: ${e.message||e}`); }
    });
  }

  if (urlEntries.length){
    await downloadUrlList(Network, [], saveDir, chapter.href, {
      concurrency: IMG_CONCURRENCY,
      filterUrl: looksLikeImageUrl,
      label: 'dex',
      entries: urlEntries,
      runtime: Runtime
    });
  }

  const savedCount = existingImagesInfo(saveDir).count;
  try{ writeJSON(progressFile, {saved:savedCount, total:total, chapterId: chapter.id||''}); }catch{}
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
    const chapterWorkers = Math.min(CHAPTER_CONCURRENCY, ordered.length);
    if (chapterWorkers <= 1){
      for (const ch of ordered){
        const chLabel = ch.label || ch.id || 'chapter';
        log(`  -> ${chLabel} (${ch.href})`);
        const chDir = path.join(outFolder, '0', String(chLabel).replace(/\s+/g,'_').replace(/\./g,'_'));
        await saveDexChapterDom(Page, Runtime, Network, ch, chDir);
      }
      return true;
    }

    log(`  chapters parallel: ${chapterWorkers}`);
    let index = 0;
    const extraTabs = [];
    async function worker(workerId, workerTab){
      const {Page: WPage, Runtime: WRuntime, Network: WNetwork} = workerTab;
      while (true){
        const i = index++;
        if (i >= ordered.length) break;
        const ch = ordered[i];
        const chLabel = ch.label || ch.id || 'chapter';
        log(`  -> ${chLabel} (${ch.href}) [w${workerId}]`);
        const chDir = path.join(outFolder, '0', String(chLabel).replace(/\s+/g,'_').replace(/\./g,'_'));
        await saveDexChapterDom(WPage, WRuntime, WNetwork, ch, chDir);
      }
    }

    const workers = [];
    workers.push(worker(1, tab));
    for (let w=1; w<chapterWorkers; w++){
      const extra = await openTab('about:blank');
      extraTabs.push(extra);
      workers.push(worker(w+1, extra.tab));
    }
    await Promise.all(workers);
    for (const extra of extraTabs){
      try{ await extra.tab.close(); }catch{}
      try{ await extra.browser.close(); }catch{}
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
    const isClan = MANHWACLAN_HOSTS.some(h=>host.endsWith(h));
    const isManhua = MANHUAUS_HOSTS.some(h=>host.endsWith(h));
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

      const chapterWorkers = Math.min(CHAPTER_CONCURRENCY, chapters.length);
      if (chapterWorkers <= 1){
        for (const ch of chapters){
          const label = ch.label || ('chapter '+ch.ch);
          log(`  -> ${label} (${ch.href})`);
          const chDir=path.join(outFolder,String(ch.ch).replace(/\./g,'_'));
          await downloadMangalibChapter(Page,Runtime,Network,ch.href,chDir);
        }
        return true;
      }

      log(`  chapters parallel: ${chapterWorkers}`);
      let index = 0;
      const extraTabs = [];
      async function worker(workerId, workerTab){
        const {Page: WPage, Runtime: WRuntime, Network: WNetwork} = workerTab;
        while (true){
          const i = index++;
          if (i >= chapters.length) break;
          const ch = chapters[i];
          const label = ch.label || ('chapter '+ch.ch);
          log(`  -> ${label} (${ch.href}) [w${workerId}]`);
          const chDir = path.join(outFolder, String(ch.ch).replace(/\./g,'_'));
          await downloadMangalibChapter(WPage, WRuntime, WNetwork, ch.href, chDir);
        }
      }

      const workers = [];
      workers.push(worker(1, tab));
      for (let w=1; w<chapterWorkers; w++){
        const extra = await openTab('about:blank');
        extraTabs.push(extra);
        workers.push(worker(w+1, extra.tab));
      }
      await Promise.all(workers);
      for (const extra of extraTabs){
        try{ await extra.tab.close(); }catch{}
        try{ await extra.browser.close(); }catch{}
      }
      return true;
    }

    if (isBuf){
      return await processMangabuff(Page, Runtime, Network, link, channelFolderAbs);
    }

    if (isClan){
      return await processManhwaClan(Page, Runtime, Network, link, channelFolderAbs, 'MANHWACLAN');
    }

    if (isManhua){
      return await processManhwaClan(Page, Runtime, Network, link, channelFolderAbs, 'MANHUAUS');
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
    console.error('Usage:\n  node mangaGrabber.js --linksJson="C:\\\\path\\\\links.json" --channelFolder="C:\\\\YT\\\\MyChannel" [--debug] [--rpm=10] [--stepDelay=0] [--imgConcurrency=1] [--chapterConcurrency=1]');
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

