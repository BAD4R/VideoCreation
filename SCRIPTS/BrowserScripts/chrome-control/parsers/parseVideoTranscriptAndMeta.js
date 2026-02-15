// yt_transcript_fetcher.js
// Р—Р°РїСѓСЃРє Chrome Р·Р°СЂР°РЅРµРµ (РїСЂРёРјРµСЂ):
//   "C:\Program Files\Google\Chrome\Application\chrome.exe"
//      --remote-debugging-port=9333 --remote-debugging-address=127.0.0.1
//      --user-data-dir=C:\Users\V\AppData\Local\Google\Chrome\RemoteControl
//      --profile-directory=Default --new-window --start-maximized --window-size=1920,1080
//
// Р—Р°РїСѓСЃРє СЃРєСЂРёРїС‚Р°:
//   node script.js --videoLink="https://www.youtube.com/watch?v=XXX" --rewriteFolderPath="C:\path\to\folder"
//
// РўСЂРµР±СѓРµС‚: npm i chrome-remote-interface

const CDP  = require('chrome-remote-interface');
const fs   = require('fs');
const path = require('path');

const CDP_HOST = process.env.CDP_HOST || '127.0.0.1';
const CDP_PORT = Number(process.env.CDP_PORT || 9333);

// ---------- РђСЂРіСѓРјРµРЅС‚С‹ ----------
function parseArgs(argv) {
  const out = { _pos: [] };
  for (let i = 0; i < argv.length; i++) {
    const tok = argv[i];
    if (tok.startsWith('--')) {
      const eq = tok.indexOf('=');
      if (eq >= 0) {
        const k = tok.slice(2, eq);
        let v = tok.slice(eq + 1).replace(/^["']|["']$/g, '');
        out[k] = v;
      } else {
        const k = tok.slice(2);
        let v = (i+1 < argv.length && !argv[i+1].startsWith('--')) ? argv[++i] : 'true';
        v = v.replace(/^["']|["']$/g, '');
        out[k] = v;
      }
    } else {
      out._pos.push(tok.replace(/^["']|["']$/g, ''));
    }
  }
  return out;
}

const args = parseArgs(process.argv.slice(2));
const videoLink = args.videoLink ?? args._pos[0];
let rewriteFolderPath = args.rewriteFolderPath ?? args._pos[1] ?? '';

if (!videoLink) {
  console.error('вќЊ РќСѓР¶РµРЅ РїР°СЂР°РјРµС‚СЂ --videoLink="https://www.youtube.com/watch?v=..."\n' +
                'РћРїС†РёРѕРЅР°Р»СЊРЅРѕ: --rewriteFolderPath="C:\\path\\to\\folder"');
  process.exit(1);
}
if (rewriteFolderPath) {
  rewriteFolderPath = path.resolve(rewriteFolderPath);
  if (!fs.existsSync(rewriteFolderPath)) fs.mkdirSync(rewriteFolderPath, { recursive: true });
}
const outDir = rewriteFolderPath || process.cwd();
const outPath = path.join(outDir, 'originalVideo.json');

function writeOriginalVideoJson(parseStatus, payload = {}) {
  const data = {
    parseStatus,
    videoTitle: payload.videoTitle ?? '',
    videoDescription: payload.videoDescription ?? '',
    videoPreviewText: payload.videoPreviewText ?? '',
    videoTranscript: payload.videoTranscript ?? '',
    videoLink: payload.videoLink ?? videoLink
  };
  fs.writeFileSync(outPath, JSON.stringify(data, null, 2), 'utf-8');
  return data;
}

// ---------- Р“Р»РѕР±Р°Р»СЊРЅС‹Рµ РЅР°СЃС‚СЂРѕР№РєРё РїР°СѓР· ----------
const STEP_DELAY_MS = Number(process.env.STEP_DELAY_MS || 4000);

// ---------- РЈС‚РёР»РёС‚С‹ ----------
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function withTimeout(promise, ms, msg='Timeout') {
  let t; const timeout = new Promise((_, rej) => t = setTimeout(() => rej(new Error(msg)), ms));
  try { return await Promise.race([promise, timeout]); }
  finally { clearTimeout(t); }
}
async function stepPause() { await sleep(STEP_DELAY_MS); }
async function stepLog(msg) { console.log(msg); await stepPause(); }

// ---------- CDP helpers ----------
async function waitForYouTubeWatchReady(Runtime, timeoutMs = 90000) {
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const start = Date.now();

  // 1) РґРѕР¶РґР°С‚СЊСЃСЏ document.readyState === "complete"
  while (Date.now() - start < timeoutMs) {
    const { result } = await Runtime.evaluate({ expression: 'document.readyState', returnByValue: true });
    if (result?.value === 'complete') break;
    await sleep(200);
  }

  // 2) РґРѕР¶РґР°С‚СЊСЃСЏ РєР»СЋС‡РµРІС‹С… СѓР·Р»РѕРІ (ytd-watch-flexy + Р·Р°РіРѕР»РѕРІРѕРє СЃ С‚РµРєСЃС‚РѕРј)
  while (Date.now() - start < timeoutMs) {
    const { result } = await Runtime.evaluate({
      returnByValue: true,
      expression: `
        (function(){
          const flexy = document.querySelector('ytd-watch-flexy');
          const title = document.querySelector('h1 yt-formatted-string, h1.title');
          return !!(flexy && title && (title.textContent||'').trim().length > 0);
        })()
      `
    });
    if (result?.value) break;
    await sleep(300);
  }

  // 3) РґРѕР¶РґР°С‚СЊСЃСЏ РёСЃС‡РµР·РЅРѕРІРµРЅРёСЏ Р»РѕР°РґРµСЂРѕРІ/СЃРєРµР»РµС‚РѕРЅРѕРІ
  while (Date.now() - start < timeoutMs) {
    const { result } = await Runtime.evaluate({
      returnByValue: true,
      expression: `
        (function(){
          const hasSkeleton =
            document.querySelector('.ytd-watch-flexy[loading], .skeleton, ytd-player[loading]') ||
            document.querySelector('tp-yt-paper-progress[aria-valuenow]');
          return !hasSkeleton;
        })()
      `
    });
    if (result?.value) break;
    await sleep(400);
  }

  // 4) С„РёРЅР°Р»СЊРЅР°СЏ РјР°Р»РµРЅСЊРєР°СЏ РїР°СѓР·Р°
  await sleep(800);
}


async function openTab() {
  const versionInfo = await CDP.Version({ host: CDP_HOST, port: CDP_PORT });
  const browserWs = versionInfo?.webSocketDebuggerUrl;
  if (!browserWs) {
    throw new Error(`РќРµ СѓРґР°Р»РѕСЃСЊ РїРѕР»СѓС‡РёС‚СЊ webSocketDebuggerUrl Сѓ Chrome (${CDP_HOST}:${CDP_PORT})`);
  }

  // РџРѕРґРєР»СЋС‡Р°РµРјСЃСЏ Рє browser endpoint, С‡С‚РѕР±С‹ РЅРµ Р·Р°РІРёСЃРµС‚СЊ РѕС‚ РЅР°Р»РёС‡РёСЏ РѕС‚РєСЂС‹С‚С‹С… РІРєР»Р°РґРѕРє.
  const browser = await CDP({ target: browserWs });
  const { Target, Emulation, Browser, Page } = browser;
  await Target.setDiscoverTargets({ discover: true });

  const { targetId } = await Target.createTarget({ url: 'about:blank' });
  const tab = await CDP({ host: CDP_HOST, port: CDP_PORT, target: targetId });
  const { Page: P, Runtime, DOM, Emulation: E, Browser: B } = tab;

  await Promise.all([P.enable(), Runtime.enable(), DOM.enable()]);
  try { await P.setLifecycleEventsEnabled({ enabled: true }); } catch {}


  // РњР°РєСЃРёРјРёР·РёСЂСѓРµРј РѕРєРЅРѕ Рё Р·Р°РґР°С‘Рј РјРµС‚СЂРёРєРё РґРёСЃРїР»РµСЏ РґР»СЏ СЃС‚Р°Р±РёР»СЊРЅРѕСЃС‚Рё UI
  try {
    const { windowId } = await B.getWindowForTarget({ targetId });
    await B.setWindowBounds({ windowId, bounds: { windowState: 'maximized' } });
  } catch {}

  try {
    await E.setDeviceMetricsOverride({
      width: 1920, height: 1080, deviceScaleFactor: 1, mobile: false,
      screenWidth: 1920, screenHeight: 1080
    });
  } catch {}

  return { browser, tab, targetId };
}

// РЎРєР°С‡РёРІР°РЅРёРµ РїСЂРµРІСЊСЋ РІ Р±СЂР°СѓР·РµСЂРµ (fetch -> blob -> <a download>), СЃРѕС…СЂР°РЅСЏСЏ РІ rewriteFolderPath
async function downloadPreviewInBrowser(videoId, outDir, Page, Runtime) {
  if (!videoId || !outDir) return { ok:false, path:'' };
  try {
    // РЅР°РїСЂР°РІР»СЏРµРј РІСЃРµ Р·Р°РіСЂСѓР·РєРё СЃС‚СЂР°РЅРёС†С‹ РІ РЅСѓР¶РЅСѓСЋ РїР°РїРєСѓ
    await Page.setDownloadBehavior({ behavior: 'allow', downloadPath: outDir });
  } catch {}

  const candidates = [
    `https://img.youtube.com/vi/${videoId}/maxresdefault.jpg`,
    `https://i.ytimg.com/vi/${videoId}/maxresdefault.jpg`,
    `https://i.ytimg.com/vi/${videoId}/sddefault.jpg`,
    `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
    `https://i.ytimg.com/vi/${videoId}/mqdefault.jpg`,
    `https://i.ytimg.com/vi_webp/${videoId}/maxresdefault.webp`,
    `https://i.ytimg.com/vi_webp/${videoId}/sddefault.webp`,
  ];

  for (const url of candidates) {
    const res = await Runtime.evaluate({
      returnByValue: true,
      expression: `
        (async function(){
          try {
            const resp = await fetch(${JSON.stringify(url)}, {cache:'no-store', credentials:'omit'});
            if (!resp.ok) return false;
            const blob = await resp.blob();
            const a = document.createElement('a');
            const obj = URL.createObjectURL(blob);
            a.href = obj;
            a.download = 'preview.jpg';
            document.body.appendChild(a);
            a.click();
            setTimeout(() => { URL.revokeObjectURL(obj); a.remove(); }, 4000);
            return true;
          } catch(e) { return false; }
        })()
      `
    });
    if (res?.result?.value) {
      const target = path.join(outDir, 'preview.jpg');
      for (let i = 0; i < 40; i++) { // РґРѕ ~20 СЃРµРєСѓРЅРґ
        if (fs.existsSync(target)) return { ok:true, path: target };
        await sleep(500);
      }
    }
  }
  return { ok:false, path:'' };
}

// РљРѕРґ, РєРѕС‚РѕСЂС‹Р№ РёСЃРїРѕР»РЅСЏРµС‚СЃСЏ РІ РєРѕРЅС‚РµРєСЃС‚Рµ СЃС‚СЂР°РЅРёС†С‹: СЃРѕР±РёСЂР°РµС‚ title/description/link Рё Р·Р°РіСЂСѓР¶Р°РµС‚ С‚СЂР°РЅСЃРєСЂРёРїС‚
function buildPageCollectorIIFE() {
  return `
  (async function () {
    const sleep = ms => new Promise(r => setTimeout(r, ms));
    const TIMEOUT_MS = 90_000;
    const STABLE_ROUNDS = 3;
    const SCROLL_PAUSE_MS = 500;
    const now = () => performance.now();

    async function waitForSelector(selector, { timeout = 15_000 } = {}) {
      const start = now();
      while (now() - start < timeout) {
        const el = document.querySelector(selector);
        if (el) return el;
        await sleep(150);
      }
      return null;
    }
    async function clickIfExists(selector) {
      const el = document.querySelector(selector);
      if (el) {
        el.click();
        await sleep(500);
        return true;
      }
      return false;
    }
    function clickButtonByText(needles) {
      const buttons = Array.from(document.querySelectorAll('button, tp-yt-paper-button, ytd-menu-service-item-renderer'));
      const btn = buttons.find(b => {
        const t = (b.innerText || b.textContent || '').trim().toLowerCase();
        return needles.some(n => t.includes(n));
      });
      if (btn) { btn.click(); return true; }
      return false;
    }

    const SHOW_TRANSCRIPT_TEXTS = [
      'show transcript','open transcript',
      'РїРѕРєР°Р·Р°С‚СЊ СЃС‚РµРЅРѕРіСЂР°РјРјСѓ','РїРѕРєР°Р·Р°С‚СЊ С‚СЂР°РЅСЃРєСЂРёРїС‚',
      'anzeigen: transkript','transcript anzeigen',
      'mostrar transcripciГіn','afficher la transcription',
      'mostra trascrizione'
    ].map(s => s.toLowerCase());

    // 1) СЂР°СЃРєСЂС‹С‚СЊ РѕРїРёСЃР°РЅРёРµ
    await clickIfExists('#expand, tp-yt-paper-button#expand');

    // 2) Р·Р°РіРѕР»РѕРІРѕРє/РѕРїРёСЃР°РЅРёРµ/СЃСЃС‹Р»РєР°
    const descElement = document.querySelector('#expanded yt-attributed-string, #description-inline-expander yt-attributed-string');
    const videoDescription = descElement ? descElement.innerText.trim() : '';
    const titleEl = document.querySelector('h1 yt-formatted-string, h1.title');
    const videoTitle = titleEl ? titleEl.innerText.trim() : '';

    const url = new URL(window.location.href);
    const videoId =
      url.searchParams.get('v') ||
      (window.location.href.match(/[?&]v=([a-zA-Z0-9_-]+)/)?.[1] ?? '') ||
      (document.querySelector('ytd-watch-flexy')?.getAttribute('video-id') ?? '');
    const videoLink = videoId ? ('https://www.youtube.com/watch?v=' + videoId) : window.location.href;

    // 3) РѕС‚РєСЂС‹С‚СЊ Transcript
    // РѕС‚РєСЂС‹С‚СЊ РјРµРЅСЋ "..."
    await clickIfExists('#top-level-buttons-computed ytd-button-renderer:last-of-type button') ||
    await clickIfExists('ytd-button-renderer#button button');
    await sleep(400);
    // РЅР°Р¶Р°С‚СЊ Show transcript РїРѕ С‚РµРєСЃС‚Р°Рј
    clickButtonByText(SHOW_TRANSCRIPT_TEXTS);
    await sleep(800);

    // 4) Р¶РґР°С‚СЊ РїР°РЅРµР»СЊ Рё СЃРµРіРјРµРЅС‚С‹
    const panel = await waitForSelector('ytd-transcript-renderer #segments-container', { timeout: 20_000 });

    async function getSegments() {
      return Array.from(
        document.querySelectorAll('ytd-transcript-segment-renderer yt-formatted-string.segment-text')
      ).map(s => (s.textContent || '').trim()).filter(Boolean);
    }

    let segments = [];
    if (panel) {
      const start = now();
      let stableCount = 0;
      let lastLen = 0;
      // Р¶РґР°С‚СЊ РїРµСЂРІС‹Р№ СЃРµРіРјРµРЅС‚
      while (now() - start < TIMEOUT_MS) {
        segments = await getSegments();
        if (segments.length > 0) break;
        await sleep(250);
      }
      if (segments.length > 0) {
        // РґРѕРіСЂСѓР¶Р°РµРј РІСЃРµ
        while (now() - start < TIMEOUT_MS) {
          panel.scrollTo(0, panel.scrollHeight);
          await sleep(SCROLL_PAUSE_MS);
          const current = await getSegments();
          if (current.length > lastLen) {
            lastLen = current.length;
            stableCount = 0;
          } else {
            stableCount += 1;
          }
          if (stableCount >= STABLE_ROUNDS) { segments = current; break; }
        }
      }
    }

    // 5) СЃРєР»РµРёС‚СЊ С‚РµРєСЃС‚
    let videoTranscript = '';
    if (segments.length > 0) {
      videoTranscript = segments
        .map(t => {
          t = t.replace(/\\s+/g, ' ').trim();
          if (!/[.!?вЂ¦]$/.test(t)) t += '.';
          return t;
        })
        .join(' ');
    }

    return {
      ok: true,
      videoId,
      videoTitle,
      videoDescription,
      videoLink,
      videoTranscript,
      transcriptOk: !!videoTranscript
    };
  })()
  `;
}

// ---------- РћСЃРЅРѕРІРЅРѕР№ РїРѕС‚РѕРє ----------
(async () => {
  let browser, tab;
  let finalStatusWritten = false;
  try {
    writeOriginalVideoJson('processing');

    await stepLog('рџ”Њ РџРѕРґРєР»СЋС‡Р°СЋСЃСЊ Рє ChromeвЂ¦');
    const opened = await openTab();
    browser = opened.browser;
    tab     = opened.tab;
    const { Page, Runtime, Emulation, Browser } = tab;

    await stepLog(`рџ“є РћС‚РєСЂС‹РІР°СЋ РІРёРґРµРѕ: ${videoLink}`);
    await Page.navigate({ url: videoLink });
    await waitForYouTubeWatchReady(Runtime, 90000);


    // РµС‰С‘ СЂР°Р· РЅР° РІСЃСЏРєРёР№ РІС‹СЃС‚Р°РІРёРј РјРµС‚СЂРёРєРё (РёРЅРѕРіРґР° РїРѕР»РµР·РЅРѕ РїРѕСЃР»Рµ РЅР°РІРёРіР°С†РёРё)
    try {
      await Emulation.setDeviceMetricsOverride({
        width: 1920, height: 1080, deviceScaleFactor: 1, mobile: false,
        screenWidth: 1920, screenHeight: 1080
      });
    } catch {}

    await stepLog('рџ“ќ Р Р°СЃРєСЂС‹РІР°СЋ РѕРїРёСЃР°РЅРёРµ Рё РїРѕРґРіРѕС‚Р°РІР»РёРІР°СЋ СЃР±РѕСЂвЂ¦');
    await stepLog('рџ’¬ РћС‚РєСЂС‹РІР°СЋ Transcript/РЎС‚РµРЅРѕРіСЂР°РјРјСѓвЂ¦');

    // РЎРѕР±РµСЂС‘Рј РґР°РЅРЅС‹Рµ РЅР° СЃС‚СЂР°РЅРёС†Рµ
    const evalRes = await Runtime.evaluate({
      expression: buildPageCollectorIIFE(),
      returnByValue: true,
      awaitPromise: true   // <<< РІР°Р¶РЅС‹Р№ РјРѕРјРµРЅС‚!
    });

    // Р•СЃР»Рё СЃРєСЂРёРїС‚ РІ Р±СЂР°СѓР·РµСЂРµ СѓРїР°Р» вЂ” РїРѕРєР°Р¶РµРј РѕС€РёР±РєСѓ
    if (evalRes.exceptionDetails) {
      console.error('рџ”Ґ РћС€РёР±РєР° РІРЅСѓС‚СЂРё СЃС‚СЂР°РЅРёС†С‹:', evalRes.exceptionDetails.text);
      throw new Error('JS РІРЅСѓС‚СЂРё СЃС‚СЂР°РЅРёС†С‹ СѓРїР°Р»');
    }

    const payload = evalRes?.result?.value;
    if (!payload || !payload.ok) {
      console.error('вљ пёЏ РџСѓСЃС‚РѕР№ РѕС‚РІРµС‚ РѕС‚ IIFE. РћС‚РІРµС‚:', payload);
      throw new Error('РќРµ СѓРґР°Р»РѕСЃСЊ СЃРѕР±СЂР°С‚СЊ РґР°РЅРЅС‹Рµ СЃРѕ СЃС‚СЂР°РЅРёС†С‹');
    }

    const {
      videoId,
      videoTitle,
      videoDescription,
      videoLink: normalizedLink,
      videoTranscript,
      transcriptOk
    } = payload;


    await stepLog('вЏі Р–РґСѓ Рё РґРѕРіСЂСѓР¶Р°СЋ СЃРµРіРјРµРЅС‚С‹ С‚СЂР°РЅСЃРєСЂРёРїС‚Р°вЂ¦');
    // (СѓР¶Рµ СЃРґРµР»Р°РЅРѕ РІРЅСѓС‚СЂРё IIFE; СЌС‚РѕС‚ С€Р°Рі РїСЂРѕСЃС‚Рѕ РґР»СЏ РІРёР·СѓР°Р»СЊРЅРѕРіРѕ Р»РѕРіР°)

    await stepLog('рџ§© РЎРєР»РµРёРІР°СЋ С‚РµРєСЃС‚ С‚СЂР°РЅСЃРєСЂРёРїС‚Р°вЂ¦');
    // (С‚РѕР¶Рµ СѓР¶Рµ РІРЅСѓС‚СЂРё IIFE)

    // РЎРєР°С‡РёРІР°РЅРёРµ РїСЂРµРІСЊСЋ С‡РµСЂРµР· Р±СЂР°СѓР·РµСЂ РІ rewriteFolderPath (РµСЃР»Рё СѓРєР°Р·Р°РЅ)
    let previewPath = '';
    if (rewriteFolderPath && videoId) {
      await stepLog('рџ–јпёЏ РЎРєР°С‡РёРІР°СЋ РїСЂРµРІСЊСЋ (С‡РµСЂРµР· Р±СЂР°СѓР·РµСЂ)вЂ¦');
      const got = await downloadPreviewInBrowser(videoId, rewriteFolderPath, Page, Runtime);
      if (got.ok) {
        previewPath = got.path;
      } else {
        console.warn('РќРµ СѓРґР°Р»РѕСЃСЊ СЃРєР°С‡Р°С‚СЊ РїСЂРµРІСЊСЋ С‡РµСЂРµР· Р±СЂР°СѓР·РµСЂ (РІСЃРµ РєР°РЅРґРёРґР°С‚С‹).');
      }
    } else {
      console.log('РџСЂРѕРїСѓСЃРєР°СЋ Р·Р°РіСЂСѓР·РєСѓ РїСЂРµРІСЊСЋ: РЅРµС‚ rewriteFolderPath РёР»Рё videoId.');
    }

    // Р—Р°РїРёСЃСЊ JSON (С‚РѕР»СЊРєРѕ РµСЃР»Рё С‚СЂР°РЅСЃРєСЂРёРїС‚ РЅРµ РїСѓСЃС‚РѕР№)
    if (!transcriptOk) {
      await stepLog('Writing originalVideo.json (fail)...');
      writeOriginalVideoJson('fail', {
        videoTitle,
        videoDescription,
        videoTranscript,
        videoLink: normalizedLink || videoLink
      });
      finalStatusWritten = true;
      console.warn('Transcript is empty or unavailable; parseStatus=fail.');
      console.log('Saved:\n', outPath);
    } else {
      await stepLog('Writing originalVideo.json...');
      writeOriginalVideoJson('success', {
        videoTitle,
        videoDescription,
        videoPreviewText: '',
        videoTranscript,
        videoLink: normalizedLink || videoLink
      });
      finalStatusWritten = true;
      console.log('Saved:\n', outPath);
      if (previewPath) console.log('Preview:', previewPath);
    }

    // Р—Р°РєСЂС‹РІР°РµРј С‚РѕР»СЊРєРѕ РЅР°С€Сѓ CDP-СЃРµСЃСЃРёСЋ
    try { await browser.Browser.close(); } catch {}
    console.log('VIDEO PARSED');
  } catch (e) {
    if (!finalStatusWritten) {
      try { writeOriginalVideoJson('fail'); } catch {}
    }
    console.error('вќЊ РћС€РёР±РєР°:', e?.message || e);
    console.log('PARSING FAILED');
    try { if (browser) await browser.Browser.close(); } catch {}
    process.exitCode = 1;
  } finally {
    try { if (tab) await tab.close(); } catch {}
    try { if (browser) await browser.close(); } catch {}
  }
})();

