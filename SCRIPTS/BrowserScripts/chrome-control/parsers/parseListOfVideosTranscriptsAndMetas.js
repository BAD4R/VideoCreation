#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawn } = require('child_process');

let puppeteer;
try {
  puppeteer = require('puppeteer-core');
} catch (err) {
  console.error('Missing dependency: puppeteer-core');
  console.error('Install it with: npm i puppeteer-core');
  process.exit(1);
}

const CONFIG = {
  debuggerPort: 9222,
  connectTimeoutMs: 30000,
  protocolTimeoutMs: 420000,
  navigationTimeoutMs: 90000,
  transcriptPanelTimeoutMs: 30000,
  transcriptLoadTimeoutMs: 90000,
  transcriptStableRounds: 6,
  transcriptScrollPauseMs: 800,
  commentsMaxScrollRounds: 70,
  commentsMaxExpandRounds: 80,
  commentsScrollPauseMs: 1100,
  commentsClickPauseMs: 220,
  commentsStableRoundsToStop: 4,
  perVideoDelayMs: 1000,
  perVideoTimeoutMs: 360000,
  previewFetchTimeoutMs: 15000,
  pageDebugOverlay: true,
};

const STOP_STATE = {
  requested: false,
  reason: '',
};

const PAUSE_STATE = {
  requested: false,
  reason: '',
};

const PAGE_BRIDGES_READY = new WeakSet();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTimeout(promiseFactory, timeoutMs, label) {
  let timer = null;

  try {
    return await Promise.race([
      Promise.resolve().then(() => promiseFactory()),
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`${label} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function requestStop(reason = 'Stop requested from UI') {
  if (!STOP_STATE.requested) {
    STOP_STATE.requested = true;
    STOP_STATE.reason = sanitizeForJsonLine(reason);
    console.warn(`[STOP] ${STOP_STATE.reason}`);
  }
}

function setPaused(paused, reason = '') {
  PAUSE_STATE.requested = !!paused;
  PAUSE_STATE.reason = sanitizeForJsonLine(reason);
  console.warn(
    `[PAUSE] ${PAUSE_STATE.requested ? 'paused' : 'resumed'}${PAUSE_STATE.reason ? `: ${PAUSE_STATE.reason}` : ''}`
  );
}

function ensureNotStopped(label = 'Operation') {
  if (STOP_STATE.requested) {
    throw new Error(`Stopped: ${STOP_STATE.reason || label}`);
  }
}

async function waitIfPaused(label = 'Operation') {
  while (PAUSE_STATE.requested) {
    ensureNotStopped(label);
    await sleep(250);
  }
}

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const part = argv[i];
    if (!part.startsWith('--')) continue;

    const key = part.slice(2);
    const next = argv[i + 1];

    if (!next || next.startsWith('--')) {
      args[key] = true;
      continue;
    }

    args[key] = next;
    i++;
  }
  return args;
}

function sanitizeForJsonLine(value) {
  return String(value || '')
    .replace(/\\/g, '')
    .replace(/[\u0000-\u001F\u007F]/g, ' ')
    .replace(/\u00A0/g, ' ')
    .replace(/"/g, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function sanitizeMultilineForJson(value) {
  return String(value || '')
    .split(/\r?\n/)
    .map((line) => sanitizeForJsonLine(line))
    .filter(Boolean)
    .join('\n');
}

function makeSafeFolderName(value, fallback = 'video') {
  let s = sanitizeForJsonLine(value);

  s = s
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '_')
    .replace(/\s+/g, '_')
    .replace(/\.+$/g, '')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');

  if (!s) s = fallback;
  if (s.length > 120) s = s.slice(0, 120).replace(/^_+|_+$/g, '');

  return s || fallback;
}

function extractVideoId(videoUrl) {
  try {
    const url = new URL(videoUrl);

    if (url.hostname.includes('youtu.be')) {
      return url.pathname.replace(/^\/+/, '').trim();
    }

    if (url.searchParams.get('v')) {
      return url.searchParams.get('v');
    }

    const match = videoUrl.match(/[?&]v=([a-zA-Z0-9_-]+)/);
    if (match) return match[1];
  } catch {}

  return 'video';
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function fileExists(p) {
  try {
    fs.accessSync(p);
    return true;
  } catch {
    return false;
  }
}

function getDefaultChromePath() {
  const candidates = [
    process.env.CHROME_PATH,
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
    path.join(process.env.LOCALAPPDATA || '', 'Google\\Chrome\\Application\\chrome.exe'),
    path.join(process.env.PROGRAMFILES || '', 'Google\\Chrome\\Application\\chrome.exe'),
    path.join(process.env['PROGRAMFILES(X86)'] || '', 'Google\\Chrome\\Application\\chrome.exe'),
  ].filter(Boolean);

  for (const p of candidates) {
    if (fileExists(p)) return p;
  }

  throw new Error(
    'Chrome executable not found. Pass --chrome-path "C:\\\\Path\\\\to\\\\chrome.exe"'
  );
}

function getDefaultUserDataDir() {
  const localAppData = process.env.LOCALAPPDATA || path.join(os.homedir(), 'AppData', 'Local');
  return path.join(localAppData, 'Google', 'Chrome', 'RemoteControl');
}

function isDefaultChromeUserDataDir(dirPath) {
  if (!dirPath) return false;

  const localAppData = process.env.LOCALAPPDATA || path.join(os.homedir(), 'AppData', 'Local');
  const defaultDir = path.join(localAppData, 'Google', 'Chrome', 'User Data');

  return path.resolve(dirPath).toLowerCase() === path.resolve(defaultDir).toLowerCase();
}

async function fetchJson(url) {
  try {
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

async function isDebuggerUp(port) {
  const json = await fetchJson(`http://127.0.0.1:${port}/json/version`);
  return !!json;
}

async function waitForDebugger(port, timeoutMs) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await isDebuggerUp(port)) return true;
    await sleep(500);
  }
  return false;
}

function launchChromeWithProfile({
  chromePath,
  port,
  userDataDir,
  profileDirectory,
}) {
  const args = [
    `--remote-debugging-port=${port}`,
    '--remote-debugging-address=127.0.0.1',
    '--no-first-run',
    '--no-default-browser-check',
    '--new-window',
    '--start-maximized',
    '--window-size=1600,1100',
    `--user-data-dir=${userDataDir}`,
    `--profile-directory=${profileDirectory}`,
    'about:blank',
  ];

  const child = spawn(chromePath, args, {
    detached: true,
    stdio: 'ignore',
    windowsHide: false,
  });

  child.unref();
}

async function getBrowser({
  chromePath,
  port,
  userDataDir,
  profileDirectory,
}) {
  if (!(await isDebuggerUp(port))) {
    launchChromeWithProfile({
      chromePath,
      port,
      userDataDir,
      profileDirectory,
    });

    const ok = await waitForDebugger(port, CONFIG.connectTimeoutMs);
    if (!ok) {
      throw new Error(
        [
          `Could not start/connect to Chrome on port ${port}.`,
          'Most often this happens because regular Chrome is already open and the profile is locked.',
          'Close all Chrome windows and run again.',
        ].join(' ')
      );
    }
  }

  return puppeteer.connect({
    browserURL: `http://127.0.0.1:${port}`,
    defaultViewport: null,
    protocolTimeout: CONFIG.protocolTimeoutMs,
  });
}

function parseVideosFromArgs(args) {
  if (args['videos-b64']) {
    try {
      const decoded = Buffer.from(args['videos-b64'], 'base64').toString('utf8');
      const parsed = JSON.parse(decoded);
      if (!Array.isArray(parsed)) throw new Error('Decoded value is not an array');
      return parsed;
    } catch (err) {
      throw new Error(`Could not parse --videos-b64: ${err.message}`);
    }
  }

  if (args['videos-json']) {
    try {
      const parsed = JSON.parse(args['videos-json']);
      if (!Array.isArray(parsed)) throw new Error('Value is not an array');
      return parsed;
    } catch (err) {
      throw new Error(`Could not parse --videos-json: ${err.message}`);
    }
  }

  throw new Error('Pass --videos-b64 or --videos-json');
}

async function fetchBlob(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), CONFIG.previewFetchTimeoutMs);
  try {
    const res = await fetch(url, { cache: 'no-store', signal: controller.signal });
    if (!res.ok) return null;
    const arrayBuffer = await res.arrayBuffer();
    return {
      buffer: Buffer.from(arrayBuffer),
      contentType: res.headers.get('content-type') || '',
    };
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchBlobViaBrowser(page, url) {
  let tempPage;

  try {
    tempPage = await page.browser().newPage();
    const response = await tempPage.goto(url, {
      waitUntil: 'networkidle2',
      timeout: CONFIG.previewFetchTimeoutMs,
    });

    if (!response || !response.ok()) return null;

    const buffer = await response.buffer();

    return {
      buffer,
      contentType: response.headers()['content-type'] || '',
    };
  } catch {
    return null;
  } finally {
    if (tempPage) {
      try {
        await tempPage.close();
      } catch {}
    }
  }
}

function inferExt(url, contentType) {
  const ct = String(contentType || '').toLowerCase();
  const u = String(url || '').toLowerCase();

  if (ct.includes('webp') || u.includes('.webp')) return 'webp';
  if (ct.includes('png') || u.includes('.png')) return 'png';
  return 'jpg';
}

async function savePreview(page, previewCandidates, videoId, outDir) {
  await waitIfPaused(`Preview ${videoId}`);
  console.log(`[NODE ${videoId}] Saving preview`);
  const candidates = [
    ...previewCandidates.filter(Boolean),
    `https://i.ytimg.com/vi/${videoId}/maxresdefault.jpg`,
    `https://i.ytimg.com/vi/${videoId}/sddefault.jpg`,
    `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
    `https://i.ytimg.com/vi_webp/${videoId}/maxresdefault.webp`,
    `https://i.ytimg.com/vi_webp/${videoId}/sddefault.webp`,
  ];

  for (const url of candidates) {
    await waitIfPaused(`Preview ${videoId}`);
    let blob = await fetchBlobViaBrowser(page, url);
    if (!blob || !blob.buffer || blob.buffer.length < 1000) {
      blob = await fetchBlob(url);
    }
    if (!blob || !blob.buffer || blob.buffer.length < 1000) continue;

    const ext = inferExt(url, blob.contentType);
    const fileName = `preview.${ext}`;
    const fullPath = path.join(outDir, fileName);
    fs.writeFileSync(fullPath, blob.buffer);
    console.log(`[NODE ${videoId}] Preview saved: ${fileName}`);
    return fileName;
  }

  console.log(`[NODE ${videoId}] Preview not found`);
  return '';
}

function attachPageDebugLogging(page, task) {
  const prefix = task?.url ? extractVideoId(task.url) : 'page';

  const onConsole = (msg) => {
    const text = msg.text();
    if (!text) return;
    console.log(`[PAGE ${prefix}] ${text}`);
  };

  const onPageError = (err) => {
    console.error(`[PAGE ${prefix} ERROR] ${err.message}`);
  };

  page.on('console', onConsole);
  page.on('pageerror', onPageError);

  return () => {
    page.off('console', onConsole);
    page.off('pageerror', onPageError);
  };
}

async function ensurePageBridges(page) {
  if (PAGE_BRIDGES_READY.has(page)) return;

  await page.exposeFunction('__ytParserRequestStop', async (reason) => {
    requestStop(reason);
    return true;
  });

  await page.exposeFunction('__ytParserSetPaused', async (paused, reason) => {
    setPaused(paused, reason);
    return true;
  });

  PAGE_BRIDGES_READY.add(page);
}

async function getWorkerPage(browser) {
  const pages = await browser.pages();
  const reusable =
    pages.find((page) => page.url() === 'about:blank') ||
    pages.find((page) => page.url().startsWith('chrome://newtab'));

  if (reusable) return reusable;
  return browser.newPage();
}

async function collectFromPage(page, task) {
  page.setDefaultNavigationTimeout(CONFIG.navigationTimeoutMs);
  await ensurePageBridges(page);

  await page.goto(task.url, {
    waitUntil: 'domcontentloaded',
    timeout: CONFIG.navigationTimeoutMs,
  });

  await sleep(2500);

  const result = await page.evaluate(async (taskLabel, taskUrl, cfg) => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const overlayEnabled = cfg.pageDebugOverlay !== false;
    let stopRequested = false;
    let pauseRequested = false;
    const debugState = {
      lines: [],
      step: 'Initializing',
      detail: '',
      body: null,
      pauseButton: null,
      stopButton: null,
    };

    function assertNotStopped() {
      if (stopRequested) {
        throw new Error('Stopped from debug overlay');
      }
    }

    async function waitWhilePaused() {
      while (pauseRequested) {
        assertNotStopped();
        await sleep(250);
      }
    }

    function stamp() {
      try {
        return new Date().toLocaleTimeString('ru-RU', { hour12: false });
      } catch {
        return new Date().toISOString().slice(11, 19);
      }
    }

    function ensureDebugOverlay() {
      if (!overlayEnabled) return null;
      if (debugState.body?.isConnected) return debugState.body;

      const box = document.createElement('div');
      box.id = '__yt_parser_debug_overlay__';
      box.style.position = 'fixed';
      box.style.top = '12px';
      box.style.right = '12px';
      box.style.width = '420px';
      box.style.maxHeight = '45vh';
      box.style.padding = '12px 14px';
      box.style.background = 'rgba(8, 12, 18, 0.82)';
      box.style.backdropFilter = 'blur(8px)';
      box.style.color = '#d7f7db';
      box.style.border = '1px solid rgba(120, 255, 170, 0.28)';
      box.style.borderRadius = '12px';
      box.style.boxShadow = '0 18px 40px rgba(0, 0, 0, 0.35)';
      box.style.fontFamily = 'Consolas, Menlo, monospace';
      box.style.fontSize = '12px';
      box.style.lineHeight = '1.45';
      box.style.whiteSpace = 'pre-wrap';
      box.style.wordBreak = 'break-word';
      box.style.zIndex = '2147483647';
      box.style.pointerEvents = 'auto';

      const header = document.createElement('div');
      header.style.display = 'flex';
      header.style.alignItems = 'center';
      header.style.justifyContent = 'space-between';
      header.style.gap = '10px';
      header.style.marginBottom = '8px';

      const title = document.createElement('div');
      title.textContent = 'YT parser debug';
      title.style.fontSize = '11px';
      title.style.textTransform = 'uppercase';
      title.style.letterSpacing = '0.08em';
      title.style.color = '#8ef0a7';

      const actions = document.createElement('div');
      actions.style.display = 'flex';
      actions.style.alignItems = 'center';
      actions.style.gap = '8px';

      const pauseButton = document.createElement('button');
      pauseButton.type = 'button';
      pauseButton.textContent = 'Pause';
      pauseButton.style.padding = '6px 10px';
      pauseButton.style.border = '1px solid rgba(255, 214, 120, 0.45)';
      pauseButton.style.borderRadius = '8px';
      pauseButton.style.background = 'rgba(95, 74, 8, 0.9)';
      pauseButton.style.color = '#ffe9b5';
      pauseButton.style.font = 'inherit';
      pauseButton.style.fontSize = '11px';
      pauseButton.style.cursor = 'pointer';
      pauseButton.style.pointerEvents = 'auto';
      pauseButton.style.flex = '0 0 auto';
      pauseButton.addEventListener('click', async (event) => {
        event.preventDefault();
        event.stopPropagation();
        pauseRequested = !pauseRequested;
        pauseButton.textContent = pauseRequested ? 'Resume' : 'Pause';
        debugLog(pauseRequested ? 'Paused' : 'Resumed', 'toggle from overlay');
        if (pauseRequested) {
          debugState.detail = 'paused from overlay';
        }
        try {
          if (typeof window.__ytParserSetPaused === 'function') {
            await window.__ytParserSetPaused(pauseRequested, 'Pause button toggled in overlay');
          }
        } catch (err) {
          console.error(`Pause toggle failed: ${err.message}`);
        }
        renderDebugOverlay();
      });

      const stopButton = document.createElement('button');
      stopButton.type = 'button';
      stopButton.textContent = 'Stop';
      stopButton.style.padding = '6px 10px';
      stopButton.style.border = '1px solid rgba(255, 120, 120, 0.45)';
      stopButton.style.borderRadius = '8px';
      stopButton.style.background = 'rgba(110, 18, 18, 0.9)';
      stopButton.style.color = '#ffd5d5';
      stopButton.style.font = 'inherit';
      stopButton.style.fontSize = '11px';
      stopButton.style.cursor = 'pointer';
      stopButton.style.pointerEvents = 'auto';
      stopButton.style.flex = '0 0 auto';
      stopButton.addEventListener('click', async (event) => {
        event.preventDefault();
        event.stopPropagation();
        stopRequested = true;
        debugState.step = 'Stopping';
        debugState.detail = 'stop requested from overlay';
        stopButton.disabled = true;
        stopButton.textContent = 'Stopping...';
        try {
          if (typeof window.__ytParserRequestStop === 'function') {
            await window.__ytParserRequestStop('Stop button clicked in overlay');
          }
        } catch (err) {
          console.error(`Stop request failed: ${err.message}`);
        }
        renderDebugOverlay();
      });

      header.appendChild(title);
      actions.appendChild(pauseButton);
      actions.appendChild(stopButton);
      header.appendChild(actions);

      const body = document.createElement('div');
      body.style.maxHeight = 'calc(45vh - 28px)';
      body.style.overflow = 'hidden';

      box.appendChild(header);
      box.appendChild(body);
      document.documentElement.appendChild(box);

      debugState.body = body;
      debugState.pauseButton = pauseButton;
      debugState.stopButton = stopButton;
      return body;
    }

    function renderDebugOverlay() {
      if (!overlayEnabled) return;
      ensureDebugOverlay();
      if (!debugState.body) return;

      const rows = [
        `[${stamp()}] ${debugState.step}${debugState.detail ? ` | ${debugState.detail}` : ''}`,
        ...debugState.lines.slice(-10),
      ];

      debugState.body.textContent = rows.join('\n');
    }

    function debugLog(message, detail = '') {
      const line = `[${stamp()}] ${message}${detail ? ` | ${detail}` : ''}`;
      debugState.lines.push(line);
      console.log(line);
      renderDebugOverlay();
    }

    function debugStep(step, detail = '') {
      debugState.step = step;
      debugState.detail = detail;
      debugLog(step, detail);
    }

    function sanitizeForJsonLine(value) {
      return String(value || '')
        .replace(/\\/g, '')
        .replace(/[\u0000-\u001F\u007F]/g, ' ')
        .replace(/\u00A0/g, ' ')
        .replace(/"/g, "'")
        .replace(/\s+/g, ' ')
        .trim();
    }

    function sanitizeMultilineForJson(value) {
      return String(value || '')
        .split(/\r?\n/)
        .map((line) => sanitizeForJsonLine(line))
        .filter(Boolean)
        .join('\n');
    }

    function isVisible(el) {
      if (!el) return false;
      const style = getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return (
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        !el.hidden &&
        rect.width > 0 &&
        rect.height > 0
      );
    }

    function clickElement(el) {
      if (!el) return false;

      try {
        el.scrollIntoView({ block: 'center', inline: 'center' });
      } catch {}

      try {
        el.click();
        return true;
      } catch {}

      try {
        el.dispatchEvent(new PointerEvent('pointerdown', { bubbles: true }));
        el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
        el.dispatchEvent(new PointerEvent('pointerup', { bubbles: true }));
        el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
        el.dispatchEvent(
          new MouseEvent('click', {
            bubbles: true,
            cancelable: true,
            view: window,
          })
        );
        return true;
      } catch {}

      return false;
    }

    function getElementText(el) {
      return (
        el?.innerText ||
        el?.textContent ||
        el?.getAttribute?.('aria-label') ||
        el?.getAttribute?.('title') ||
        ''
      )
        .trim()
        .toLowerCase();
    }

    function findButtonsByText(texts, root = document) {
      const needles = texts.map((t) => t.toLowerCase());

      const els = Array.from(
        root.querySelectorAll(
          [
            'button',
            "[role='button']",
            'tp-yt-paper-button',
            'yt-button-shape button',
            'ytd-button-renderer',
            'ytd-menu-service-item-renderer',
          ].join(', ')
        )
      );

      return els.filter((el) => {
        const txt = getElementText(el);
        if (!txt) return false;
        return needles.some((n) => txt.includes(n));
      });
    }

    async function waitForWatchReady(timeoutMs = 30000) {
      debugStep('Waiting for watch page');
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        await waitWhilePaused();
        assertNotStopped();
        const titleEl = document.querySelector('h1 yt-formatted-string, h1.title');
        const flexy = document.querySelector('ytd-watch-flexy');
        if (flexy && titleEl && (titleEl.textContent || '').trim()) {
          debugStep('Watch page ready');
          return true;
        }
        await sleep(250);
      }
      debugStep('Watch page timeout', `${timeoutMs}ms`);
      return false;
    }

    async function acceptConsentIfNeeded() {
      debugStep('Checking consent dialog');
      await waitWhilePaused();
      assertNotStopped();
      const texts = [
        'accept all',
        'i agree',
        'accept',
        'принять',
        'принять все',
        'разрешить все',
        'согласен',
      ];

      const buttons = findButtonsByText(texts);
      for (const btn of buttons) {
        if (!isVisible(btn)) continue;
        const txt = getElementText(btn);
        if (!txt) continue;
        clickElement(btn);
        await sleep(1500);
        debugStep('Consent accepted', txt);
        return true;
      }

      debugStep('Consent not shown');
      return false;
    }

    async function openDescription() {
      debugStep('Opening description');
      await waitWhilePaused();
      assertNotStopped();
      const descriptionBlock =
        document.querySelector('#bottom-row #description') ||
        document.querySelector('ytd-watch-metadata #description') ||
        document.querySelector('#description-inner') ||
        document.querySelector('#bottom-row');

      if (descriptionBlock) {
        clickElement(descriptionBlock);
        await sleep(700);
      }

      const expandBtn =
        document.querySelector('#description-inline-expander tp-yt-paper-button#expand') ||
        document.querySelector('#description-inline-expander #expand') ||
        document.querySelector('tp-yt-paper-button#expand') ||
        document.querySelector('#expand');

      if (expandBtn && isVisible(expandBtn)) {
        clickElement(expandBtn);
        await sleep(1200);
        debugStep('Description expanded');
        return;
      }

      const moreBtn = findButtonsByText(['...ещё', 'ещё', 'more'])[0];
      if (moreBtn) {
        clickElement(moreBtn);
        await sleep(1200);
      }
    }

    function collectTitleAndDescription() {
      const rawTitle =
        document.querySelector('h1 yt-formatted-string, h1.title')?.innerText || '';

      const rawDescription =
        document.querySelector('#description-inline-expander #expanded')?.innerText ||
        document.querySelector('#expanded yt-attributed-string')?.innerText ||
        document.querySelector('#description-inline-expander yt-attributed-string')?.innerText ||
        document.querySelector('#snippet-text')?.innerText ||
        '';

      return {
        videoTitle: sanitizeForJsonLine(rawTitle),
        videoDescription: sanitizeMultilineForJson(rawDescription),
      };
    }

    function getTranscriptPanelCandidates() {
      return [
        ...document.querySelectorAll('ytd-transcript-search-panel-renderer'),
        ...document.querySelectorAll('ytd-engagement-panel-section-list-renderer[target-id*="transcript"]'),
        ...document.querySelectorAll('ytd-engagement-panel-section-list-renderer[target-id="engagement-panel-searchable-transcript"]'),
        ...document.querySelectorAll('ytd-engagement-panel-section-list-renderer[target-id="PAmodern_transcript_view"]'),
        ...document.querySelectorAll('div#panels > ytd-engagement-panel-section-list-renderer'),
      ];
    }

    function isTranscriptLikePanel(panel) {
      if (!panel) return false;

      if (panel.matches?.('ytd-transcript-search-panel-renderer')) return true;

      const targetId = String(panel.getAttribute?.('target-id') || '').toLowerCase();
      if (targetId.includes('transcript')) return true;

      const transcriptRows = panel.querySelectorAll(
        'ytd-transcript-segment-renderer, transcript-segment-view-model'
      ).length;
      if (transcriptRows > 0) return true;

      const searchField = panel.querySelector('input[placeholder*="transcript" i], input[aria-label*="transcript" i]');
      if (searchField) return true;

      const titleText = sanitizeForJsonLine(
        panel.querySelector('#header, #title-container, #title')?.innerText || ''
      ).toLowerCase();
      if (titleText.includes('transcript')) return true;

      return false;
    }

    function isExpandedTranscriptPanel(panel) {
      if (!panel) return false;

      const host =
        panel.closest('ytd-engagement-panel-section-list-renderer') ||
        panel.closest('ytd-transcript-search-panel-renderer') ||
        panel;

      if (!isTranscriptLikePanel(host) && !isTranscriptLikePanel(panel)) return false;

      if (!isVisible(host) && !isVisible(panel)) return false;

      const visibilityAttr = String(
        host.getAttribute('visibility') || panel.getAttribute('visibility') || ''
      ).toUpperCase();

      if (visibilityAttr.includes('HIDDEN') || visibilityAttr.includes('COLLAPSED')) {
        return false;
      }

      const expandedAttr = String(
        host.getAttribute('is-expanded') ||
        host.getAttribute('aria-expanded') ||
        panel.getAttribute('aria-expanded') ||
        ''
      ).toLowerCase();

      if (expandedAttr === 'false') return false;

      return true;
    }

    function findTranscriptPanel(options = {}) {
      const visibleOnly = !!options.visibleOnly;
      const panels = [...new Set(getTranscriptPanelCandidates())];

      for (const panel of panels) {
        if (!isTranscriptLikePanel(panel)) continue;
        if (visibleOnly && !isExpandedTranscriptPanel(panel)) continue;
        return panel;
      }

      return null;
    }

    function scrollTranscriptAreaIntoView(stepIndex = 0) {
      const anchors = [
        document.querySelector('ytd-video-description-transcript-section-renderer'),
        document.querySelector('#description'),
        document.querySelector('#description-inline-expander'),
        document.querySelector('#meta'),
        document.querySelector('#below'),
        document.querySelector('#panels'),
      ].filter(Boolean);

      const anchor = anchors[0];
      if (anchor) {
        try {
          anchor.scrollIntoView({ block: 'start', inline: 'nearest' });
        } catch {}
      }

      const delta = Math.max(350, Math.min(900, 350 + stepIndex * 120));
      try {
        window.scrollBy(0, delta);
      } catch {}
    }

    async function clickTranscriptButton() {
      debugStep('Searching transcript button');
      const directSelectors = [
        'ytd-video-description-transcript-section-renderer button[aria-label*="transcript" i]',
        'ytd-video-description-transcript-section-renderer yt-button-shape button',
        'ytd-video-description-transcript-section-renderer button',
        'ytd-video-description-transcript-section-renderer #primary-button',
        'ytd-video-description-transcript-section-renderer #button-container',
      ];

      for (const selector of directSelectors) {
        const directBtn = document.querySelector(selector);
        if (!directBtn || !isVisible(directBtn)) continue;
        const txt = sanitizeForJsonLine(getElementText(directBtn));
        clickElement(directBtn);
        await sleep(1500);
        if (findTranscriptPanel({ visibleOnly: true })) {
          debugStep('Transcript button clicked', txt || selector);
          return true;
        }
      }
      const texts = [
        'показать текст видео',
        'расшифровка',
        'текст видео',
        'show transcript',
        'transcript',
      ];

      const roots = [
        document.querySelector('ytd-video-description-transcript-section-renderer'),
        document.querySelector('#panels'),
        document,
      ].filter(Boolean);

      for (let attempt = 0; attempt < 8; attempt++) {
        await waitWhilePaused();
        assertNotStopped();
        debugStep('Searching transcript button', `attempt=${attempt + 1}/8`);

        for (const root of roots) {
          const buttons = findButtonsByText(texts, root);
          for (const btn of buttons) {
            if (!isVisible(btn)) continue;
            const txt = sanitizeForJsonLine(getElementText(btn));
            clickElement(btn);
            await sleep(1500);
            if (findTranscriptPanel({ visibleOnly: true })) {
              debugStep('Transcript button clicked', txt);
              return true;
            }
          }
        }

        scrollTranscriptAreaIntoView(attempt);
        await sleep(900);
      }

      debugStep('Transcript button not found');
      return !!findTranscriptPanel({ visibleOnly: true });
    }

    async function waitForTranscriptPanel(timeoutMs) {
      debugStep('Waiting transcript panel');
      const start = Date.now();
      while (Date.now() - start < timeoutMs) {
        await waitWhilePaused();
        assertNotStopped();
        const panel = findTranscriptPanel({ visibleOnly: true });
        if (panel) {
          debugStep('Transcript panel ready');
          return panel;
        }
        await sleep(250);
      }
      debugStep('Transcript panel timeout', `${timeoutMs}ms`);
      return null;
    }

    function extractTranscriptItems(panel) {
      if (!panel) return [];

      const items = [];
      const seen = new Set();

      function pushItem(timestamp, text) {
        const cleanTimestamp = sanitizeForJsonLine(timestamp);
        const cleanText = sanitizeForJsonLine(text);
        if (!cleanText) return;

        const key = `${cleanTimestamp}__${cleanText}`;
        if (seen.has(key)) return;
        seen.add(key);
        items.push({ timestamp: cleanTimestamp, text: cleanText });
      }

      const oldRows = Array.from(panel.querySelectorAll('ytd-transcript-segment-renderer'));
      for (const row of oldRows) {
        const timestamp =
          row.querySelector('.segment-timestamp')?.textContent || '';
        const text =
          row.querySelector('yt-formatted-string.segment-text')?.textContent ||
          row.querySelector('.segment-text')?.textContent ||
          '';
        pushItem(timestamp, text);
      }

      const modernRows = Array.from(panel.querySelectorAll('transcript-segment-view-model'));
      for (const row of modernRows) {
        const timestamp =
          row.querySelector('.ytwTranscriptSegmentViewModelTimestamp')?.textContent ||
          row.querySelector("[class*='TranscriptSegmentViewModelTimestamp']")?.textContent ||
          '';
        const text =
          row.querySelector("span[role='text']")?.textContent ||
          row.querySelector('.yt-core-attributed-string')?.textContent ||
          '';
        pushItem(timestamp, text);
      }

      return items;
    }

    function getTranscriptScrollTargets(panel) {
      const candidates = [
        panel.querySelector('#segments-container'),
        panel.querySelector('#body'),
        panel.querySelector('#content'),
        panel.querySelector('.ytSectionListRendererContents'),
        panel.querySelector('yt-section-list-renderer'),
        panel.querySelector('ytd-transcript-segment-list-renderer'),
        panel,
      ].filter(Boolean);

      return [...new Set(candidates)];
    }

    async function loadFullTranscript(panel, cfg) {
      const scrollTargets = getTranscriptScrollTargets(panel);

      let stableRounds = 0;
      let lastSignature = '';
      let best = [];
      const start = Date.now();
      let lastReportedSignature = '';

      debugStep('Loading transcript', `targets=${scrollTargets.length}`);

      while (Date.now() - start < cfg.transcriptLoadTimeoutMs) {
        await waitWhilePaused();
        assertNotStopped();
        const current = extractTranscriptItems(panel);
        if (current.length > best.length) best = current;

        const last = current[current.length - 1];
        const signature = `${current.length}|${last?.timestamp || ''}|${last?.text || ''}`;

        if (signature === lastSignature) {
          stableRounds++;
        } else {
          stableRounds = 0;
          lastSignature = signature;
        }

        const reportSignature = `${current.length}|${stableRounds}|${best.length}`;
        if (reportSignature !== lastReportedSignature) {
          lastReportedSignature = reportSignature;
          debugStep(
            'Loading transcript',
            `segments=${current.length}, best=${best.length}, stable=${stableRounds}/${cfg.transcriptStableRounds}`
          );
        }

        for (const target of scrollTargets) {
          try {
            target.scrollTop = target.scrollHeight || 999999;
          } catch {}
        }

        if (!scrollTargets.length) {
          window.scrollBy(0, 500);
        }

        await sleep(cfg.transcriptScrollPauseMs);

        if (stableRounds >= cfg.transcriptStableRounds) break;
      }

      debugStep('Transcript load finished', `segments=${best.length}`);
      return best;
    }

    async function clickAllMatching(selectors, perRoundLimit = 200) {
      let clicked = 0;

      for (let i = 0; i < perRoundLimit; i++) {
        await waitWhilePaused();
        assertNotStopped();

        const candidates = selectors.flatMap((selector) =>
          Array.from(document.querySelectorAll(selector))
        );

        const btn = candidates.find(isVisible);
        if (!btn) break;

        const ok = clickElement(btn);
        if (!ok) break;

        clicked++;
        await sleep(cfg.commentsClickPauseMs);
      }

      return clicked;
    }

    function getCommentsRoot() {
      return document.querySelector('ytd-comments') || document.querySelector('#comments');
    }

    function focusCommentsRoot() {
      const commentsRoot = getCommentsRoot();
      if (!commentsRoot) return false;

      try {
        commentsRoot.scrollIntoView({ behavior: 'instant', block: 'start' });
      } catch {}

      return true;
    }

    async function ensureCommentsVisible() {
      debugStep('Opening comments');

      for (let i = 0; i < 12; i++) {
        await waitWhilePaused();
        assertNotStopped();

        focusCommentsRoot();
        await sleep(1000);
        await expandEverythingVisible();

        if (getLoadedCommentNodes().length > 0) return;

        window.scrollBy(0, Math.max(Math.floor(window.innerHeight * 0.75), 650));
        await sleep(1200);
      }
    }

    async function expandEverythingVisible() {
      const commentsRoot = getCommentsRoot() || document;

      function isCommentExpansionButton(el) {
        if (!el || !isVisible(el)) return false;
        const text = getElementText(el);
        if (!text) return false;
        if (text === 'ответить' || text === 'reply') return false;
        if (text.includes('скрыть ответы') || text.includes('hide replies')) return false;
        if (text.includes('ещ') || text.includes('more')) return true;
        if (text.includes('показать ответы') || text.includes('show replies')) return true;
        if (/\b\d+\s*ответ/.test(text) || /\b\d+\s*repl/.test(text)) return true;
        if (text.includes('ответов') || text.includes('replies')) return true;
        return false;
      }

      for (let round = 0; round < cfg.commentsMaxExpandRounds; round++) {
        await waitWhilePaused();
        assertNotStopped();

        let clickedThisRound = 0;

        clickedThisRound += await clickAllMatching([
          'ytd-expander tp-yt-paper-button#more',
          'ytd-expander #more',
        ]);

        clickedThisRound += await clickAllMatching([
          'ytd-button-renderer#more-replies button',
          'ytd-button-renderer#more-replies-sub-thread button',
        ]);

        clickedThisRound += await clickAllMatching([
          'ytd-continuation-item-renderer #button button',
          'ytd-continuation-item-renderer button',
        ]);

        const genericButtons = Array.from(
          commentsRoot.querySelectorAll('button, [role="button"], tp-yt-paper-button')
        );
        for (const btn of genericButtons) {
          if (!isCommentExpansionButton(btn)) continue;
          if (!clickElement(btn)) continue;
          clickedThisRound++;
          await sleep(cfg.commentsClickPauseMs);
        }

        if (clickedThisRound === 0) break;
        await sleep(700);
      }
    }

    function getLoadedCommentNodes() {
      const root = document.querySelector('ytd-comments') || document.querySelector('#comments') || document;
      const selectors = [
        'ytd-comment-thread-renderer #content-text',
        'ytd-comment-view-model #content-text',
        '#comments #content-text',
      ];

      const nodes = selectors.flatMap((selector) => Array.from(root.querySelectorAll(selector)));
      const unique = [];
      const seen = new Set();

      for (const node of nodes) {
        if (!node || seen.has(node)) continue;
        seen.add(node);

        const text = sanitizeForJsonLine(node.innerText || node.textContent || '');
        if (!text) continue;

        const owner = node.closest('ytd-comment-thread-renderer, ytd-comment-view-model, ytd-comment-renderer');
        if (!owner) continue;

        unique.push(node);
      }

      return unique;
    }

    function getLastLoadedCommentNode() {
      const nodes = getLoadedCommentNodes();
      return nodes.length ? nodes[nodes.length - 1] : null;
    }

    function hasPendingCommentControls() {
      const root = getCommentsRoot();
      if (!root) return false;

      const spinners = Array.from(
        root.querySelectorAll(
          'ytd-continuation-item-renderer tp-yt-paper-spinner-lite, ytd-continuation-item-renderer #spinner, yt-ghost-comments'
        )
      ).some((el) => {
        const rect = el.getBoundingClientRect();
        return rect.width > 0 || rect.height > 0;
      });

      if (spinners) return true;

      const buttons = Array.from(
        root.querySelectorAll(
          [
            'ytd-continuation-item-renderer button',
            'ytd-continuation-item-renderer [role="button"]',
            'ytd-button-renderer#more-replies button',
            'ytd-button-renderer#more-replies-sub-thread button',
            'button',
            '[role="button"]',
          ].join(', ')
        )
      );

      return buttons.some((el) => {
        if (!isVisible(el)) return false;
        const text = getElementText(el);
        if (!text) return false;
        if (text === 'ответить' || text === 'reply') return false;
        if (text.includes('ещ') || text.includes('more')) return true;
        if (text.includes('показать ответы') || text.includes('show replies')) return true;
        if (/\b\d+\s*ответ/.test(text) || /\b\d+\s*repl/.test(text)) return true;
        if (text.includes('ответов') || text.includes('replies')) return true;
        return false;
      });
    }

    async function scrollCommentsUntilLoaded() {
      let prevCount = 0;
      let prevHeight = 0;
      let stableCountRounds = 0;
      let stableHeightRounds = 0;
      const seenLines = [];
      const seenKeys = new Set();

      debugStep('Loading comments');

      for (let round = 0; round < cfg.commentsMaxScrollRounds; round++) {
        await waitWhilePaused();
        assertNotStopped();

        focusCommentsRoot();
        await expandEverythingVisible();

        for (const entry of extractCommentsEntries()) {
          if (!entry?.key || seenKeys.has(entry.key)) continue;
          seenKeys.add(entry.key);
          seenLines.push(entry.line);
        }

        const count = seenLines.length;
        const height = document.documentElement.scrollHeight;
        const pendingControls = hasPendingCommentControls();

        if (count === prevCount) stableCountRounds++;
        else {
          stableCountRounds = 0;
          prevCount = count;
        }

        if (height === prevHeight) stableHeightRounds++;
        else {
          stableHeightRounds = 0;
          prevHeight = height;
        }

        debugStep(
          'Loading comments',
          `items=${count}, pending=${pendingControls ? 'yes' : 'no'}, stable=${Math.min(stableCountRounds, stableHeightRounds)}/${cfg.commentsStableRoundsToStop}`
        );

        if (count === 0) {
          focusCommentsRoot();
        } else {
          const lastNode = getLastLoadedCommentNode();
          if (lastNode) {
            try {
              lastNode.scrollIntoView({ behavior: 'instant', block: 'end' });
            } catch {}
          }
          window.scrollBy(0, Math.max(Math.floor(window.innerHeight * 0.65), 520));
        }

        await sleep(cfg.commentsScrollPauseMs);
        await expandEverythingVisible();

        if (
          stableCountRounds >= cfg.commentsStableRoundsToStop &&
          stableHeightRounds >= cfg.commentsStableRoundsToStop &&
          !hasPendingCommentControls()
        ) {
          break;
        }
      }

      await expandEverythingVisible();
      return seenLines;
    }

    function extractCommentsEntries() {
      const nodes = getLoadedCommentNodes();
      const entries = [];
      const occurrences = new Map();

      for (const [index, node] of nodes.entries()) {
        const text = sanitizeForJsonLine(node.innerText || node.textContent || '');
        if (!text) continue;

        const isReply = !!node.closest('ytd-comment-replies-renderer, ytd-comment-thread-renderer[is-sub-thread]');
        const line = isReply ? `> ${text}` : text;
        const seenCount = occurrences.get(line) || 0;
        occurrences.set(line, seenCount + 1);
        entries.push({
          key: `${index}:${line}:${seenCount}`,
          line,
        });
      }

      return entries;
    }

    async function collectCommentsText() {
      try {
        await ensureCommentsVisible();
        const lines = await scrollCommentsUntilLoaded();
        if (lines.length === 0) {
          lines.push(...extractCommentsEntries().map((entry) => entry.line));
        }
        const text = sanitizeMultilineForJson(lines.join('\n'));
        debugStep('Comments loaded', `items=${lines.length}, chars=${text.length}`);
        return text;
      } catch (err) {
        debugStep('Comments failed', err.message);
        return '';
      }
    }

    function getPreviewCandidates(videoId) {
      const ogImage =
        document.querySelector('meta[property="og:image"]')?.content ||
        document.querySelector('link[rel="image_src"]')?.href ||
        '';

      return [
        ogImage,
        `https://img.youtube.com/vi/${videoId}/maxresdefault.jpg`,
        `https://i.ytimg.com/vi/${videoId}/maxresdefault.jpg`,
        `https://i.ytimg.com/vi/${videoId}/sddefault.jpg`,
        `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
        `https://i.ytimg.com/vi_webp/${videoId}/maxresdefault.webp`,
      ].filter(Boolean);
    }

    function extractVideoId(url) {
      try {
        const u = new URL(url);
        if (u.hostname.includes('youtu.be')) {
          return u.pathname.replace(/^\/+/, '').trim();
        }
        if (u.searchParams.get('v')) return u.searchParams.get('v');
      } catch {}

      const m = String(url || '').match(/[?&]v=([a-zA-Z0-9_-]+)/);
      return m ? m[1] : 'video';
    }

    debugStep('Video started', taskUrl);

    await waitForWatchReady(30000);
    await acceptConsentIfNeeded();
    await openDescription();

    let panel = findTranscriptPanel({ visibleOnly: true });

    if (!panel) {
      debugStep('Opening transcript after description');
      await clickTranscriptButton();
      panel = await waitForTranscriptPanel(cfg.transcriptPanelTimeoutMs);
    } else {
      debugStep('Transcript panel already open');
    }

    const meta = collectTitleAndDescription();
    const videoId = extractVideoId(taskUrl);
    debugStep('Metadata collected', meta.videoTitle || videoId);

    let segments = [];
    if (panel) {
      await sleep(1200);
      segments = await loadFullTranscript(panel, cfg);
      if (!segments.length) {
        debugStep('Transcript visible but empty', 'retrying after description');
        await clickTranscriptButton();
        panel = await waitForTranscriptPanel(Math.min(10000, cfg.transcriptPanelTimeoutMs));
        if (panel) {
          await sleep(1200);
          segments = await loadFullTranscript(panel, cfg);
        }
      }
    } else {
      debugStep('Transcript unavailable', 'retrying after description');
      await clickTranscriptButton();
      panel = await waitForTranscriptPanel(Math.min(10000, cfg.transcriptPanelTimeoutMs));
      if (panel) {
        await sleep(1200);
        segments = await loadFullTranscript(panel, cfg);
      } else {
        debugStep('Transcript unavailable');
      }
    }

    const videoTranscript = sanitizeForJsonLine(segments.map((x) => x.text).join(' '));
    const videoTranscriptTimed = sanitizeMultilineForJson(
      segments.map((x) => `[${x.timestamp || '??:??'}] ${x.text}`).join('\n')
    );
    const videoCommets = await collectCommentsText();

    debugStep(
      'Video finished',
      `segments=${segments.length}, transcriptChars=${videoTranscript.length}, commentsChars=${videoCommets.length}`
    );

    return {
      videoId,
      videoPerformanceLabel: sanitizeForJsonLine(taskLabel || ''),
      videoTitle: meta.videoTitle || '',
      videoDescription: meta.videoDescription || '',
      videoTranscript: videoTranscript || '',
      videoTranscriptTimed: videoTranscriptTimed || '',
      videoCommets: videoCommets || '',
      previewCandidates: getPreviewCandidates(videoId),
      sourceUrl: taskUrl,
    };
  }, task.label || '', task.url, CONFIG);

  return result;
}

async function processOneVideo(browser, page, task, outputRoot) {
  await waitIfPaused(`Before video ${task.url}`);
  ensureNotStopped(`Before video ${task.url}`);
  const videoId = extractVideoId(task.url);
  const detachDebugLogging = attachPageDebugLogging(page, task);
  const successDir = path.join(outputRoot, `${videoId}`);
  const failedDir = path.join(outputRoot, `FAILED-${videoId}`);

  try {
    const data = await withTimeout(
      () => collectFromPage(page, task),
      CONFIG.perVideoTimeoutMs,
      `Video ${videoId}`
    );

    const resultJson = {
      videoPerformanceLabel: data.videoPerformanceLabel || '',
      videoTitle: sanitizeForJsonLine(data.videoTitle || ''),
      videoDescription: sanitizeMultilineForJson(data.videoDescription || ''),
      videoTranscript: sanitizeForJsonLine(data.videoTranscript || ''),
      videoTranscriptTimed: sanitizeMultilineForJson(data.videoTranscriptTimed || ''),
      videoCommets: sanitizeMultilineForJson(data.videoCommets || ''),
      previewFileName: '',
      sourceUrl: task.url,
      videoId,
    };

    const transcriptMissing = !resultJson.videoTranscript && !resultJson.videoTranscriptTimed;
    const commentsMissing = !resultJson.videoCommets;
    const failureReasons = [];

    if (transcriptMissing) failureReasons.push('Transcript not collected');
    if (commentsMissing) failureReasons.push('Comments not collected');

    const failed = failureReasons.length > 0;
    const failureMessage = failureReasons.join('; ');
    const targetDir = failed ? failedDir : successDir;

    ensureDir(targetDir);
    console.log(`[NODE ${videoId}] Output dir ready: ${targetDir}`);

    await waitIfPaused(`Before preview ${videoId}`);
    const previewFileName = await savePreview(page, data.previewCandidates || [], videoId, targetDir);
    resultJson.previewFileName = previewFileName || '';
    resultJson.status = failed ? 'FAILED' : 'OK';

    if (failed) {
      resultJson.error = failureMessage;
      fs.writeFileSync(
        path.join(targetDir, '_error.json'),
        JSON.stringify(
          {
            ok: false,
            error: failureMessage,
            reasons: failureReasons,
            videoId,
            sourceUrl: task.url,
          },
          null,
          2
        ),
        'utf8'
      );
    }

    fs.writeFileSync(
      path.join(targetDir, 'videoData.json'),
      JSON.stringify(resultJson, null, 2),
      'utf8'
    );
    console.log(`[NODE ${videoId}] videoData.json written`);

    if (failed) {
      console.error(`[FAIL] ${task.url}`);
      console.error(`       ${failureMessage}`);

      return {
        ok: false,
        folder: targetDir,
        videoId,
        error: failureMessage,
        url: task.url,
      };
    }

    console.log(`[OK] ${task.url}`);
    console.log(`     saved to: ${targetDir}`);

    return {
      ok: true,
      folder: targetDir,
      videoId,
      title: resultJson.videoTitle,
    };
  } catch (err) {
    ensureDir(failedDir);
    fs.writeFileSync(
      path.join(failedDir, '_error.json'),
      JSON.stringify(
        {
          ok: false,
          error: err.message,
          videoId,
          sourceUrl: task.url,
        },
        null,
        2
      ),
      'utf8'
    );

    console.error(`[FAIL] ${task.url}`);
    console.error(`       ${err.message}`);

    return {
      ok: false,
      folder: failedDir,
      videoId,
      error: err.message,
      url: task.url,
    };
  } finally {
    detachDebugLogging();
  }
}

async function main() {
  const args = parseArgs(process.argv);

  const tasks = parseVideosFromArgs(args).map((item) => {
    if (typeof item === 'string') {
      return { url: item, label: '' };
    }
    return {
      url: item.url,
      label: item.label || '',
    };
  });

  const outputRoot = args.output;
  if (!outputRoot) {
    throw new Error('Pass --output "C:\\\\path\\\\to\\\\folder"');
  }

  ensureDir(outputRoot);

  const chromePath = args['chrome-path'] || getDefaultChromePath();
  const port = Number(args.port || CONFIG.debuggerPort);
  const userDataDir = args['user-data-dir'] || getDefaultUserDataDir();
  const profileDirectory = args['profile-directory'] || 'Default';

  console.log('Chrome path:', chromePath);
  console.log('User data dir:', userDataDir);
  console.log('Profile directory:', profileDirectory);
  console.log('Debugger port:', port);
  console.log('Output:', outputRoot);
  console.log('Videos:', tasks.length);

  if (isDefaultChromeUserDataDir(userDataDir)) {
    console.warn('');
    console.warn('[WARN] You are using the main Chrome "User Data" directory.');
    console.warn('[WARN] If regular Chrome is already open, Windows will usually forward the launch');
    console.warn('[WARN] request to the existing browser process and remote debugging will not start.');
    console.warn('[WARN] Prefer a dedicated automation dir, for example:');
    console.warn('[WARN]   --user-data-dir "C:\\Users\\V\\AppData\\Local\\Google\\Chrome\\RemoteControl"');
    console.warn('');
  }

  const browser = await getBrowser({
    chromePath,
    port,
    userDataDir,
    profileDirectory,
  });
  const workerPage = await getWorkerPage(browser);

  const summary = [];

  try {
    for (const task of tasks) {
      await waitIfPaused('Main loop');
      if (STOP_STATE.requested) {
        console.warn(`[STOP] Skipping remaining videos: ${STOP_STATE.reason}`);
        break;
      }
      if (!task.url) continue;
      const one = await processOneVideo(browser, workerPage, task, outputRoot);
      summary.push(one);
      if (STOP_STATE.requested) {
        console.warn(`[STOP] Processing interrupted: ${STOP_STATE.reason}`);
        break;
      }
      await waitIfPaused('Between videos');
      await sleep(CONFIG.perVideoDelayMs);
    }
  } finally {
    try {
      await browser.disconnect();
    } catch {}
  }

  const summaryPath = path.join(outputRoot, '_summary.json');
  fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2), 'utf8');

  const okCount = summary.filter((x) => x.ok).length;
  const failCount = summary.filter((x) => !x.ok).length;

  console.log('');
  console.log('Done');
  console.log('Success:', okCount);
  console.log('Failed:', failCount);
  console.log('Summary:', summaryPath);

  process.exit(0);
}

main().catch((err) => {
  console.error(err.stack || err.message || String(err));
  process.exit(1);
});
