// publisherScript.js
// Запуск: 
//   node publisherScript.js --channelName="..." --mainFolderPath="..." --videoFolderName="..." --reuseIndex=2
//
// Требует: npm i chrome-remote-interface
// Chrome запусти так (пример):
//   "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
//      --remote-debugging-port=9333 --remote-debugging-address=127.0.0.1
//      --user-data-dir=C:\\Users\\V\\AppData\\Local\\Google\\Chrome\\RemoteControl
//      --profile-directory=Default --new-window --start-maximized --window-size=1920,1080

const CDP  = require('chrome-remote-interface');
const fs   = require('fs');
const path = require('path');

// ============================ Настройки ============================
const STEP_DELAY     = Number(process.env.STEP_DELAY || 0);  // пауза между шагами (видимый прогресс)
// const GLOBAL_TIMEOUT = Number(process.env.GLOBAL_TIMEOUT || 9999999);
const GLOBAL_TIMEOUT = Number(9999999);

const sleep = ms => new Promise(r => setTimeout(r, ms));
let EXTRA_DELAY_AFTER_MONETIZATION = 0;
async function stepLog(msg) { console.log(`➡️ ${msg}`); await sleep(STEP_DELAY + EXTRA_DELAY_AFTER_MONETIZATION); }

async function withTimeout(promise, ms = GLOBAL_TIMEOUT, msg = 'Timeout') {
  let t; const timeout = new Promise((_,rej)=> t=setTimeout(()=>rej(new Error(msg)), ms));
  try { return await Promise.race([promise, timeout]); } finally { clearTimeout(t); }
}
function sanitizeFileName(name) {
  let n = (name || 'Untitled').replace(/[<>:"/\\|?*\x00-\x1F]/g, ' ');
  n = n.replace(/\s{2,}/g, ' ').trim().replace(/[\. ]+$/g, '');
  if (!n) n = 'video';
  return n;
}
function fileExistsCaseInsensitive(p) {
  if (fs.existsSync(p)) return p;
  const dir = path.dirname(p);
  const base = path.basename(p);
  if (!fs.existsSync(dir)) return null;
  const hit = fs.readdirSync(dir).find(f => f.toLowerCase() === base.toLowerCase());
  return hit ? path.join(dir, hit) : null;
}

// ============================ Аргументы ============================
function parseArgs(argv) {
  const out = { _pos: [] };
  for (let i=0; i<argv.length; i++) {
    const tok = argv[i];
    if (tok.startsWith('--')) {
      const eq = tok.indexOf('=');
      if (eq >= 0) {
        const k = tok.slice(2, eq);
        let v = tok.slice(eq+1).replace(/^["']|["']$/g,'');
        out[k] = v;
      } else {
        const k = tok.slice(2);
        let v = (i+1<argv.length && !argv[i+1].startsWith('--')) ? argv[++i] : 'true';
        out[k] = v.replace(/^["']|["']$/g,'');
      }
    } else {
      out._pos.push(tok.replace(/^["']|["']$/g,''));
    }
  }
  return out;
}
const args = parseArgs(process.argv.slice(2));
let channelName     = args.channelName     ?? args.c ?? args._pos[0];
let mainFolderPath  = args.mainFolderPath  ?? args.m ?? args._pos[1];
let videoFolderName = args.videoFolderName ?? args.v ?? args._pos[2];
const reuseIndex    = Number(args.reuseIndex ?? 2); // по умолчанию кликаем 3-ю карточку

if (!channelName || !mainFolderPath || !videoFolderName) {
  console.error('Usage:\n' +
    '  node publisherScript.js --channelName="..." --mainFolderPath="..." --videoFolderName="..." [--reuseIndex=2]\n' +
    'или позиционно:\n' +
    '  node publisherScript.js "<channelName>" "<mainFolderPath>" "<videoFolderName>" [reuseIndex]');
  process.exit(1);
}
mainFolderPath = path.resolve(mainFolderPath);

// ============================ CDP helpers ============================
async function exists(Runtime, selector) {
  const { result } = await Runtime.evaluate({
    expression: `!!document.querySelector(${JSON.stringify(selector)})`,
    returnByValue: true
  });
  return !!result?.value;
}
async function waitForSelector(Runtime, selector, timeoutMs = GLOBAL_TIMEOUT, poll = 250) {
  const t0 = Date.now();
  while (Date.now()-t0 < timeoutMs) {
    if (await exists(Runtime, selector)) return true;
    await sleep(poll);
  }
  return false;
}
async function waitGone(Runtime, selector, timeoutMs = GLOBAL_TIMEOUT, poll = 250) {
  const t0 = Date.now();
  while (Date.now()-t0 < timeoutMs) {
    if (!(await exists(Runtime, selector))) return true;
    await sleep(poll);
  }
  return false;
}
async function waitForCondition(Runtime, fnBody, timeoutMs = GLOBAL_TIMEOUT, poll = 300) {
  const t0 = Date.now();
  while (Date.now()-t0 < timeoutMs) {
    const { result, exceptionDetails } = await Runtime.evaluate({
      expression: `(() => { ${fnBody} })()`,
      returnByValue: true
    });
    if (!exceptionDetails && result && result.value) return true;
    await sleep(poll);
  }
  return false;
}
async function clickSelector(Runtime, selector) {
  const { result } = await Runtime.evaluate({
    expression: `
      (function(){
        const el = document.querySelector(${JSON.stringify(selector)});
        if (!el) return false;
        (el.closest('button,[role="button"]') || el).click();
        return true;
      })()
    `,
    returnByValue: true
  });
  return !!result?.value;
}
async function clickContainsText(Runtime, selector, text) {
  const { result } = await Runtime.evaluate({
    expression: `
      (function(){
        const want = ${JSON.stringify(text.toLowerCase())};
        const nodes = Array.from(document.querySelectorAll(${JSON.stringify(selector)}));
        const n = nodes.find(e => (e.textContent||'').trim().toLowerCase().includes(want));
        if (!n) return false;
        (n.closest('button,[role="button"],tp-yt-paper-item,a,#content') || n).click();
        return true;
      })()
    `,
    returnByValue: true
  });
  return !!result?.value;
}
async function safeClick(Runtime, selector, tries = 3, pause = 350) {
  for (let i=0;i<tries;i++) {
    if (await clickSelector(Runtime, selector)) return true;
    await sleep(pause);
  }
  return false;
}
async function setContentEditable(Runtime, selector, text) {
  const { result } = await Runtime.evaluate({
    expression: `
      (function(){
        const el = document.querySelector(${JSON.stringify(selector)});
        if (!el) return false;
        const s = ${JSON.stringify(text || '')};
        el.focus();
        el.innerText = s;
        el.dispatchEvent(new InputEvent('input',{bubbles:true}));
        el.dispatchEvent(new Event('change',{bubbles:true}));
        return true;
      })()
    `,
    returnByValue: true
  });
  return !!result?.value;
}
// Shadow DOM-safe file input
async function getLastFileInputNodeId(DOM) {
  const { root } = await DOM.getDocument({ depth: -1, pierce: true });
  const { nodeIds } = await DOM.querySelectorAll({ nodeId: root.nodeId, selector: 'input[type="file"]' });
  if (!nodeIds || nodeIds.length === 0) return null;
  return nodeIds[nodeIds.length - 1];
}
async function uploadFileToLatestInput(DOM, filePath) {
  const nodeId = await getLastFileInputNodeId(DOM);
  if (!nodeId) throw new Error('Не найден input[type=file] (shadow DOM)');
  await DOM.setFileInputFiles({ nodeId, files: [filePath] });
}
// Клик по элементу внутри (в т.ч. закрытого) Shadow DOM через CDP (pierce:true)
async function pierceAndClick(DOM, Runtime, selectors = [], sequence = true) {
  const { root } = await DOM.getDocument({ depth: -1, pierce: true });
  for (const sel of selectors) {
    const { nodeId } = await DOM.querySelector({ nodeId: root.nodeId, selector: sel });
    if (!nodeId) continue;

    try { await DOM.scrollIntoViewIfNeeded({ nodeId }); } catch {}

    const resolved = await DOM.resolveNode({ nodeId });
    const objectId = resolved?.object?.objectId;
    if (!objectId) continue;

    // снять disabled/aria-disabled на всякий
    try {
      await Runtime.callFunctionOn({
        objectId,
        functionDeclaration: `function(){
          try { this.disabled = false; this.removeAttribute && this.removeAttribute('disabled'); this.removeAttribute && this.removeAttribute('aria-disabled'); } catch {}
        }`,
        awaitPromise: false
      });
    } catch {}

    if (sequence) {
      try {
        await Runtime.callFunctionOn({
          objectId,
          functionDeclaration: `function(){
            const el = this.closest?.('button,[role="button"]') || this;
            el.scrollIntoView?.({block:'center', inline:'center'});
            const r = el.getBoundingClientRect();
            const cx = Math.max(1, Math.floor(r.left + r.width/2));
            const cy = Math.max(1, Math.floor(r.top  + r.height/2));
            const opts = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };
            el.dispatchEvent(new PointerEvent('pointerover', opts));
            el.dispatchEvent(new PointerEvent('pointerdown', opts));
            el.dispatchEvent(new MouseEvent('mouseover',  opts));
            el.dispatchEvent(new MouseEvent('mousedown',  opts));
            el.dispatchEvent(new PointerEvent('pointerup', opts));
            el.dispatchEvent(new MouseEvent('mouseup',    opts));
            el.dispatchEvent(new MouseEvent('click',      opts));
          }`,
          awaitPromise: false
        });
        return true;
      } catch {}
    }

    try {
      await Runtime.callFunctionOn({
        objectId,
        functionDeclaration: `function(){ (this.closest?.('button,[role="button"]')||this).click(); }`,
        awaitPromise: false
      });
      return true;
    } catch {}
  }
  return false;
}


// Создать новую вкладку Studio и подключиться
async function openStudioTab() {
  const browser = await CDP({ host: '127.0.0.1', port: 9333 });
  const { Target } = browser;

  await Target.setDiscoverTargets({ discover: true });
  const { targetId } = await Target.createTarget({ url: 'about:blank' });
  const tab = await CDP({ host: '127.0.0.1', port: 9333, target: targetId });
  const { Page, DOM, Runtime, Browser, Emulation } = tab;

  await Promise.all([Page.enable(), DOM.enable(), Runtime.enable()]);
  await Page.setLifecycleEventsEnabled({ enabled: true }); // ждём networkIdle

  await Page.navigate({ url: 'https://studio.youtube.com/' });

  // ждём networkIdle до 15 cек (ускоряет прогрузку шапки/меню каналов)
  let idleSeen = false;
  Page.lifecycleEvent(({ name }) => { if (name === 'networkIdle') idleSeen = true; });
  const t0 = Date.now();
  while (!idleSeen && Date.now() - t0 < 15000) { await new Promise(r => setTimeout(r, 100)); }

  const ok = await withTimeout(waitForCondition(Runtime, `
    const hostOk = location.hostname.includes('studio.youtube.com');
    const shell  = document.querySelector('ytd-app, ytcp-app, ytcp-uploads-dialog, ytcp-header');
    return hostOk && !!shell;
  `), GLOBAL_TIMEOUT, 'Studio shell load timeout');
  if (!ok) throw new Error('Studio shell load timeout');

  try {
    const { windowId } = await Browser.getWindowForTarget({ targetId });

    // ставим обычный размер окна и только потом максимум
    await Browser.setWindowBounds({ windowId, bounds: { width: 1920, height: 1080, windowState: 'normal' } });
    await Browser.setWindowBounds({ windowId, bounds: { windowState: 'maximized' } });

    // отключаем любые эмуляции метрик на всякий
    try { await Emulation.clearDeviceMetricsOverride(); } catch {}
  } catch {}

  await Page.bringToFront();

  // ждём появление одного из двух вариантов кнопки аккаунта (без Shadow DOM)
  // ждём кнопку аккаунта через pierce:true (ищем в Shadow DOM тоже)
  await withTimeout((async () => {
    const { root } = await DOM.getDocument({ depth: -1, pierce: true });
    const sels = [
      'ytcp-topbar-menu-button-renderer#account-button',
      'ytd-topbar-menu-button-renderer #avatar-btn'
    ];
    const t0 = Date.now();
    while (Date.now() - t0 < 30000) {
      for (const sel of sels) {
        const { nodeId } = await DOM.querySelector({ nodeId: root.nodeId, selector: sel });
        if (nodeId) return true;
      }
      await sleep(250);
    }
    return false;
  })(), 30000, 'avatar/account button not found');



  return { browser, tab };
}

// ============================ Основной сценарий ============================
(async () => {
  let browser, tab;
  try {
    // ---------- Шаг 0. Чтение меты и файлов ----------
    await stepLog('Читаем конфиги и ищем файлы');
    const globalParamsPath = path.join(mainFolderPath, 'globalParams.json');
    if (!fs.existsSync(globalParamsPath)) throw new Error(`Файл не найден: ${globalParamsPath}`);
    const globalParams = JSON.parse(fs.readFileSync(globalParamsPath, 'utf-8'));
    const fullName = globalParams?.channels?.[channelName]?.fullName;
    if (!fullName) throw new Error(`Не найден fullName для channelName="${channelName}" в globalParams.json`);

    const videoDir = path.join(mainFolderPath, channelName, 'VIDEOS', videoFolderName);
    if (!fs.existsSync(videoDir)) throw new Error(`Папка не найдена: ${videoDir}`);

    const metaPath = path.join(videoDir, 'videoMeta.json');
    if (!fs.existsSync(metaPath)) throw new Error(`Файл не найден: ${metaPath}`);
    const meta = JSON.parse(fs.readFileSync(metaPath, 'utf-8'));
    const safeTitle = sanitizeFileName(meta.title);

    // Видео: ищем/переименовываем
    const tryExts = ['.mov','.MOV','.mp4','.MP4','.mkv','.MKV','.m4v','.M4V'];
    let videoFile = null;
    for (const ext of tryExts) {
      const orig = path.join(videoDir, `${videoFolderName}${ext}`);
      const renamed = path.join(videoDir, `${safeTitle}${ext}`);
      const foundOrig = fileExistsCaseInsensitive(orig);
      const foundRenamed = fileExistsCaseInsensitive(renamed);
      if (foundOrig) {
        if (!foundRenamed || path.resolve(foundRenamed) === path.resolve(foundOrig)) {
          if (path.resolve(foundOrig) !== path.resolve(renamed)) fs.renameSync(foundOrig, renamed);
          videoFile = renamed; break;
        } else {
          videoFile = foundRenamed; break;
        }
      }
      if (foundRenamed) { videoFile = foundRenamed; break; }
    }
    if (!videoFile) throw new Error('Видео файл не найден ни как <videoFolderName>.* ни как <title>.*');

    // Превью: приоритет prev* → preview_16-9*
    const thumbCandidates = [
      'prev.png','prev.jpg','prev.jpeg','prev.webp',
      'preview_16-9.png','preview_16-9.jpg','preview_16-9.jpeg','preview_16-9.webp'
    ].map(n => path.join(videoDir, n));
    const previewFile = thumbCandidates.find(p => fs.existsSync(p)) || null;

    // ---------- Шаг 1. Открыть Studio ----------
    await stepLog('Открываем YouTube Studio');
    const opened = await openStudioTab();
    browser = opened.browser;
    tab     = opened.tab;
    const { Page, Runtime, DOM } = tab;

    // ---------- Шаг 2. Switch account ----------
    // Жмём по одному из двух вариантов кнопки аккаунта (оба могут быть в Shadow DOM)
    const accountClicked = await pierceAndClick(DOM, Runtime, [
      'ytcp-topbar-menu-button-renderer#account-button',
      'ytd-topbar-menu-button-renderer #avatar-btn'
    ]);
    if (!accountClicked) throw new Error('account button click failed');

    // ждём открытие меню
    await withTimeout(waitForSelector(Runtime, 'ytd-multi-page-menu-renderer, ytd-popup-container'), GLOBAL_TIMEOUT, 'account menu timeout');

    await stepLog('Выбираем "Switch account"');
    await clickContainsText(Runtime, 'tp-yt-paper-item,ytd-compact-link-renderer', 'Switch account');

    await stepLog(`Переключаемся на канал: ${fullName}`);
    await withTimeout((async () => {
      for (let i = 0; i < 120; i++) {
        const { result } = await Runtime.evaluate({
          expression: `
            (function(){
              const list = Array.from(document.querySelectorAll('ytd-account-item-renderer'));
              const t = list.find(x => (x.querySelector('#channel-title')?.innerText||'').trim() === ${JSON.stringify(fullName)});
              if (!t) return false; (t.querySelector('tp-yt-paper-icon-item, tp-yt-paper-item, a, #channel-title') || t).click();
              return true;
            })()
          `,
          returnByValue: true
        });
        if (result?.value) return true;
        await sleep(500);
      }
      return false;
    })(), GLOBAL_TIMEOUT, 'switch account timeout');

    await sleep(2200); // редирект/инициализация

    // ---------- Шаг 3. Create → Upload videos ----------
    await stepLog('Жмём Create');
    await withTimeout(waitForCondition(Runtime, `
      const byAria = document.querySelector('button[aria-label="Create"], ytcp-icon-button[aria-label="Create"]');
      const byText = Array.from(document.querySelectorAll('button,.ytcpButtonShapeImpl__button-text-content'))
        .find(n => (n.textContent||'').trim().toLowerCase() === 'create');
      return !!(byAria || byText);
    `), GLOBAL_TIMEOUT, 'Create not found');
    await Runtime.evaluate({ expression: `
      (function(){
        const a=document.querySelector('button[aria-label="Create"], ytcp-icon-button[aria-label="Create"]');
        if (a) { (a.closest('button,[role="button"]')||a).click(); return; }
        const b=Array.from(document.querySelectorAll('button,.ytcpButtonShapeImpl__button-text-content'))
          .find(n => (n.textContent||'').trim().toLowerCase() === 'create');
        if (b) (b.closest('button,[role="button"]')||b).click();
      })()
    `});

    await stepLog('Выбираем "Upload videos"');
    await withTimeout(waitForCondition(Runtime, `
      const hasItem = Array.from(document.querySelectorAll('yt-formatted-string, tp-yt-paper-item'))
        .some(n => (n.textContent||'').trim().toLowerCase() === 'upload videos');
      return hasItem;
    `), GLOBAL_TIMEOUT, 'Upload menu not found');
    await Runtime.evaluate({ expression: `
      (function(){
        const nodes = Array.from(document.querySelectorAll('yt-formatted-string, tp-yt-paper-item'));
        const item = nodes.find(n => (n.textContent||'').trim().toLowerCase() === 'upload videos');
        (item?.closest('tp-yt-paper-item,a,[role="menuitem"]')||item)?.click();
      })()
    `});

    await stepLog('Ждём input[type="file"] и отдаём файл');
    await withTimeout(waitForCondition(Runtime, `return !!document.querySelector('input[type="file"]')`), GLOBAL_TIMEOUT, 'file input not mounted');
    await uploadFileToLatestInput(DOM, videoFile);

    await stepLog('Ждём форму деталей');
    await withTimeout(waitForSelector(Runtime, 'ytcp-video-title #textbox'), GLOBAL_TIMEOUT, 'details load timeout');

    // ---------- Шаг 4. Reuse details (надёжно) ----------
    if (await exists(Runtime, '#reuse-details-button button')) {
      await stepLog('Открываем Reuse details');
      await safeClick(Runtime, '#reuse-details-button button');

      // Ждём появления диалога
      await withTimeout(waitForSelector(Runtime,
        'ytcp-uploads-reuse-details-selection-dialog, ytcp-video-pick-dialog-contents'
      ), GLOBAL_TIMEOUT, 'reuse dialog timeout');

      // ⚙️ helpers для pierce:true
      async function pierceCount(DOM, selector) {
        const { root } = await DOM.getDocument({ depth: -1, pierce: true });
        const { nodeIds } = await DOM.querySelectorAll({ nodeId: root.nodeId, selector });
        return (nodeIds && nodeIds.length) || 0;
      }
      async function pierceNthClickCenter(DOM, Runtime, selector, index = 0) {
        const { root } = await DOM.getDocument({ depth: -1, pierce: true });
        const { nodeIds } = await DOM.querySelectorAll({ nodeId: root.nodeId, selector });
        if (!nodeIds || !nodeIds.length) return false;
        const nodeId = nodeIds[Math.min(index, nodeIds.length - 1)];

        try { await DOM.scrollIntoViewIfNeeded({ nodeId }); } catch {}

        const { object } = await DOM.resolveNode({ nodeId });
        const objectId = object?.objectId;
        if (!objectId) return false;

        // Кликаем по центру карточки (не по ссылкам «Learn more»)
        await Runtime.callFunctionOn({
          objectId,
          functionDeclaration: `function(){
            const card = this;
            const el = card; // берём сам контейнер карточки
            el.scrollIntoView?.({block:'center', inline:'center'});
            const r = el.getBoundingClientRect();
            const cx = Math.max(1, Math.floor(r.left + r.width/2));
            const cy = Math.max(1, Math.floor(r.top  + r.height/2));
            const opts = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };
            el.dispatchEvent(new PointerEvent('pointerover', opts));
            el.dispatchEvent(new PointerEvent('pointerdown', opts));
            el.dispatchEvent(new MouseEvent('mouseover',  opts));
            el.dispatchEvent(new MouseEvent('mousedown',  opts));
            el.dispatchEvent(new PointerEvent('pointerup', opts));
            el.dispatchEvent(new MouseEvent('mouseup',    opts));
            el.dispatchEvent(new MouseEvent('click',      opts));
          }`,
          awaitPromise: false
        });
        return true;
      }

      // Ждём карточки до 20 сек (с учётом медленной подгрузки)
      await stepLog(`Ищу карточки (ожидаю до 20 сек)…`);
      let cards = 0;
      {
        const T0 = Date.now();
        while (Date.now() - T0 < 20000) {
          cards = await pierceCount(DOM, 'ytcp-video-pick-dialog-contents ytcp-entity-card');
          if (cards > 0) break;
          await sleep(500);
        }
      }
      console.log(`📋 Карточек найдено: ${cards}`);

      if (cards > 0) {
        // Кликаем по карточке строго по индексу (центр контейнера)
        const idx = Math.min(reuseIndex|0, Math.max(cards - 1, 0));
        await stepLog(`Кликаю карточку #${idx}`);
        await pierceNthClickCenter(DOM, Runtime, 'ytcp-video-pick-dialog-contents ytcp-entity-card', idx);
        await sleep(800);

        // Теперь жмём "Reuse"/"Select" только если кнопка доступна
        await stepLog('Жмём Reuse/Select');
        const reuseClicked = await (async () => {
          // Несколько селекторов для кнопки выбора
          const sels = [
            'ytcp-uploads-reuse-details-selection-dialog ytcp-button#select-button button',
            'ytcp-button#select-button button',
            'button[aria-label="Reuse"]',
            'ytcp-uploads-reuse-details-selection-dialog button'
          ];
          for (const sel of sels) {
            const ok = await (async () => {
              const { root } = await DOM.getDocument({ depth: -1, pierce: true });
              const { nodeId } = await DOM.querySelector({ nodeId: root.nodeId, selector: sel });
              if (!nodeId) return false;
              try { await DOM.scrollIntoViewIfNeeded({ nodeId }); } catch {}
              const { object } = await DOM.resolveNode({ nodeId });
              const objectId = object?.objectId;
              if (!objectId) return false;
              try {
                await Runtime.callFunctionOn({
                  objectId,
                  functionDeclaration: `function(){
                    const el = this.closest?.('button,[role="button"]') || this;
                    if (el.disabled || el.getAttribute?.('aria-disabled') === 'true') return false;
                    el.scrollIntoView?.({block:'center', inline:'center'});
                    const r = el.getBoundingClientRect();
                    const cx = Math.max(1, Math.floor(r.left + r.width/2));
                    const cy = Math.max(1, Math.floor(r.top  + r.height/2));
                    const opts = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };
                    el.dispatchEvent(new PointerEvent('pointerover', opts));
                    el.dispatchEvent(new PointerEvent('pointerdown', opts));
                    el.dispatchEvent(new MouseEvent('mouseover',  opts));
                    el.dispatchEvent(new MouseEvent('mousedown',  opts));
                    el.dispatchEvent(new PointerEvent('pointerup', opts));
                    el.dispatchEvent(new MouseEvent('mouseup',    opts));
                    el.dispatchEvent(new MouseEvent('click',      opts));
                    return true;
                  }`,
                  returnByValue: true,
                  awaitPromise: false
                });
                return true;
              } catch { return false; }
            })();
            if (ok) return true;
          }
          return false;
        })();

        if (!reuseClicked) {
          console.log('⚠️ Не удалось нажать Reuse — продолжаю без Reuse.');
        }
      } else {
        console.log('⚠️ Карточек нет — пропускаю Reuse.');
      }

    } // конец Шага 4

    // ---------- Шаг 5. Title + Description ----------
    function _isBlankOrNull(v) {
      if (v == null) return true;
      const s = String(v).trim();
      return s === '' || s.toUpperCase() === 'NULL';
    }

    if (!_isBlankOrNull(meta.title)) {
      await stepLog('Заполняем Title');
      await setContentEditable(Runtime, 'ytcp-video-title #textbox', meta.title);
    } else {
      console.log('⏭ Пропускаю Title (пусто или "NULL")');
    }

    if (!_isBlankOrNull(meta.description)) {
      await stepLog('Заполняем Description');
      await setContentEditable(Runtime, 'ytcp-video-description #textbox', meta.description);
    } else {
      console.log('⏭ Пропускаю Description (пусто или "NULL")');
    }

    // ---------- Шаг 6. Show more → Altered content = No ----------
    await stepLog('Show more → Altered content = No');
    await clickContainsText(Runtime, 'button', 'Show more')
      || await clickContainsText(Runtime, 'button', 'Show advanced settings');
    await withTimeout(waitForSelector(Runtime, 'tp-yt-paper-radio-button[name="VIDEO_HAS_ALTERED_CONTENT_NO"]'), GLOBAL_TIMEOUT, 'altered content radios timeout');
    await safeClick(Runtime, 'tp-yt-paper-radio-button[name="VIDEO_HAS_ALTERED_CONTENT_NO"]');

    // ---------- Шаг 7. Превью (как в debug) ----------
    if (previewFile) {
      await stepLog('Загружаем превью');
      let changedViaMenu = false;
      const hasOptions = await exists(Runtime, 'ytcp-thumbnail-editor #options-button');
      if (hasOptions) {
        await safeClick(Runtime, 'ytcp-thumbnail-editor #options-button');
        const menuOk = await waitForSelector(Runtime, 'tp-yt-paper-dialog.style-scope.ytcp-text-menu, ytcp-text-menu', 8000);
        if (menuOk) {
          await Runtime.evaluate({ expression: `
            (function(){
              const item = document.querySelector('tp-yt-paper-item[test-id="CHANGE"]');
              (item || document.querySelector('#text-item-0'))?.click();
            })()
          `});
          const inputOk = await withTimeout(waitForSelector(Runtime, 'input[type="file"]'), GLOBAL_TIMEOUT, 'thumb change input timeout');
          if (inputOk) { await uploadFileToLatestInput(DOM, previewFile); changedViaMenu = true; }
        }
      }
      if (!changedViaMenu) {
        const { result: canDirectRes } = await Runtime.evaluate({
          expression: `
            (function(){
              const b = document.querySelector('ytcp-thumbnail-editor #select-button');
              return !!(b && !b.disabled);
            })()
          `,
          returnByValue: true
        });
        const canDirect = !!canDirectRes?.value;
        if (canDirect) {
          await safeClick(Runtime, 'ytcp-thumbnail-editor #select-button');
          await withTimeout(waitForSelector(Runtime, 'input[type="file"]'), GLOBAL_TIMEOUT, 'thumb input timeout');
          await uploadFileToLatestInput(DOM, previewFile);
        } else if (!hasOptions) {
          console.log('⚠️ Не удалось открыть загрузку превью: нет Options и Select недоступен.');
        }
      }
      await sleep(1200);
      console.log('✅ Превью загружено');
    } else {
      console.log('⚠️ Превью не найдено в папке, шаг пропущен.');
    }

    // ---------- Шаг 8. Monetization (надёжно с ожиданиями) ----------
    EXTRA_DELAY_AFTER_MONETIZATION = 1000; // с этого шага добавляем +1s на каждом этапе
    await stepLog('Открываем вкладку Monetization');
    const hasMonetization = await waitForSelector(Runtime, 'button[test-id="MONETIZATION"]', 5000);
    if (hasMonetization) {
      await safeClick(Runtime, 'button[test-id="MONETIZATION"]');
      await withTimeout(waitForSelector(Runtime, 'ytcp-video-monetization'), GLOBAL_TIMEOUT, 'monetization section timeout');

      await stepLog('Открываю окно настройки монетизации');
      await Runtime.evaluate({ expression: `document.querySelector('ytcp-video-monetization ytcp-icon-button')?.click()` });
      await withTimeout(waitForSelector(Runtime, 'ytcp-video-monetization-edit-dialog'), GLOBAL_TIMEOUT, 'monetization dialog timeout');

      await stepLog('Ставлю On');
      // ждём появление радиокнопки
      await withTimeout(waitForSelector(Runtime, 'ytcp-video-monetization-edit-dialog tp-yt-paper-radio-button#radio-on'), GLOBAL_TIMEOUT, 'monetization radio timeout');

      // кликаем по ON нативной последовательностью
      await Runtime.evaluate({ expression: `
        (function(){
          const el = document.querySelector('ytcp-video-monetization-edit-dialog tp-yt-paper-radio-button#radio-on');
          if (!el) return false;
          const r = el.getBoundingClientRect();
          const cx = Math.max(1, Math.floor(r.left + r.width/2));
          const cy = Math.max(1, Math.floor(r.top  + r.height/2));
          const opts = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };
          el.dispatchEvent(new PointerEvent('pointerover', opts));
          el.dispatchEvent(new PointerEvent('pointerdown', opts));
          el.dispatchEvent(new MouseEvent('mouseover',  opts));
          el.dispatchEvent(new MouseEvent('mousedown',  opts));
          el.dispatchEvent(new PointerEvent('pointerup', opts));
          el.dispatchEvent(new MouseEvent('mouseup',    opts));
          el.dispatchEvent(new MouseEvent('click',      opts));
          return true;
        })()
      `});

      // === Ждём активную кнопку Done (через pierce:true) ===
      await stepLog('Ждём активную кнопку Done');
      await withTimeout((async () => {
        const { root } = await DOM.getDocument({ depth: -1, pierce: true });
        const sels = [
          'tp-yt-paper-dialog[aria-label="Edit video monetization status"] ytcp-button#save-button button[aria-label="Done"]',
          'ytcp-video-monetization-edit-dialog ytcp-button#save-button button[aria-label="Done"]',
          'ytcp-video-monetization-edit-dialog button[aria-label="Done"]'
        ];
        const t0 = Date.now();
        while (Date.now() - t0 < GLOBAL_TIMEOUT) {
          for (const sel of sels) {
            const { nodeId } = await DOM.querySelector({ nodeId: root.nodeId, selector: sel });
            if (!nodeId) continue;
            const { object } = await DOM.resolveNode({ nodeId });
            const objectId = object?.objectId;
            if (!objectId) continue;
            const { result } = await Runtime.callFunctionOn({
              objectId,
              returnByValue: true,
              functionDeclaration: `function(){
                const b = this;
                return !(b.disabled || b.getAttribute?.('aria-disabled') === 'true');
              }`
            });
            if (result?.value === true) return true;
          }
          await sleep(200);
        }
        return false;
      })(), GLOBAL_TIMEOUT, 'Done not enabled');

      // === Жмём Done нативной последовательностью через pierce:true ===
      await stepLog('Жмём Done');
      {
        const sels = [
          'tp-yt-paper-dialog[aria-label="Edit video monetization status"] ytcp-button#save-button button[aria-label="Done"]',
          'ytcp-video-monetization-edit-dialog ytcp-button#save-button button[aria-label="Done"]',
          'ytcp-video-monetization-edit-dialog button[aria-label="Done"]'
        ];
        let clicked = false;
        const { root } = await DOM.getDocument({ depth: -1, pierce: true });
        for (const sel of sels) {
          const { nodeId } = await DOM.querySelector({ nodeId: root.nodeId, selector: sel });
          if (!nodeId) continue;
          try { await DOM.scrollIntoViewIfNeeded({ nodeId }); } catch {}
          const { object } = await DOM.resolveNode({ nodeId });
          const objectId = object?.objectId;
          if (!objectId) continue;

          await Runtime.callFunctionOn({
            objectId,
            functionDeclaration: `function(){
              const el = this.closest?.('button,[role="button"]') || this;
              try { el.disabled = false; el.removeAttribute && el.removeAttribute('disabled'); el.removeAttribute && el.removeAttribute('aria-disabled'); } catch {}
              el.scrollIntoView?.({block:'center', inline:'center'});
              const r = el.getBoundingClientRect();
              const cx = Math.max(1, Math.floor(r.left + r.width/2));
              const cy = Math.max(1, Math.floor(r.top  + r.height/2));
              const opts = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };
              el.dispatchEvent(new PointerEvent('pointerover', opts));
              el.dispatchEvent(new PointerEvent('pointerdown', opts));
              el.dispatchEvent(new MouseEvent('mouseover',  opts));
              el.dispatchEvent(new MouseEvent('mousedown',  opts));
              el.dispatchEvent(new PointerEvent('pointerup', opts));
              el.dispatchEvent(new MouseEvent('mouseup',    opts));
              el.dispatchEvent(new MouseEvent('click',      opts));
              return true;
            }`,
            awaitPromise: false
          });
          clicked = true;
          break;
        }
        if (!clicked) throw new Error('Done button not found to click');
      }

      console.log('✅ Monetization включена');



    } else {
      console.log('⚠️ Monetization вкладки нет — пропускаем');
    }

    // ---------- Шаг 9. Ad suitability (исправлено) ----------
    await stepLog('Открываю вкладку Ad suitability');
    const hasAdSuit = await waitForSelector(Runtime, 'button[test-id="CONTENT_RATINGS"]', 5000);

    if (hasAdSuit) {
      await safeClick(Runtime, 'button[test-id="CONTENT_RATINGS"]');
      await withTimeout(
        waitForSelector(Runtime, 'ytcp-checkbox-lit.all-none-checkbox'),
        GLOBAL_TIMEOUT,
        'ad suitability section timeout'
      );

      await stepLog('Ставлю "None of the above"');
      await Runtime.evaluate({
        expression: `
          (function(){
            const el = document.querySelector('ytcp-checkbox-lit.all-none-checkbox #checkbox');
            if (!el) return false;
            const r = el.getBoundingClientRect();
            const cx = Math.max(1, Math.floor(r.left + r.width/2));
            const cy = Math.max(1, Math.floor(r.top  + r.height/2));
            const o = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };
            el.dispatchEvent(new PointerEvent('pointerover', o));
            el.dispatchEvent(new PointerEvent('pointerdown', o));
            el.dispatchEvent(new MouseEvent('mouseover',  o));
            el.dispatchEvent(new MouseEvent('mousedown',  o));
            el.dispatchEvent(new PointerEvent('pointerup', o));
            el.dispatchEvent(new MouseEvent('mouseup',    o));
            el.dispatchEvent(new MouseEvent('click',      o));
            return true;
          })()
        `
      });

      await stepLog('Жмём Submit rating');

      // 1) Пытаемся кликнуть сразу, если кнопка уже активна
      const clickedImmediate = await (async () => {
        const sels = [
          'ytcp-button#submit-questionnaire-button button[aria-label="Submit rating"]',
          'button[aria-label="Submit rating"]',
          'ytcp-button#submit-questionnaire-button button'
        ];
        const { root } = await DOM.getDocument({ depth: -1, pierce: true });
        for (const sel of sels) {
          const { nodeId } = await DOM.querySelector({ nodeId: root.nodeId, selector: sel });
          if (!nodeId) continue;
          try { await DOM.scrollIntoViewIfNeeded({ nodeId }); } catch {}
          const { object } = await DOM.resolveNode({ nodeId });
          const objectId = object?.objectId;
          if (!objectId) continue;
          const { result: enabled } = await Runtime.callFunctionOn({
            objectId,
            returnByValue: true,
            functionDeclaration: `function(){
              const b = this.closest?.('button,[role="button"]') || this;
              return !(b.disabled || b.getAttribute?.('aria-disabled') === 'true');
            }`
          });
          if (enabled?.value !== false) {
            await Runtime.callFunctionOn({
              objectId,
              functionDeclaration: `function(){
                const el = this.closest?.('button,[role="button"]') || this;
                try { el.disabled = false; el.removeAttribute?.('disabled'); el.removeAttribute?.('aria-disabled'); } catch {}
                el.scrollIntoView?.({block:'center', inline:'center'});
                const r = el.getBoundingClientRect();
                const cx = Math.max(1, Math.floor(r.left + r.width/2));
                const cy = Math.max(1, Math.floor(r.top  + r.height/2));
                const o = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };
                el.dispatchEvent(new PointerEvent('pointerover', o));
                el.dispatchEvent(new PointerEvent('pointerdown', o));
                el.dispatchEvent(new MouseEvent('mouseover',  o));
                el.dispatchEvent(new MouseEvent('mousedown',  o));
                el.dispatchEvent(new PointerEvent('pointerup', o));
                el.dispatchEvent(new MouseEvent('mouseup',    o));
                el.dispatchEvent(new MouseEvent('click',      o));
              }`,
              awaitPromise: false
            });
            return true;
          }
        }
        return false;
      })();

      // 2) Если не вышло сразу — ждём активации и жмём по расширенному списку селекторов
      if (!clickedImmediate) {
        await stepLog('Жмём Submit rating (повтор)');
        const submitClicked = await (async () => {
          const { root } = await DOM.getDocument({ depth: -1, pierce: true });
          const sels = [
            'ytcp-button#submit-questionnaire-button button[aria-label="Submit rating"]',
            'ytpp-self-certification-predictor ytcp-button#submit-questionnaire-button button[aria-label="Submit rating"]',
            'ytpp-self-certification-predictor ytcp-button#submit-questionnaire-button button',
            'ytcp-button#submit-questionnaire-button button',
            'button[aria-label="Submit rating"]'
          ];
          for (const sel of sels) {
            const { nodeId } = await DOM.querySelector({ nodeId: root.nodeId, selector: sel });
            if (!nodeId) continue;
            try { await DOM.scrollIntoViewIfNeeded({ nodeId }); } catch {}
            const { object } = await DOM.resolveNode({ nodeId });
            const objectId = object?.objectId;
            if (!objectId) continue;
            const { result: enabled } = await Runtime.callFunctionOn({
              objectId,
              returnByValue: true,
              functionDeclaration: `function(){
                const el = this.closest?.('button,[role="button"]') || this;
                return !(el.disabled || el.getAttribute?.('aria-disabled') === 'true');
              }`
            });
            if (enabled?.value === false) continue;
            await Runtime.callFunctionOn({
              objectId,
              functionDeclaration: `function(){
                const el = this.closest?.('button,[role="button"]') || this;
                try { el.disabled = false; el.removeAttribute?.('disabled'); el.removeAttribute?.('aria-disabled'); } catch {}
                el.scrollIntoView?.({block:'center', inline:'center'});
                const r = el.getBoundingClientRect();
                const cx = Math.max(1, Math.floor(r.left + r.width/2));
                const cy = Math.max(1, Math.floor(r.top  + r.height/2));
                const o = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };
                el.dispatchEvent(new PointerEvent('pointerover', o));
                el.dispatchEvent(new PointerEvent('pointerdown', o));
                el.dispatchEvent(new MouseEvent('mouseover',  o));
                el.dispatchEvent(new MouseEvent('mousedown',  o));
                el.dispatchEvent(new PointerEvent('pointerup', o));
                el.dispatchEvent(new MouseEvent('mouseup',    o));
                el.dispatchEvent(new MouseEvent('click',      o));
              }`,
              awaitPromise: false
            });
            return true;
          }
          // Фолбэк по тексту
          const { result } = await Runtime.evaluate({
            returnByValue: true,
            expression: `
              (function(){
                const btn = Array.from(document.querySelectorAll('button, ytcp-button-shape button'))
                  .find(b => (b.textContent||'').toLowerCase().includes('submit rating'));
                if (!btn) return false;
                const el = btn.closest?.('button,[role="button"]') || btn;
                try { el.disabled = false; el.removeAttribute?.('disabled'); el.removeAttribute?.('aria-disabled'); } catch {}
                el.scrollIntoView?.({block:'center', inline:'center'});
                const r = el.getBoundingClientRect();
                const cx = Math.max(1, Math.floor(r.left + r.width/2));
                const cy = Math.max(1, Math.floor(r.top  + r.height/2));
                const o = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };
                el.dispatchEvent(new PointerEvent('pointerover', o));
                el.dispatchEvent(new PointerEvent('pointerdown', o));
                el.dispatchEvent(new MouseEvent('mouseover',  o));
                el.dispatchEvent(new MouseEvent('mousedown',  o));
                el.dispatchEvent(new PointerEvent('pointerup', o));
                el.dispatchEvent(new MouseEvent('mouseup',    o));
                el.dispatchEvent(new MouseEvent('click',      o));
                return true;
              })()
            `
          });
          return !!result?.value;
        })();

        if (!submitClicked) {
          console.log('⚠️ Submit rating: кнопка не кликнулась. Продолжаю дальше.');
        }
      }

      await sleep(1200);
      console.log('✅ Ad suitability отмечено');

    } else {
      console.log('⚠️ Ad suitability вкладки нет — пропускаем');
    }


    // ---------- Шаг 10. Visibility → Publish (+ повторный Publish в диалоге) ----------
    await stepLog('Открываю вкладку Visibility');

    // ждём 5 секунд, чтобы вкладка гарантированно подгрузилась
    await sleep(5000);

    const hasVisibility = await waitForSelector(Runtime, 'button[test-id="REVIEW"]', 5000);
    if (hasVisibility) {
      await safeClick(Runtime, 'button[test-id="REVIEW"]');
      // ещё короткая пауза после клика, чтобы форма развернулась
      await sleep(1200);
      await withTimeout(
        waitForSelector(Runtime, 'tp-yt-paper-radio-button[name="PUBLIC"]'),
        GLOBAL_TIMEOUT,
        'visibility section timeout'
      );
    } else {
      console.log('⚠️ Visibility вкладки нет — пробую выбрать Public напрямую');
    }

    await stepLog('Ставлю Public');
    await Runtime.evaluate({
      expression: `
        (function(){
          const el = document.querySelector('tp-yt-paper-radio-button[name="PUBLIC"]');
          if (!el) return false;
          el.scrollIntoView?.({block:'center'});
          const r = el.getBoundingClientRect();
          const cx = Math.max(1, Math.floor(r.left + r.width/2));
          const cy = Math.max(1, Math.floor(r.top  + r.height/2));
          const o = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };
          el.dispatchEvent(new PointerEvent('pointerover', o));
          el.dispatchEvent(new PointerEvent('pointerdown', o));
          el.dispatchEvent(new MouseEvent('mouseover',  o));
          el.dispatchEvent(new MouseEvent('mousedown',  o));
          el.dispatchEvent(new PointerEvent('pointerup', o));
          el.dispatchEvent(new MouseEvent('mouseup',    o));
          el.dispatchEvent(new MouseEvent('click',      o));
          // страховка: выставим checked, если YouTube не дернул реактивщину
          try { el.setAttribute('checked', ''); } catch {}
          return true;
        })()
      `
    });
    await sleep(500);

    await stepLog('Publish');
    await withTimeout(
      waitForSelector(Runtime, 'ytcp-button#done-button button[aria-label="Publish"], button[aria-label="Publish"]'),
      GLOBAL_TIMEOUT,
      'publish button not found'
    );
    await (clickContainsText(Runtime, 'button', 'Publish')
      || safeClick(Runtime, 'ytcp-button#done-button button[aria-label="Publish"]'));
    await sleep(900);


    await safeClick(Runtime, 'tp-yt-paper-radio-button[name="PUBLIC"]');
    await sleep(250);

    await stepLog('Publish');
    await clickContainsText(Runtime, 'button', 'Publish')
      || await safeClick(Runtime, 'ytcp-button#done-button button[aria-label="Publish"]');
    await sleep(900);

    // Диалог “We’re still checking your video” — жмём Publish внутри него «первым способом» (native pointer/mouse/click)
    await withTimeout((async () => {
      // === как в рабочем сниппете ===
      function _getPublishElementsExpr() {
        return `
          (function(){
            const host = Array.from(document.querySelectorAll('ytcp-button#secondary-action-button'))
              .find(el => el.closest('#dialog-buttons'));
            if (!host) return { found:false };
            const inner =
              host.shadowRoot?.querySelector('button[aria-label="Publish"]') ||
              host.querySelector('button[aria-label="Publish"]') ||
              host.shadowRoot?.querySelector('button') ||
              host.querySelector('button');
            return { found:true, hasInner: !!inner };
          })()
        `;
      }

      async function tryNativeClicksExact(Runtime) {
        // выполняем в контексте страницы «жёсткий» клик по inner-кнопке
        const { result } = await Runtime.evaluate({
          returnByValue: true,
          expression: `
            (function(){
              const host = Array.from(document.querySelectorAll('ytcp-button#secondary-action-button'))
                .find(el => el.closest('#dialog-buttons'));
              if (!host) return 'NO_HOST';
              const btn =
                host.shadowRoot?.querySelector('button[aria-label="Publish"]') ||
                host.querySelector('button[aria-label="Publish"]') ||
                host.shadowRoot?.querySelector('button') ||
                host.querySelector('button');
              if (!btn) return 'NO_INNER';

              try { btn.disabled = false; btn.removeAttribute('disabled'); btn.removeAttribute('aria-disabled'); } catch {}
              btn.scrollIntoView({ block: 'center', inline: 'center' });

              const r  = btn.getBoundingClientRect();
              const cx = Math.floor(r.left + r.width/2), cy = Math.floor(r.top + r.height/2);
              const opts = { bubbles:true, cancelable:true, composed:true, view:window, clientX:cx, clientY:cy };

              btn.dispatchEvent(new PointerEvent('pointerover', opts));
              btn.dispatchEvent(new PointerEvent('pointerdown', opts));
              btn.dispatchEvent(new MouseEvent('mouseover',  opts));
              btn.dispatchEvent(new MouseEvent('mousedown',  opts));
              btn.dispatchEvent(new PointerEvent('pointerup', opts));
              btn.dispatchEvent(new MouseEvent('mouseup',    opts));
              btn.dispatchEvent(new MouseEvent('click',      opts));

              return 'CLICK_SENT';
            })()
          `
        });
        return result?.value || 'NO_RESULT';
      }

      // ждём максимум 4 сек, появится ли диалог вообще — если нет, просто выходим
      const dialogAppeared = await waitForSelector(Runtime, '#dialog-buttons', 4000);
      if (!dialogAppeared) return true;

      // если publish внутри футера реально существует — пробуем до 3 раз с паузами по 5 сек
      for (let i = 0; i < 3; i++) {
        // проверим, что элемент вообще виден сейчас
        const { result:present } = await Runtime.evaluate({
          returnByValue: true,
          expression: _getPublishElementsExpr()
        });
        if (!present?.value?.found) break; // нет футера — диалог, возможно, уже закрылся

        await tryNativeClicksExact(Runtime);
        // даём интерфейсу закрыть диалог
        const gone = await waitGone(Runtime, '#dialog-buttons', 1500);
        if (gone) return true;

        // пауза между попытками (как просили)
        await sleep(5000);
      }

      // финальная проверка: вдруг закрылся без нашего waitGone
      return await waitGone(Runtime, '#dialog-buttons', 3000);
    })(), 30000, 'secondary publish dialog timeout');


    // === WAIT PUBLISH RESULT (unified) ===
    // Учитываем "upload complete ... processing will begin shortly"
    // Успех/ошибка логируем строго 'PUBLISH SUCCESS' / 'PUBLISH ERROR'
    try {
      await stepLog('⏳ Ждём завершение загрузки/обработки/проверок…');

      async function getProgressSnapshot(Runtime) {
        const { result } = await Runtime.evaluate({
          returnByValue: true,
          expression: `
            (function(){
              const labels = [];

              // Классический компонент прогресса
              const prog = document.querySelector('ytcp-video-upload-progress');
              if (prog) {
                const a = prog.querySelector('.progress-label')?.innerText || '';
                const b = prog.getAttribute('aria-label') || '';
                const c = prog.textContent || '';
                labels.push(a,b,c);
              }

              // Альтернативные места (диалоги)
              const alt1 = document.querySelector('#dialog.ytcp-uploads-dialog, ytcp-uploads-dialog, ytcp-uploads-still-processing-dialog, tp-yt-paper-dialog#dialog');
              if (alt1) {
                const t = alt1.textContent || '';
                labels.push(t);
                const h = alt1.querySelector('[slot="content"], .content, .dialog-content, .progress-text, .primary-text, .secondary-text');
                if (h) labels.push(h.textContent || '');
              }

              const raw = labels.filter(Boolean).join(' | ');
              const text = raw.replace(/\\s+/g,' ').trim().toLowerCase();

              const uploadCompleteSoonProcessing = /upload\\s*complete.*processing\\s*will\\s*begin\\s*shortly/.test(text);
              const checksDone = /(checks\\s*(complete|completed)|no\\s*issues|checked)/.test(text);
              const processing = /(processing|sd\\b|hd\\b)/.test(text);
              const uploading  = /(upload(ing)?\\b|\\b\\d+%)/.test(text) && !/upload\\s*complete/.test(text);

              return { text, flags: { uploadCompleteSoonProcessing, checksDone, processing, uploading } };
            })()
          `
        });
        return result?.value || { text:'', flags:{ uploadCompleteSoonProcessing:false, checksDone:false, processing:false, uploading:false } };
      }

      // Логика: успех сразу при "upload complete … processing will begin shortly"
      // или когда ушли uploading/processing, или checksDone
      {
        const MAX = 15 * 60 * 1000;
        const POLL = 5000;
        const t0 = Date.now();

        while (Date.now() - t0 < MAX) {
          const snap = await getProgressSnapshot(Runtime);
          const { uploadCompleteSoonProcessing, checksDone, processing, uploading } = snap.flags;

          if (uploadCompleteSoonProcessing) {
            // Успех для каналов без монетизации
            break;
          }
          if ((!uploading && !processing) || checksDone) {
            // Общий успех
            break;
          }

          if (snap.text) console.log('…', snap.text);
          await sleep(POLL);
        }
      }

      // Закрываем текущую CDP-сессию браузера и завершаем единым SUCCESS-логом
      try { await browser.Browser.close(); } catch {}
      console.log('PUBLISH SUCCESS');
      return;

    } catch (e) {
      console.log('PUBLISH ERROR');
      throw e;
    }

  } catch (e) {
    console.log('PUBLISH ERROR');
    console.error('❌ Ошибка:', e?.message || e);
    try { if (browser) await browser.Browser.close(); } catch {}
    process.exitCode = 1;
  } finally {
    try { if (tab) await tab.close(); } catch {}
    try { if (browser) await browser.close(); } catch {}
  }
})();
