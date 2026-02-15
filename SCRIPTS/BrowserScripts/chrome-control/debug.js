// studio_reuse_debug.js
// Запуск: node studio_reuse_debug.js --channelName="..." --mainFolderPath="..." --videoFolderName="..." --reuseIndex=2
// Требует: npm i chrome-remote-interface
// Chrome должен быть запущен так:
//   "C:\Program Files\Google\Chrome\Application\chrome.exe"
//      --remote-debugging-port=9333 --remote-debugging-address=127.0.0.1
//      --user-data-dir=C:\Users\V\AppData\Local\Google\Chrome\RemoteControl
//      --profile-directory=Default --new-window --start-maximized --window-size=1920,1080

const CDP  = require('chrome-remote-interface');
const fs   = require('fs');
const path = require('path');

// ============================ Настройки ============================
const STEP_DELAY      = 4000;    // 4 сек задержка между шагами (видимый прогресс)
const GLOBAL_TIMEOUT  = 60000;   // 60 сек на ожидание любого элемента/условия

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function stepLog(msg) { console.log(`➡️ ${msg}`); await sleep(STEP_DELAY); }
async function withTimeout(promise, ms = GLOBAL_TIMEOUT, msg = 'Timeout') {
  let t; const timeout = new Promise((_,rej) => t = setTimeout(()=>rej(new Error(msg)), ms));
  try { return await Promise.race([promise, timeout]); } finally { clearTimeout(t); }
}
function fileExistsCaseInsensitive(p) {
  if (fs.existsSync(p)) return p;
  const dir = path.dirname(p);
  const base = path.basename(p);
  if (!fs.existsSync(dir)) return null;
  const hit = fs.readdirSync(dir).find(f => f.toLowerCase() === base.toLowerCase());
  return hit ? path.join(dir, hit) : null;
}

// ============================ Парс аргументов ============================
function parseArgs(argv) {
  const out = { _pos: [] };
  for (let i=0;i<argv.length;i++) {
    const tok = argv[i];
    if (tok.startsWith('--')) {
      const eq = tok.indexOf('=');
      if (eq >= 0) {
        const k = tok.slice(2,eq);
        let v = tok.slice(eq+1).replace(/^["']|["']$/g,'');
        out[k] = v;
      } else {
        const k = tok.slice(2);
        let v = (i+1<argv.length && !argv[i+1].startsWith('--')) ? argv[++i] : 'true';
        out[k] = v.replace(/^["']|["']$/g,'');
      }
    } else out._pos.push(tok.replace(/^["']|["']$/g,''));
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
    '  node studio_reuse_debug.js --channelName="..." --mainFolderPath="..." --videoFolderName="..." [--reuseIndex=2]\n' +
    'или позиционно:\n' +
    '  node studio_reuse_debug.js "<channelName>" "<mainFolderPath>" "<videoFolderName>" [reuseIndex]');
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
// Shadow DOM input[type=file]
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

// Найти открытую вкладку Studio и подключиться к ней
async function attachToExistingStudioTab() {
  const browser = await CDP({ host: '127.0.0.1', port: 9333 });
  const { Target } = browser;
  await Target.setDiscoverTargets({ discover: true });
  const { targetInfos } = await Target.getTargets();

  const pick = () => {
    // сначала вкладку загрузки
    let cand = targetInfos.find(t =>
      t.url.includes('studio.youtube.com') &&
      (t.url.includes('/upload') || t.url.includes('uploads') || t.url.includes('udvid')));
    if (cand) return cand;
    // потом любая студия
    cand = targetInfos.find(t => t.url.includes('studio.youtube.com'));
    return cand || null;
  };

  const target = pick();
  if (!target) throw new Error('Не найдена открытая вкладка YouTube Studio');

  const tab = await CDP({ host: '127.0.0.1', port: 9333, target: target.targetId });
  const { Page, Runtime, DOM, Browser, Emulation } = tab;
  await Promise.all([Page.enable(), Runtime.enable(), DOM.enable()]);

  // Максимизируем окно
  try {
    const { windowId } = await Browser.getWindowForTarget({ targetId: target.targetId });
    await Browser.setWindowBounds({ windowId, bounds: { windowState: 'maximized' } });
    await Emulation.setDeviceMetricsOverride({
      width: 1920, height: 1080, deviceScaleFactor: 1, mobile: false,
      screenWidth: 1920, screenHeight: 1080
    });
  } catch {}

  return { browser, tab, url: target.url };
}

// ============================ Основной поток ============================
(async () => {
  let browser, tab;
  try {
    // 0) Файлы и превью
    const videoDir = path.join(mainFolderPath, channelName, 'VIDEOS', videoFolderName);
    if (!fs.existsSync(videoDir)) throw new Error(`Папка не найдена: ${videoDir}`);

    // порядок: prev.* → preview_16-9.*
    const thumbCandidates = [
      'prev.png','prev.jpg','prev.jpeg','prev.webp',
      'preview_16-9.png','preview_16-9.jpg','preview_16-9.jpeg','preview_16-9.webp'
    ].map(n => path.join(videoDir, n));
    const previewFile = thumbCandidates.find(p => fs.existsSync(p)) || null;

    await stepLog('Ищу открытую вкладку YouTube Studio…');
    const attached = await attachToExistingStudioTab();
    browser = attached.browser;
    tab     = attached.tab;
    const { Runtime, DOM } = tab;
    console.log(`🔗 Подключился к: ${attached.url}`);

    // 1) Reuse details
    await stepLog('Нажимаю «Reuse details»…');
    await withTimeout(waitForSelector(Runtime, '#reuse-details-button, #reuse-details-button button'), GLOBAL_TIMEOUT, 'reuse button not found');
    await safeClick(Runtime, '#reuse-details-button button') || await safeClick(Runtime, '#reuse-details-button');
    await waitForSelector(Runtime, 'ytcp-uploads-reuse-details-selection-dialog, ytcp-video-pick-dialog-contents', 8000);

    // 2) Выбор карточки
    await stepLog(`Ищу карточки и кликаю индекс ${reuseIndex}…`);
    const { result: cardsCountRes } = await Runtime.evaluate({
      expression: `
        (function(){
          const list = Array.from(document.querySelectorAll('ytcp-video-pick-dialog-contents ytcp-entity-card'));
          return list.length;
        })()
      `,
      returnByValue: true
    });
    console.log(`📋 Карточек найдено: ${cardsCountRes?.value ?? 0}`);

    await Runtime.evaluate({
      expression: `
        (function(){
          const list = Array.from(document.querySelectorAll('ytcp-video-pick-dialog-contents ytcp-entity-card'));
          const idx = Math.min(${reuseIndex}|0, Math.max(list.length-1,0));
          const c = list[idx] || list[0];
          if (!c) return false;
          (c.querySelector('#content,.thumbnail,.title') || c).click();
          return true;
        })()
      `,
      returnByValue: true
    });
    await sleep(500);

    // 3) Жмём Reuse (если диалог ещё виден)
    await stepLog('Жму кнопку «Reuse»…');
    const clickedReuse = await Runtime.evaluate({
      expression: `
        (function(){
          const scope = document.querySelector('ytcp-uploads-reuse-details-selection-dialog') || document;
          if (!scope) return false;
          const btn =
            scope.querySelector('button[aria-label="Reuse"]') ||
            Array.from(scope.querySelectorAll('button')).find(b => (b.innerText||'').trim()==='Reuse') ||
            scope.querySelector('ytcp-button#select-button button') ||
            scope.querySelector('#select-button button');
          if (!btn) return false;
          btn.click(); return true;
        })()
      `,
      returnByValue: true
    });
    if (!clickedReuse?.result?.value) {
      console.log('⚠️ Кнопка «Reuse» не найдена (возможно, диалог уже закрылся после выбора карточки).');
    }

    // 4) Show more → Altered content = No
    await stepLog('Show more → Altered content = No');
    await clickContainsText(Runtime, 'button', 'Show more')
      || await clickContainsText(Runtime, 'button', 'Show advanced settings');
    await withTimeout(waitForSelector(Runtime, 'tp-yt-paper-radio-button[name="VIDEO_HAS_ALTERED_CONTENT_NO"]'), GLOBAL_TIMEOUT, 'altered content radios timeout');
    await safeClick(Runtime, 'tp-yt-paper-radio-button[name="VIDEO_HAS_ALTERED_CONTENT_NO"]');

    // 5) Загрузка превью (через Options → Change, fallback на Select)
    if (previewFile) {
      await stepLog('Загружаю превью…');
      // сначала пробуем кнопку Options → Change (случай когда миниатюра уже есть)
      let changedViaMenu = false;
      const hasOptions = await exists(Runtime, 'ytcp-thumbnail-editor #options-button');
      if (hasOptions) {
        await safeClick(Runtime, 'ytcp-thumbnail-editor #options-button');
        // ждём меню-текст (tp-yt-paper-dialog ytcp-text-menu)
        const menuOk = await waitForSelector(Runtime, 'tp-yt-paper-dialog.style-scope.ytcp-text-menu, ytcp-text-menu', 8000);
        if (menuOk) {
          // клик по item с test-id="CHANGE"
          await Runtime.evaluate({ expression: `
            (function(){
              const item = document.querySelector('tp-yt-paper-item[test-id="CHANGE"]');
              (item || document.querySelector('#text-item-0'))?.click();
            })()
          `});
          // ждём input[type=file]
          const inputOk = await withTimeout(waitForSelector(Runtime, 'input[type="file"]'), GLOBAL_TIMEOUT, 'thumb change input timeout');
          if (inputOk) {
            await uploadFileToLatestInput(DOM, previewFile);
            changedViaMenu = true;
          }
        }
      }

      // если не получилось через Options → Change — пробуем прямую кнопку Select
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

    // 6) Monetization
    await stepLog('Переход к Monetization…');
    const hasMonetization = await waitForSelector(Runtime, 'button[test-id="MONETIZATION"]', 5000);
    if (hasMonetization) {
      await safeClick(Runtime, 'button[test-id="MONETIZATION"]');
      await withTimeout(waitForSelector(Runtime, 'ytcp-video-monetization'), GLOBAL_TIMEOUT, 'monetization section timeout');

      await Runtime.evaluate({ expression: `document.querySelector('ytcp-video-monetization ytcp-icon-button')?.click()` });
      await withTimeout(waitForSelector(Runtime, 'ytcp-video-monetization-edit-dialog'), GLOBAL_TIMEOUT, 'monetization dialog timeout');

      await safeClick(Runtime, 'tp-yt-paper-radio-button#radio-on');
      await clickContainsText(Runtime, 'button', 'Done')
        || await safeClick(Runtime, 'ytcp-video-monetization-edit-dialog ytcp-button-shape button[aria-label="Done"]');
      await sleep(700);
      console.log('✅ Monetization включена');
    } else {
      console.log('⚠️ Monetization вкладки нет — пропускаем');
    }

    // 7) Ad suitability
    await stepLog('Переход к Ad suitability…');
    const hasAdSuit = await waitForSelector(Runtime, 'button[test-id="CONTENT_RATINGS"]', 5000);
    if (hasAdSuit) {
      await safeClick(Runtime, 'button[test-id="CONTENT_RATINGS"]');
      await withTimeout(waitForSelector(Runtime, 'ytcp-checkbox-lit.all-none-checkbox'), GLOBAL_TIMEOUT, 'ad suitability section timeout');

      await Runtime.evaluate({ expression: `
        (function(){
          const el = document.querySelector('ytcp-checkbox-lit.all-none-checkbox #checkbox');
          if (el) el.click();
        })()
      `});
      await sleep(250);
      await clickContainsText(Runtime, 'button', 'Submit rating');
      await sleep(800);
      console.log('✅ Ad suitability отмечено');
    } else {
      console.log('⚠️ Ad suitability вкладки нет — пропускаем');
    }

    // 8) Visibility → Publish
    await stepLog('Открываю Visibility…');
    const visTabReady =
      (await safeClick(Runtime, 'button[test-id="REVIEW"]')) ||
      (await clickContainsText(Runtime, 'button', 'Visibility'));
    if (!visTabReady) throw new Error('visibility section timeout');

    await withTimeout(waitForSelector(Runtime, 'tp-yt-paper-radio-button[name="PUBLIC"]'), GLOBAL_TIMEOUT, 'visibility radios timeout');
    await safeClick(Runtime, 'tp-yt-paper-radio-button[name="PUBLIC"]');
    await sleep(250);

    await stepLog('Публикую (Publish)…');
    await clickContainsText(Runtime, 'button', 'Publish')
      || await safeClick(Runtime, 'ytcp-button#done-button button[aria-label="Publish"]');
    await sleep(900);

    // 8.1) Диалог “We’re still checking your video” — надёжно жмём вторую Publish и ждём закрытия
    await withTimeout((async () => {
    // ждём сам диалог (коротко), чтобы не падать если его нет
    const hasDialog = await waitForSelector(Runtime, 'tp-yt-paper-dialog#dialog', 4000);
    if (!hasDialog) return true; // диалога нет — ок

    // несколько попыток нажать Publish внутри диалога
    for (let i = 0; i < 20; i++) {
        const { result } = await Runtime.evaluate({
        returnByValue: true,
        expression: `
            (function(){
            const dlg = document.querySelector('tp-yt-paper-dialog#dialog');
            if (!dlg) return 'NO_DIALOG';

            // 1) точный селектор secondary-action Publish
            let target =
                dlg.querySelector('ytcp-button#secondary-action-button button[aria-label="Publish"]') ||
                dlg.querySelector('ytcp-button#secondary-action-button button');

            // 2) fallback: любая кнопка с текстом "Publish" в футере диалога
            if (!target) {
                const btns = Array.from(dlg.querySelectorAll('button'));
                target = btns.find(b => (b.textContent||'').trim().toLowerCase() === 'publish');
            }

            // 3) если нашли внутреннюю кнопку — кликаем
            if (target) {
                // иногда кликается лучше по хосту ytcp-button
                const host = target.closest('ytcp-button') || target;
                // синтетические события — повышают надёжность
                function fire(el, type){
                el.dispatchEvent(new MouseEvent(type, {bubbles:true, cancelable:true, view:window}));
                }
                fire(host, 'mouseover'); fire(host, 'mousedown'); fire(host, 'mouseup'); host.click();
                return 'CLICKED';
            }

            // 4) крайний случай: кликаем по всему блоку диалога в надежде на дефолтную кнопку
            dlg.click();
            return 'FALLBACK';
            })()
        `
        });

        // если диалог уже пропал — выходим
        const gone = await waitGone(Runtime, 'tp-yt-paper-dialog#dialog', 300);
        if (gone) return true;

        await sleep(300);
    }

    // финальное ожидание закрытия (может закрыться с задержкой)
    const closed = await waitGone(Runtime, 'tp-yt-paper-dialog#dialog', 3000);
    return !!closed;
    })(), 10000, 'secondary publish dialog timeout');


    // 9) Немного подождём прогресс (debug)
    await stepLog('⏳ Ждём завершение Upload/Processing/Checks…');
    async function readProgress(Runtime) {
      const { result } = await Runtime.evaluate({
        expression: `
          (function(){
            const p = document.querySelector('ytcp-video-upload-progress');
            if (!p) return {done:true,label:''};
            const label = (p.querySelector('.progress-label')?.innerText||'').toLowerCase();
            const uploading  = label.includes('upload') || /\\d+%/.test(label);
            const processing = label.includes('processing');
            const checksDone = label.includes('checks complete') || label.includes('no issues') || label.includes('checked');
            const done = (!uploading && !processing) || checksDone;
            return {done,label};
          })()
        `,
        returnByValue: true
      });
      return result?.value || { done:false, label:'' };
    }
    const t0 = Date.now(), MAX = 10 * 60 * 1000;
    while (Date.now()-t0 < MAX) {
      const { done, label } = await readProgress(Runtime);
      if (done) break;
      if (label) console.log('…', label);
      await sleep(5000);
    }

    console.log('✅ DEBUG DONE (Reuse→Altered content→Thumbnail→Monetization→Ad suitability→Publish).');

    // Debug-режим: не закрываем весь браузер
  } catch (e) {
    console.error('❌ Ошибка:', e?.message || e);
    process.exitCode = 1;
  } finally {
    try { if (tab) await tab.close(); } catch {}
    try { if (browser) await browser.close(); } catch {}
  }
})();
