#!/usr/bin/env node
// Dependency-free export using Node.js 22+ and a local Chrome/Chromium binary.
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const directory = dirname(fileURLToPath(import.meta.url));
const chromeBinary = process.env.CHROME_BIN || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const profile = await mkdtemp(join(tmpdir(), 'cdc-slides-'));
const chrome = spawn(chromeBinary, [
  '--headless=new', '--no-first-run', '--no-default-browser-check',
  '--disable-background-networking', '--disable-extensions',
  '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank',
], { stdio: ['ignore', 'ignore', 'pipe'] });
let processError;
let chromeLog = '';
chrome.on('error', error => { processError = error; });
chrome.stderr.on('data', data => { chromeLog = (chromeLog + data).slice(-8000); });
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
let socket;
const pending = new Map();
let nextId = 0;
const scriptErrors = [];

async function waitFor(check, description, timeout = 15000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (processError) throw processError;
    if (chrome.exitCode !== null) throw new Error(`Chrome exited: ${chromeLog}`);
    const result = await check();
    if (result) return result;
    await sleep(100);
  }
  throw new Error(`Timed out waiting for ${description}`);
}

function command(method, params = {}) {
  const id = ++nextId;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`CDP timeout: ${method}`));
    }, 30000);
    pending.set(id, { resolve, reject, timer });
    socket.send(JSON.stringify({ id, method, params }));
  });
}

async function evaluate(expression) {
  const result = await command('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
  if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
  return result.result.value;
}

try {
  const port = await waitFor(async () => {
    try { return (await readFile(join(profile, 'DevToolsActivePort'), 'utf8')).split('\n')[0]; }
    catch { return undefined; }
  }, 'Chrome debugging port');
  const response = await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, { method: 'PUT' });
  if (!response.ok) throw new Error(`Chrome target creation failed: ${response.status}`);
  const target = await response.json();
  socket = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Chrome websocket connection timed out')), 10000);
    socket.addEventListener('open', () => { clearTimeout(timer); resolve(); }, { once: true });
    socket.addEventListener('error', () => { clearTimeout(timer); reject(new Error('Chrome websocket connection failed')); }, { once: true });
  });
  socket.addEventListener('message', event => {
    const message = JSON.parse(event.data);
    if (message.method === 'Runtime.exceptionThrown') scriptErrors.push(message.params);
    const request = pending.get(message.id);
    if (!request) return;
    clearTimeout(request.timer);
    pending.delete(message.id);
    if (message.error) request.reject(new Error(JSON.stringify(message.error)));
    else request.resolve(message.result);
  });
  socket.addEventListener('close', () => {
    for (const request of pending.values()) {
      clearTimeout(request.timer);
      request.reject(new Error('Chrome websocket closed'));
    }
    pending.clear();
  });
  await command('Page.enable');
  await command('Runtime.enable');
  await command('Emulation.setDeviceMetricsOverride', {
    width: 1600, height: 900, deviceScaleFactor: 1, mobile: false,
  });
  const url = pathToFileURL(join(directory, 'index.html'));
  url.searchParams.set('capture', '1');
  await command('Page.navigate', { url: url.href });
  await waitFor(() => evaluate(`document.readyState === 'complete' && typeof show === 'function' && [...document.images].every(image => image.complete && image.naturalWidth > 0)`), 'presentation and SVG images');
  const count = await evaluate('slides.length');
  const validation = [];
  for (let index = 0; index < count; index++) {
    await evaluate(`show(${index})`);
    await evaluate('new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))');
    const problems = await evaluate(`(() => {
      const slide = document.querySelector('.slide.active');
      const banner = slide.querySelector('.banner');
      const issues = [];
      if (banner) {
        const boundary = banner.getBoundingClientRect().top;
        for (const block of slide.querySelectorAll('.cards,.status')) {
          if (block.getBoundingClientRect().bottom > boundary - 12) issues.push('Content overlaps or crowds the bottom banner');
        }
      }
      for (const card of slide.querySelectorAll('.card')) {
        const outer = card.getBoundingClientRect();
        for (const child of card.children) {
          const rect = child.getBoundingClientRect();
          if (rect.right > outer.right - 12 || rect.bottom > outer.bottom - 12) issues.push('Card child exceeds its content area');
        }
      }
      if (slide.scrollWidth > 1600 || slide.scrollHeight > 900) issues.push('Slide content exceeds 1600 × 900');
      return issues;
    })()`);
    validation.push({ slide: index + 1, problems });
    const screenshot = await command('Page.captureScreenshot', { format: 'png', captureBeyondViewport: false });
    const filename = `slide-${String(index + 1).padStart(2, '0')}.png`;
    await writeFile(join(directory, filename), Buffer.from(screenshot.data, 'base64'));
    console.log(`Exported ${filename}${problems.length ? ` — WARNING: ${problems.join('; ')}` : ' — layout checks passed'}`);
  }
  await evaluate('show(0); document.getElementById("next").click()');
  if (await evaluate('current') !== 1) throw new Error('Next-slide control failed');
  await evaluate('document.getElementById("prev").click()');
  if (await evaluate('current') !== 0) throw new Error('Previous-slide control failed');
  await command('Emulation.setEmulatedMedia', { media: 'print' });
  await evaluate(`slides.forEach(slide => slide.removeAttribute('aria-hidden'))`);
  const pdf = await command('Page.printToPDF', {
    printBackground: true, preferCSSPageSize: true, displayHeaderFooter: false,
    marginTop: 0, marginBottom: 0, marginLeft: 0, marginRight: 0,
  });
  await writeFile(join(directory, 'online-snapshot-design.pdf'), Buffer.from(pdf.data, 'base64'));
  console.log('Exported online-snapshot-design.pdf');
  if (scriptErrors.length) throw new Error(`Browser script errors: ${JSON.stringify(scriptErrors)}`);
  if (validation.some(result => result.problems.length)) throw new Error('Layout validation failed; inspect exported slide images');
  console.log('Validated slide layout, SVG image loading, browser scripts and navigation controls.');
} finally {
  if (socket) socket.close();
  chrome.kill('SIGTERM');
  if (!processError && chrome.exitCode === null) {
    await Promise.race([new Promise(resolve => chrome.once('exit', resolve)), sleep(3000)]);
    if (chrome.exitCode === null) {
      chrome.kill('SIGKILL');
      await Promise.race([new Promise(resolve => chrome.once('exit', resolve)), sleep(1000)]);
    }
  }
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 200 });
}
