#!/usr/bin/env node
// Documentation-only tooling. Requires Node 22+, marked 18.1.0, and Chrome/Chromium.
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { dirname, join, resolve, basename } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = dirname(fileURLToPath(import.meta.url));
const { marked } = await import(process.env.MARKED_MODULE
  ? pathToFileURL(resolve(process.env.MARKED_MODULE)).href : 'marked');
const chromeBinary = process.env.CHROME_BIN || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const temporary = await mkdtemp(join(process.env.TMPDIR || '/tmp', 'cdc-doc-export-'));
const jobs = [
  { name: 'feasibility-report', title: 'Online snapshots', subtitle: 'Feasibility report', audience: 'Engineering leadership · Database owners · SRE', files: ['feasibility-report.md', 'evidence.md'] },
  { name: 'design-rfc', title: 'Online incremental snapshots', subtitle: 'Engineering design RFC', audience: 'Core maintainers · Adapter owners · Database engineers', files: ['design-rfc.md', 'validation-plan.md', 'evidence.md'] },
];
const slug = text => text.replace(/<[^>]+>/g, '').replace(/&[^;]+;/g, '').toLowerCase().replace(/[^\p{L}\p{N}\s-]/gu, '').trim().replace(/\s+/g, '-');
const escape = text => text.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
const css = `
@page { size: A4; margin: 16mm 14mm 18mm; }
* { box-sizing: border-box; } body { margin:0; color:#142d40; font:10pt/1.38 Arial,Helvetica,sans-serif; }
h1,h2,h3 { line-height:1.2; break-after:avoid; } h1 { font-size:23pt; margin:0 0 18px; letter-spacing:-.5px; }
h2 { font-size:14pt; margin:20px 0 10px; padding-top:7px; border-top:1px solid #ccd6dd; }
h3 { font-size:11.5pt; margin:17px 0 8px; } p { margin:0 0 8px; orphans:3; widows:3; }
a { color:#08776e; text-decoration:none; } strong { color:#123247; }
table { border-collapse:collapse; width:100%; margin:10px 0 15px; font-size:8.4pt; line-height:1.34; table-layout:auto; }
thead { display:table-header-group; } th { background:#e6eeef; text-align:left; color:#123247; }
th,td { padding:6px 8px; border-bottom:1px solid #d9e1e6; vertical-align:top; overflow-wrap:anywhere; }
th:first-child,td:first-child { overflow-wrap:normal; }
tr { break-inside:avoid; } tbody tr:nth-child(even) { background:#f6f8fa; }
code { font:8.5pt/1.4 Menlo,Consolas,monospace; overflow-wrap:anywhere; } p code,td code,li code { background:#edf2f5; padding:1px 3px; }
pre { white-space:pre-wrap; overflow-wrap:anywhere; background:#edf2f5; border-left:3px solid #087f75; padding:12px; break-inside:avoid; }
ul,ol { padding-left:21px; margin:8px 0 14px; } li { margin-bottom:6px; } img { width:100%; height:auto; display:block; margin:14px 0 9px; }
p:has(img) { break-inside:avoid; break-after:avoid; } p:has(img)+p { break-before:avoid; font-size:8.8pt; color:#526777; }
.document + .document { break-before:page; } .cover { min-height:247mm; break-after:page; padding-top:12mm; }
.eyebrow { color:#08776e; text-transform:uppercase; letter-spacing:2px; font-size:10pt; font-weight:700; }
.cover h1 { font-size:35pt; max-width:155mm; margin:25px 0 10px; line-height:1.08; }
.subtitle { font-size:20pt; color:#526777; margin-bottom:25px; }
.notice { background:#e1f2ef; border-left:4px solid #087f75; padding:15px 18px; margin:26px 0; }
.cover .meta { font-size:10pt; color:#526777; } .toc { columns:2; column-gap:22px; font-size:9pt; padding:0; list-style:none; }
.toc li { break-inside:avoid; margin:0 0 8px; } .cover h2 { font-size:13pt; margin-top:28px; }
.companion { color:#526777; } .appendix-label { color:#08776e; text-transform:uppercase; font-weight:700; font-size:9pt; letter-spacing:1px; }
`;

async function render(job) {
  const docs = [];
  const contents = [];
  for (const filename of job.files) {
    const prefix = basename(filename, '.md');
    let markdown = await readFile(join(root, filename), 'utf8');
    for (const match of [...markdown.matchAll(/!\[([^\]]*)\]\((diagrams\/[^)]+\.svg)\)/g)]) {
      const image = await readFile(join(root, match[2]));
      markdown = markdown.replace(match[0], `![${match[1]}](data:image/svg+xml;base64,${image.toString('base64')})`);
    }
    let html = marked.parse(markdown, { gfm:true });
    html = html.replace(/<h([1-6])>([\s\S]*?)<\/h\1>/g, (_, level, text) => {
      const id = `${prefix}--${slug(text)}`;
      if (level === '2' && filename === job.files[0]) contents.push(`<li><a href="#${id}">${text}</a></li>`);
      return `<h${level} id="${id}">${text}</h${level}>`;
    });
    html = html.replace(/<a href="([^"]+)">([\s\S]*?)<\/a>/g, (_, href, label) => {
      if (/^https?:/.test(href)) return `<a href="${href}">${label}</a>`;
      const [target, fragment] = href.split('#');
      if (!target) return `<a href="#${prefix}--${fragment}">${label}</a>`;
      if (job.files.includes(target)) return `<a href="#${basename(target, '.md')}--${fragment || slug(target === 'evidence.md' ? 'Online snapshots: evidence and provenance' : target === 'validation-plan.md' ? 'Online snapshots: validation and qualification plan' : 'RFC: online incremental snapshots')}">${label}</a>`;
      if (target.startsWith('../../')) {
        const source = target.slice(6);
        return `<a href="https://github.com/Trendyol/go-pq-cdc/blob/4a7e9b094a19935591b691cf8de38546a98b69d0/${source}${fragment ? `#${fragment}` : ''}">${label}</a>`;
      }
      return `<span class="companion">${label} [companion: ${escape(target)}]</span>`;
    });
    docs.push(`<section class="document">${filename === job.files[0] ? '' : '<p class="appendix-label">Supporting appendix</p>'}${html}</section>`);
  }
  const cover = `<section class="cover"><p class="eyebrow">go-pq-cdc · architecture review</p><h1>${job.title}</h1><p class="subtitle">${job.subtitle}</p><p class="meta">${job.audience}</p><div class="notice"><strong>PROPOSED — NOT IMPLEMENTED</strong><p>Decision requested: approve bounded validation, not production release.</p><p>PostgreSQL 16+ · Kafka and Elasticsearch · Restricted first scope</p></div><p class="meta">13 October 2026 · Revision 0.1<br>Source: 4a7e9b0 plus local changes<br>Owners and approvers: unassigned</p><h2>Reading guide</h2><ul class="toc">${contents.join('')}</ul><p class="meta">Evidence appendices distinguish passing baseline checks from unexecuted qualification work. Source links resolve to the recorded commit and can differ from the local working tree.</p></section>`;
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>${escape(job.subtitle)}</title><style>${css}</style></head><body>${cover}${docs.join('')}</body></html>`;
}

let browser;
let socket;
let browserError;
let browserLog = '';
const pending = new Map();
let sequence = 0;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function waitFor(check, label) {
  const deadline = Date.now() + 20000;
  while (Date.now() < deadline) {
    if (browserError) throw browserError;
    if (browser?.exitCode !== null) throw new Error(`Chrome exited: ${browserLog}`);
    const value = await check();
    if (value) return value;
    await sleep(100);
  }
  throw new Error(`Timed out: ${label}`);
}
function command(method, params = {}) {
  const id = ++sequence;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`CDP timeout: ${method}`)); }, 30000);
    pending.set(id, { resolve, reject, timer });
    socket.send(JSON.stringify({ id, method, params }));
  });
}
async function evaluate(expression) {
  const result = await command('Runtime.evaluate', { expression, awaitPromise:true, returnByValue:true });
  if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
  return result.result.value;
}

try {
  browser = spawn(chromeBinary, ['--headless=new','--disable-extensions','--no-first-run','--disable-background-networking','--remote-debugging-port=0',`--user-data-dir=${temporary}`,'about:blank'], { stdio:['ignore','ignore','pipe'] });
  browser.on('error', error => { browserError = error; });
  browser.stderr.on('data', data => { browserLog = (browserLog + data).slice(-8000); });
  const port = await waitFor(async () => { try { return (await readFile(join(temporary,'DevToolsActivePort'),'utf8')).split('\n')[0]; } catch { return null; } }, 'Chrome port');
  const response = await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, { method:'PUT' });
  if (!response.ok) throw new Error(`Chrome target creation failed: ${response.status}`);
  socket = new WebSocket((await response.json()).webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('WebSocket connection timeout')),10000);
    socket.addEventListener('open', () => { clearTimeout(timer); resolve(); }, { once:true });
    socket.addEventListener('error', () => { clearTimeout(timer); reject(new Error('WebSocket connection failed')); }, { once:true });
  });
  socket.addEventListener('message', event => {
    const reply = JSON.parse(event.data);
    const request = pending.get(reply.id);
    if (!request) return;
    pending.delete(reply.id); clearTimeout(request.timer);
    if (reply.error) request.reject(new Error(JSON.stringify(reply.error))); else request.resolve(reply.result);
  });
  await command('Page.enable');
  await command('Runtime.enable');
  await command('Emulation.setDeviceMetricsOverride', { width:688,height:1000,deviceScaleFactor:1,mobile:false });
  await command('Emulation.setEmulatedMedia', { media:'print' });
  for (const job of jobs) {
    const htmlPath = join(temporary, `${job.name}.html`);
    await writeFile(htmlPath, await render(job));
    await command('Page.navigate', { url:pathToFileURL(htmlPath).href });
    await waitFor(() => evaluate(`document.readyState === 'complete' && document.title === ${JSON.stringify(job.subtitle)} && [...document.images].every(i=>i.complete && i.naturalWidth>0)`), job.name);
    await evaluate('document.fonts.ready');
    const issues = await evaluate(`({ overflow:[...document.querySelectorAll('table,pre,img')].filter(e=>e.getBoundingClientRect().right>document.documentElement.clientWidth+1).map(e=>e.tagName), broken:[...document.querySelectorAll('a[href^="#"]')].filter(a=>!document.getElementById(a.getAttribute('href').slice(1))).map(a=>a.getAttribute('href')) })`);
    if (issues.overflow.length || issues.broken.length) throw new Error(`Document validation failed: ${JSON.stringify(issues)}`);
    const pdf = await command('Page.printToPDF', { printBackground:true,preferCSSPageSize:true,displayHeaderFooter:true,headerTemplate:'<div></div>',footerTemplate:`<div style="font:8px Arial;color:#526777;width:100%;margin:0 14mm;display:flex;justify-content:space-between"><span>go-pq-cdc · ${job.subtitle} · Proposed / 0.1</span><span><span class="pageNumber"></span> / <span class="totalPages"></span></span></div>` });
    const bytes = Buffer.from(pdf.data,'base64');
    if (!bytes.subarray(0,5).equals(Buffer.from('%PDF-'))) throw new Error('Invalid PDF output');
    await writeFile(join(root,`${job.name}.pdf`),bytes);
    console.log(`${job.name}.pdf: ${bytes.length} bytes; images, internal anchors, and horizontal bounds passed`);
    if (process.env.PREVIEW_DIR) {
      await writeFile(join(resolve(process.env.PREVIEW_DIR),`${job.name}.html`),await render(job));
    }
  }
} finally {
  for (const request of pending.values()) { clearTimeout(request.timer); request.reject(new Error('Exporter closing')); }
  socket?.close();
  if (browser && !browserError && browser.exitCode === null) {
    browser.kill('SIGTERM');
    await Promise.race([new Promise(resolve=>browser.once('exit',resolve)),sleep(3000)]);
    if (browser.exitCode === null) { browser.kill('SIGKILL'); await Promise.race([new Promise(resolve=>browser.once('exit',resolve)),sleep(1000)]); }
  }
  await rm(temporary,{recursive:true,force:true,maxRetries:5,retryDelay:200});
}
