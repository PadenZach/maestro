// Opt-in browser acceptance. Dependencies and the browser are explicitly supplied;
// this script never downloads tools or connects to an operator's database.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const modulePath = process.env.PLAYWRIGHT_MODULE;
const executablePath = process.env.CHROMIUM_BIN;
assert(modulePath && executablePath, 'Set PLAYWRIGHT_MODULE and CHROMIUM_BIN explicitly');
const { chromium } = await import(pathToFileURL(resolve(modulePath)));
const { expect } = await import(pathToFileURL(join(dirname(resolve(modulePath)), 'test.mjs')));
const mode = process.argv[2] || 'fake';
assert(['fake', 'real'].includes(mode), 'Expected fake or real mode');
const version = 'postgres-gate-build-123456';
const longVersion = 'build-' + '0123456789abcdef'.repeat(30);
const opaque = 'opaque:<script>window.payloadExecuted=true</script>' + 'x'.repeat(600);
const peers = new Set();
let server;
let browser;

async function until(check, label, timeout = 10000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  throw new Error(`Timed out waiting for ${label}`);
}

async function freePort() {
  const listener = createServer();
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  const { port } = listener.address();
  await new Promise(resolve => listener.close(resolve));
  return port;
}

function workflow(id, name = 'gate_workflow', status = 'SUCCESS') {
  return {
    WorkflowUUID: id, WorkflowName: name, Status: status,
    CreatedAt: '1700000000000', UpdatedAt: '1700000001000',
    ApplicationVersion: version, ApplicationName: 'browser',
    Priority: '0', WasForkedFrom: false, QueueName: '',
    Input: opaque, Output: opaque, Error: null,
    ParentWorkflowID: id === 'child' ? 'root' : id === 'grandchild' ? 'child' : null,
    CompletedAt: status === 'PENDING' ? null : '1700000001000',
  };
}

function step(child, name = 'child_call', id = 1) {
  return { function_id: id, function_name: name, output: opaque, error: null,
    child_workflow_id: child, started_at_epoch_ms: '1700000000000',
    completed_at_epoch_ms: '1700000001000' };
}

async function fakeExecutor(base, app, state) {
  const socket = new WebSocket(base.replace('http:', 'ws:') + `/websocket/${app}/fixture`);
  peers.add(socket);
  socket.addEventListener('close', () => peers.delete(socket));
  const rows = [...Array.from({ length: 60 }, (_, i) => workflow(`other-${i}`, 'billing')),
    ...Array.from({ length: 30 }, (_, i) => workflow(`match-${i}`))];
  socket.addEventListener('message', event => {
    const request = JSON.parse(event.data);
    const id = request.workflow_id;
    let response;
    switch (request.type) {
      case 'executor_info':
        response = { executor_id: app, application_version: version, language: 'python', dbos_version: '3.1.0' };
        break;
      case 'get_workflow':
        response = { output: id === 'missing' ? null : workflow(id, 'gate_workflow', id === 'root' ? 'PENDING' : id === 'failed' ? 'ERROR' : 'SUCCESS') };
        if (response.output && state.longVersion) response.output.ApplicationVersion = longVersion;
        if (response.output && state.noVersion) response.output.ApplicationVersion = null;
        break;
      case 'list_steps': {
        let steps = id === 'shared' ? [step('child', 'first_call'), step('child', 'second_call', 2)]
          : id === 'root' ? [step('child'), step('missing', 'missing_call', 2), step('empty', 'empty_call', 3), step('failed', 'failed_call', 4)]
          : id === 'child' ? [step('grandchild')]
          : id === 'grandchild' ? [step('root', 'cycle_call')]
          : [];
        if (id === 'root' && state.omitRootStep) steps = steps.filter(row => row.function_id !== 1);
        if (id === 'gantt') steps = [
          { ...step('gantt-child', 'start_child'), started_at_epoch_ms: '1000', completed_at_epoch_ms: '5000' },
          { ...step(null, 'finish', 2), started_at_epoch_ms: '4500', completed_at_epoch_ms: '5000' },
        ];
        if (id === 'gantt-child') steps = [{ ...step('gantt-grandchild', 'child_step'), started_at_epoch_ms: '2000', completed_at_epoch_ms: '3000' }];
        if (id === 'gantt-grandchild') steps = [{ ...step(null, 'grandchild_step'), started_at_epoch_ms: '2500', completed_at_epoch_ms: '2750' }];
        response = { output: steps };
        break;
      }
      case 'get_workflow_events': response = { events: [] }; break;
      case 'get_workflow_notifications':
        response = { notifications: [{ topic: null, message: '', created_at_epoch_ms: 1700000000000, consumed: false }] };
        break;
      case 'get_workflow_streams':
        response = state.refuse ? { error_message: 'fixture stream refusal' }
          : { streams: [{ key: 'ordered', values: ['first', opaque, 'last'] }] };
        break;
      case 'list_workflows': {
        const b = request.body || {};
        let selected = rows.filter(row => (!b.workflow_name?.length || b.workflow_name.includes(row.WorkflowName))
          && (!b.status?.length || b.status.includes(row.Status))
          && (!b.workflow_id_prefix?.length || b.workflow_id_prefix.some(prefix => row.WorkflowUUID.startsWith(prefix)))
          && (!b.queue_name?.length || b.queue_name.includes(row.QueueName)));
        response = { output: selected.slice(b.offset || 0, (b.offset || 0) + (b.limit ?? selected.length)) };
        break;
      }
      default: response = { error_message: 'unexpected fixture command' };
    }
    socket.send(JSON.stringify({ ...response, type: request.type, request_id: request.request_id }));
  });
  await until(async () => {
    const all = await (await fetch(base + '/api/executors')).json();
    return all.some(peer => peer.app === app);
  }, `${app} registration`);
  return socket;
}

const drawer = page => page.getByRole('dialog', { name: 'Workflow inspection' });

async function compactTimeline(page, base) {
  await page.goto(`${base}/apps/browser/workflows/gantt`);
  await expect(page.locator('.workflow-tools'), 'workflow instruction strip removed').toHaveCount(0);
  await expect(page.locator('body')).toHaveCSS('background-color', 'rgb(40, 44, 52)');
  await expect(page.locator('.tl-bar').first(), 'successful steps retain One Dark green').toHaveCSS('background-color', 'rgb(152, 195, 121)');
  const row = id => page.locator(`.step-inspection[data-step-workflow="${id}"]`).first().locator('xpath=ancestor::*[contains(concat(" ",normalize-space(@class)," ")," tl-row ")][1]');
  const rootRow = row('gantt');
  const fold = rootRow.getByRole('button', { name: 'Expand child workflow gantt-child', exact: true });
  await expect(fold, 'fold control belongs to the invoking step row').toBeVisible();
  await expect(row('gantt-child')).toHaveCount(0);
  const first = await rootRow.boundingBox();
  const second = await page.locator('.step-inspection[data-step-workflow="gantt"][data-step-id="2"]').boundingBox();
  assert(second.y - first.y <= 40, 'collapsed child occupies only its invoking row');
  await fold.press('Enter');
  await expect(row('gantt-child')).toBeVisible();
  await expect(drawer(page), 'folding must not open inspection').not.toBeVisible();
  await row('gantt-child').getByRole('button', { name: 'Expand child workflow gantt-grandchild', exact: true }).press('Enter');
  await expect(row('gantt-grandchild')).toBeVisible();
  await expect(page.locator('.tl-head'), 'one shared time axis').toHaveCount(1);
  // Read both rectangles in one layout snapshot: focusing a newly expanded
  // branch can scroll between separate browser round-trips.
  const footerFollowsContent = await page.evaluate(() => {
    const content = document.querySelector('main').getBoundingClientRect();
    const footer = document.querySelector('footer').getBoundingClientRect();
    return footer.top >= content.bottom - 1;
  });
  assert(footerFollowsContent, 'footer follows long content without covering it');
  const rootTrack = await rootRow.locator('.tl-track').boundingBox();
  const rootDuration = await rootRow.locator('.tl-dur-col').boundingBox();
  for (const [id, offset, width] of [['gantt-child', .25, .25], ['gantt-grandchild', .375, .0625]]) {
    const track = await row(id).locator('.tl-track').boundingBox();
    const bar = await row(id).locator('.tl-bar').boundingBox();
    const duration = await row(id).locator('.tl-dur-col').boundingBox();
    assert(Math.abs(track.x - rootTrack.x) < 1 && Math.abs(track.width - rootTrack.width) < 1, `${id} track aligns with root`);
    assert(Math.abs(bar.x - (rootTrack.x + rootTrack.width * offset)) < 1 && Math.abs(bar.width - rootTrack.width * width) < 1, `${id} uses root time scale`);
    assert(Math.abs(duration.x + duration.width - rootDuration.x - rootDuration.width) < 1, `${id} duration stays aligned`);
  }
  if (process.env.CONSOLE_SCREENSHOT_DIR) await page.screenshot({ path: join(process.env.CONSOLE_SCREENSHOT_DIR, 'compact-gantt.png') });
  await row('gantt-child').locator('.step-inspection').press('Enter');
  await expectSelection(page, 'gantt-child', 1);
  await expect(row('gantt-grandchild'), 'inspection preserves folding').toBeVisible();
  await page.keyboard.press('Escape');
  await rootRow.getByRole('button', { name: 'Collapse child workflow gantt-child', exact: true }).press('Enter');
  await expect(row('gantt-child')).not.toBeVisible();
  await page.locator('h1 .workflow-inspect').press('Enter');
  await expectSelection(page, 'gantt');
  await page.keyboard.press('Escape');
  await expect(page.locator('h1 .workflow-inspect')).toBeFocused();
}

async function expectSelection(page, workflow, stepID) {
  const body = drawer(page).locator('[data-inspected-workflow]');
  await expect(drawer(page)).toBeVisible();
  await expect(body).toHaveAttribute('data-inspected-workflow', workflow);
  if (stepID !== undefined) await expect(body).toHaveAttribute('data-inspected-step', String(stepID));
}

async function inspectTree(page, base, app, root, child, grandchild) {
  await page.goto(`${base}/apps/${app}/workflows/${root}`);
  const branches = page.locator('.child-branch');
  await branches.first().locator(':scope > .tl-row .tl-fold').press('Enter');
  const childStep = page.locator(`.step-inspection[data-step-workflow="${child}"]`).first();
  await expect(childStep).toBeVisible();
  await childStep.press('Enter');
  await expectSelection(page, child, await childStep.getAttribute('data-step-id'));
  await expect(drawer(page).getByRole('button', { name: 'Close details' })).toBeFocused();
  const nested = branches.first().locator('.child-content .child-branch').first();
  await nested.locator(':scope > .tl-row .tl-fold').press('Enter');
  await expect(page.locator(`.step-inspection[data-step-workflow="${grandchild}"]`).first()).toBeVisible();
  await branches.first().locator(':scope > .tl-row .tl-fold').press('Enter');
  await expect(childStep).not.toBeVisible();
  await branches.first().locator(':scope > .tl-row .tl-fold').press('Enter');
  await expect(childStep).toBeVisible();
  return childStep;
}

async function navigateParent(page, base, app, root, child) {
  await page.locator(`.child-link[href="/apps/${app}/workflows/${child}"]`).first().click();
  await expect(page).toHaveURL(`${base}/apps/${app}/workflows/${child}`);
  await page.getByRole('link', { name: `Parent workflow ${root}`, exact: true }).click();
  await expect(page).toHaveURL(`${base}/apps/${app}/workflows/${root}`);
}

async function refreshDetails(page) {
  await page.evaluate(() => new Promise(resolve => {
    const settled = event => {
      if (event.detail.target.id !== 'wf-live') return;
      document.removeEventListener('htmx:afterSettle', settled);
      resolve();
    };
    document.addEventListener('htmx:afterSettle', settled);
    htmx.ajax('GET', location.pathname + '/live', { target: '#wf-live', swap: 'innerHTML' });
  }));
}

async function refreshDuringInspection(page, base) {
  await page.goto(`${base}/apps/browser/workflows/root`);
  await page.evaluate(() => {
    const original = window.fetch;
    window.inspectionReads = 0;
    window.fetch = function(input, ...args) {
      if (String(input).endsWith('/inspect?step=1')) ++window.inspectionReads;
      return original.call(this, input, ...args);
    };
  });
  let release;
  const barrier = new Promise(resolve => { release = resolve; });
  let held = false;
  const pattern = '**/workflows/root/inspect?step=1';
  await page.route(pattern, async route => {
    const response = await route.fetch();
    held = true;
    await barrier;
    await route.fulfill({ response });
  });
  await page.locator('.step-inspection[data-step-workflow="root"][data-step-id="1"]').press('Enter');
  await until(() => held, 'slow inspection response');
  await refreshDetails(page);
  await refreshDetails(page);
  assert.equal(await page.evaluate(() => window.inspectionReads), 1, 'live refresh must let a pending inspection finish');
  release();
  await expectSelection(page, 'root', 1);
  await page.unroute(pattern);
}

async function independentBranches(page, base) {
  await page.goto(`${base}/apps/browser/workflows/shared`);
  const rootStep = page.locator('.step-inspection[data-step-workflow="shared"][data-step-id="1"]');
  const branches = page.locator('#wf-live > div > .timeline > .timeline-rows > .child-branch');
  await rootStep.press('Enter');
  await refreshDetails(page);
  await expectSelection(page, 'shared', 1);
  await expect(branches.first().locator(':scope > .tl-row .tl-fold'), 'inspection does not open its child branch').toHaveAttribute('aria-expanded', 'false');
  await page.keyboard.press('Escape');
  await expect(rootStep).toBeFocused();
  await branches.first().locator(':scope > .tl-row .tl-fold').press('Enter');
  const firstChild = branches.first().locator('.step-inspection[data-step-workflow="child"]').first();
  await firstChild.press('Enter');
  await branches.nth(1).locator(':scope > .tl-row .tl-fold').press('Enter');
  const otherChild = branches.nth(1).locator('.step-inspection[data-step-workflow="child"]').first();
  await expect(otherChild).toBeVisible();
  await expect(otherChild).toHaveAttribute('aria-expanded', 'false');
  await expectSelection(page, 'child', 1);
  await branches.nth(1).locator(':scope > .tl-row .tl-fold').press('Enter');
  await refreshDetails(page);
  await expect(firstChild).toHaveAttribute('aria-expanded', 'true');
  await expect(rootStep).toHaveAttribute('aria-expanded', 'false');
  await expect(branches.nth(1).locator(':scope > .tl-row .tl-fold'), 'sibling folding stays independent').toHaveAttribute('aria-expanded', 'false');
  await page.keyboard.press('Escape');
  await expect(firstChild, 'focus returns to the same ancestry after refresh').toBeFocused();
}

async function delayedInspection(page, base) {
  await page.goto(`${base}/apps/browser/workflows/shared`);
  // Ignore abort at the transport seam so the obsolete body really arrives.
  // Also verify the UI still aborts its signal: both defenses are required.
  await page.evaluate(() => {
    const original = window.fetch;
    window.fetch = function(input, options) {
      if (String(input).endsWith('/child/inspect?step=1')) {
        window.inspectionSignal = options.signal;
        return original.call(this, input, { ...options, signal: undefined });
      }
      return original.call(this, input, options);
    };
  });
  await page.locator('.child-branch').first().locator(':scope > .tl-row .tl-fold').press('Enter');
  const childStep = page.locator('.step-inspection[data-step-workflow="child"]').first();
  let held;
  let delivered;
  const pending = new Promise(resolve => { delivered = resolve; });
  const pattern = '**/workflows/child/inspect?step=1';
  await page.route(pattern, async route => {
    const response = await route.fetch();
    await new Promise(resolve => { held = resolve; });
    await route.fulfill({ response });
    delivered();
  });
  await childStep.press('Enter');
  await until(() => held, 'held child inspection response');
  await expect(drawer(page).locator('.drawer-body'), 'nested step inspection shows drawer loading').toHaveAttribute('aria-busy', 'true');
  await page.locator('.step-inspection[data-step-workflow="shared"][data-step-id="2"]').press('Enter');
  await expectSelection(page, 'shared', 2);
  assert(await page.evaluate(() => window.inspectionSignal.aborted), 'selection change aborts the previous read');
  held();
  await pending;
  await expectSelection(page, 'shared', 2);
  await page.unroute(pattern);

  held = undefined;
  const closed = new Promise(resolve => { delivered = resolve; });
  await page.route(pattern, async route => {
    const response = await route.fetch();
    await new Promise(resolve => { held = resolve; });
    await route.fulfill({ response });
    delivered();
  });
  await childStep.press('Enter');
  await until(() => held, 'pending inspection before close');
  await page.keyboard.press('Escape');
  assert(await page.evaluate(() => window.inspectionSignal.aborted), 'close aborts the selected read');
  held();
  await closed;
  await expect(drawer(page), 'a late response cannot reopen a closed drawer').not.toBeVisible();
  await expect(childStep).toBeFocused();
  await page.unroute(pattern);

  await page.route(pattern, route => route.abort());
  await childStep.press('Enter');
  await expect(drawer(page)).toContainText('Could not load details. Try again.');
  await page.unroute(pattern);
  await drawer(page).getByRole('button', { name: 'Retry', exact: true }).click();
  await expectSelection(page, 'child', 1);
}

async function clipboardLifecycle(page, base, errors) {
  await page.goto(`${base}/apps/browser/workflows/root`);
  await page.evaluate(() => Object.defineProperty(navigator, 'clipboard', {
    configurable: true, value: { writeText: () => new Promise(resolve => { window.finishCopy = resolve; }) },
  }));
  await page.locator('.version-copy').click();
  await page.mouse.move(0, 0);
  await refreshDetails(page);
  await page.evaluate(() => window.finishCopy());
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  assert.deepEqual(errors, [], 'clipboard completion after live refresh must not use removed tooltip refs');
  await expect(page.getByRole('tooltip')).not.toBeVisible();
  await page.locator('.version-copy').click();
  await page.mouse.move(0, 0);
  await page.keyboard.press('Escape');
  await page.evaluate(() => window.finishCopy());
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await expect(page.getByRole('tooltip'), 'pending clipboard completion must respect Escape').not.toBeVisible();
}

try {
  browser = await chromium.launch({ executablePath, headless: true });
  const context = await browser.newContext({ permissions: ['clipboard-read', 'clipboard-write'] });
  const page = await context.newPage();
  page.setDefaultTimeout(7000);
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  if (mode === 'fake') {
    assert(process.env.MAESTRO_BIN, 'Set MAESTRO_BIN to an explicitly built test binary');
    const port = await freePort();
    const base = `http://127.0.0.1:${port}`;
    server = spawn(process.env.MAESTRO_BIN, ['--listen', `127.0.0.1:${port}`], {
      stdio: 'ignore', env: { PATH: process.env.PATH },
    });
    await until(async () => {
      assert(server.exitCode === null, 'maestro exited before readiness');
      try { return (await fetch(base + '/healthz')).ok; } catch { return false; }
    }, 'maestro readiness');
    const state = { refuse: false };
    let peer = await fakeExecutor(base, 'browser', state);
    await fakeExecutor(base, 'other', state);

    await compactTimeline(page, base);

    await page.goto(`${base}/apps/browser/workflows/shared`);
    await page.locator('.step-inspection[data-step-workflow="shared"][data-step-id="1"]').click();
    await expect(page.getByRole('dialog', { name: 'Workflow inspection' }), 'step details open a flyout').toBeVisible();
    await page.keyboard.press('Escape');

    await page.goto(base);
    await expect(page.locator('.status-green'), 'online retains One Dark green').toHaveCSS('background-color', 'rgb(152, 195, 121)');
    const footer = page.getByRole('contentinfo');
    await expect(footer).toContainText('Maestro');
    await expect(footer.getByRole('link', { name: 'MIT License' })).toHaveAttribute('href', 'https://opensource.org/license/mit');
    await expect(footer).toContainText('Not affiliated with DBOS, Inc. in any way.');
    await expect(footer.locator('[aria-label="Maestro version"]')).toHaveText(/\S+/);
    const footerBox = await footer.boundingBox();
    assert(Math.abs(footerBox.y + footerBox.height - page.viewportSize().height) < 1, 'footer sits at bottom of short page');
    if (process.env.CONSOLE_SCREENSHOT_DIR) await page.screenshot({ path: join(process.env.CONSOLE_SCREENSHOT_DIR, 'footer.png') });
    const control = page.locator('.version-control').first();
    const copy = control.getByRole('button', { name: 'Copy application version', exact: true });
    await expect(copy, 'full application version copy control').toBeVisible();
    const cardBeforeTooltip = await page.locator('.app-card').first().boundingBox();
    await copy.hover();
    await expect(control.getByRole('tooltip')).toBeVisible();
    const cardAfterTooltip = await page.locator('.app-card').first().boundingBox();
    assert.deepEqual(cardAfterTooltip, cardBeforeTooltip, 'version tooltip must not reflow its card');
    await expect(control.getByRole('tooltip')).toBeVisible();
    await expect(control.getByRole('tooltip')).toHaveCSS('position', 'fixed');
    await expect(control.locator('.version-full')).toBeVisible();

    await copy.focus();
    await expect(control.locator('.version-full')).toBeVisible();
    await expect(control.locator('.version-full')).toHaveText(version);
    if (process.env.CONSOLE_SCREENSHOT_DIR) await page.screenshot({ path: join(process.env.CONSOLE_SCREENSHOT_DIR, 'version-tooltip.png') });
    await copy.press('Enter');
    await expect(control.locator('.version-feedback')).toContainText(/copied/i);
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), version);
    assert.equal(page.url(), base + '/');
    await page.evaluate(() => Object.defineProperty(navigator, 'clipboard', {
      configurable: true, value: { writeText: async () => { throw new Error('fixture denial'); } },
    }));
    await copy.click();
    await expect(control.locator('.version-feedback')).not.toContainText(/copied/i);
    await expect(control.locator('.version-full')).toBeVisible();
    await page.evaluate(() => Object.defineProperty(navigator, 'clipboard', { configurable: true, value: undefined }));
    await copy.press('Enter');
    await expect(control.locator('.version-feedback')).toContainText('Could not copy');

    await page.goto(`${base}/apps/browser/workflows`);
    const query = page.locator('input[name=name]');
    await query.fill(' GATE ');
    await query.press('Tab');
    await expect(page.locator('#wf-rows tbody tr')).toHaveCount(25);
    await expect(page.locator('#wf-rows')).not.toContainText('billing');
    await page.getByRole('button', { name: 'Next' }).click();
    await expect(page.locator('#wf-rows tbody tr')).toHaveCount(5);
    await page.getByRole('button', { name: 'Prev' }).click();
    await expect(page.locator('#wf-rows tbody tr')).toHaveCount(25);
    await page.locator('select[name=status]').selectOption('ERROR');
    await expect(page.locator('#wf-rows')).toContainText('No workflows match');

    await independentBranches(page, base);
    await delayedInspection(page, base);
    await refreshDuringInspection(page, base);
    await clipboardLifecycle(page, base, errors);
    const childStep = await inspectTree(page, base, 'browser', 'root', 'child', 'grandchild');
    await expect(page.locator('#wf-live > div > .status-row > .badge.running'), 'running retains One Dark blue').toHaveCSS('color', 'rgb(97, 175, 239)');
    await page.locator('.child-inspect').first().click();
    await expectSelection(page, 'child');
    await expect(drawer(page).locator('[data-field="workflowId"] .field-value')).toHaveText('child');
    await expect(drawer(page).getByRole('link', { name: 'Open full workflow' })).toHaveAttribute('href', '/apps/browser/workflows/child');
    await page.locator('.workflow-inspect').click();
    await expectSelection(page, 'root');
    await expect(drawer(page).locator('[data-field="input"] .field-value')).toHaveText(opaque);
    await expect(drawer(page).locator('[data-field="priority"] .field-value')).toHaveText('0');
    await expect(drawer(page).locator('[data-field="wasForkedFrom"] .field-value')).toHaveText('false');
    await expect(drawer(page).locator('[data-field="queueName"] .field-value')).toHaveText('""');
    assert.equal(await page.evaluate(() => window.payloadExecuted), undefined);
    await expect(drawer(page).locator('.stream-values li')).toHaveText(['first', opaque, 'last']);
    await expect(drawer(page).locator('section').filter({ has: page.getByRole('heading', { name: 'Notifications', exact: true }) }).locator('tbody td'))
      .toHaveText(['null', '""', '1700000000000', 'false']);
    await expect(drawer(page)).toContainText(/no events/i);
    if (process.env.CONSOLE_SCREENSHOT_DIR) {
      await expect(drawer(page)).toHaveCSS('transform', 'none');
      await page.screenshot({ path: join(process.env.CONSOLE_SCREENSHOT_DIR, 'drawer-desktop.png') });
    }
    const versionButton = page.locator('.version-copy').first();
    await versionButton.focus();
    await expect(page.getByRole('tooltip')).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(page.getByRole('tooltip')).not.toBeVisible();
    await expect(drawer(page), 'first Escape dismisses tooltip only').toBeVisible();
    await page.keyboard.press('Escape');
    await expect(drawer(page)).not.toBeVisible();
    await childStep.press('Enter');
    // Wait for the actual live DOM replacement, then check selected identity.
    await page.evaluate(() => {
      document.addEventListener('htmx:afterSwap', event => {
        if (event.detail.target.id === 'wf-live') document.body.dataset.liveSwapped = 'yes';
      });
    });
    await page.waitForFunction(() => document.body.dataset.liveSwapped === 'yes');
    await expect(childStep).toBeVisible();
    await expectSelection(page, 'child', 1);
    await expect(page.locator('body')).toContainText('Workflow relationship cycle');
    await page.locator('.child-branch[data-child-workflow="missing"]').first().locator(':scope > .tl-row .tl-fold').press('Enter');
    await expect(page.locator('body')).toContainText(/not found|missing workflow/i);
    await page.locator('.child-branch[data-child-workflow="empty"]').first().locator(':scope > .tl-row .tl-fold').press('Enter');
    await expect(page.locator('body')).toContainText('No steps recorded for this workflow.');
    await page.locator('.child-branch[data-child-workflow="failed"]').first().locator(':scope > .tl-row .tl-fold').press('Enter');
    await expect(page.locator('[data-child-status="ERROR"]')).toHaveText('Child workflow status: ERROR');
    await expect(page.locator('[data-child-status="ERROR"]'), 'failed child retains One Dark red').toHaveCSS('color', 'rgb(224, 108, 117)');
    await page.locator('.step-inspection[data-step-workflow="root"][data-step-id="1"]').press('Enter');
    state.omitRootStep = true;
    await expect(drawer(page)).toContainText('Step 1 in workflow root is unavailable after refresh.');
    await page.keyboard.press('Escape');
    await expect(page.locator('#workflow-view')).toBeFocused();
    state.omitRootStep = false;
    state.refuse = true;
    await page.reload();
    await expect(page.locator('.status-amber'), 'read failure retains One Dark warning yellow').toHaveCSS('background-color', 'rgb(229, 192, 123)');
    await page.locator('.workflow-inspect').click();
    await expect(drawer(page)).toContainText('fixture stream refusal');
    await expect(drawer(page)).toContainText(/no events/i);
    await page.setViewportSize({ width: 390, height: 844 });
    await expect(drawer(page)).toHaveCSS('transform', 'none');
    const box = await drawer(page).boundingBox();
    assert(box.x >= 0 && box.x + box.width <= 390, 'drawer fits a narrow viewport');
    await expect(drawer(page).getByRole('button', { name: 'Close details' })).toBeVisible();
    if (process.env.CONSOLE_SCREENSHOT_DIR) await page.screenshot({ path: join(process.env.CONSOLE_SCREENSHOT_DIR, 'drawer-mobile.png') });
    await page.keyboard.press('Escape');
    state.longVersion = true;
    await page.reload();
    await page.locator('.version-copy').focus();
    await expect(page.getByRole('tooltip').locator('.version-full')).toHaveText(longVersion);
    await expect(page.getByRole('tooltip')).toBeVisible();
    await expect.poll(async () => {
      const box = await page.getByRole('tooltip').boundingBox();
      return box && box.x >= 0 && box.x + box.width <= 390 && box.y >= 0 && box.y + box.height <= 844;
    }, { message: 'long version tooltip fits a narrow viewport' }).toBe(true);
    if (process.env.CONSOLE_SCREENSHOT_DIR) await page.screenshot({ path: join(process.env.CONSOLE_SCREENSHOT_DIR, 'tooltip-mobile.png') });
    await page.locator('.version-copy').press('Enter');
    await expect(page.locator('.version-feedback')).toContainText('copied');
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), longVersion);
    state.longVersion = false;
    state.noVersion = true;
    await page.reload();
    await expect(page.locator('#wf-live')).toContainText('Application version unavailable');
    await expect(page.locator('#wf-live .version-copy')).toHaveCount(0);
    state.noVersion = false;
    await navigateParent(page, base, 'browser', 'root', 'child');

    peer.close();
    await until(async () => (await (await fetch(base + '/api/executors')).json()).length === 1, 'selected app disconnect');
    await page.goto(`${base}/apps/browser`);
    await expect(page.locator('body')).toContainText('No connected executors for this application');
    await expect(page.locator('.status-dot')).toHaveClass(/status-green/);
    await page.goto(`${base}/apps/browser/workflows`);
    await expect(page.locator('.empty')).toContainText(/no executors|unavailable/i);
    await page.goto(base);
    await expect(page.locator('.app-name')).toHaveText(['other']);
    peer = await fakeExecutor(base, 'browser', state);
    await page.goto(`${base}/apps/browser`);
    await expect(page.locator('.badge')).toHaveText('Available');
    console.log('PASS browser: version/copy fallback, complete search paging, nested drawer inspection/refresh, related refusal, availability/reconnect');
  } else {
    const [base, readyFile] = process.argv.slice(3);
    assert(/^http:\/\/(127\.0\.0\.1|localhost):\d+$/.test(base), 'Real browser fixture requires an explicit loopback base');
    const ready = JSON.parse(await readFile(readyFile, 'utf8'));
    await inspectTree(page, base, ready.app, ready.parent, ready.child, ready.grandchild);
    await navigateParent(page, base, ready.app, ready.parent, ready.child);
    await page.goto(`${base}/apps/${ready.app}/workflows?name=GATE_CONSOLE_MATCH&status=SUCCESS`);
    await expect(page.locator('#wf-rows tbody tr')).toHaveCount(25);
    const listedIDs = () => page.locator('#wf-rows tbody tr td:nth-child(2) a').evaluateAll(
      links => links.map(link => decodeURIComponent(new URL(link.href).pathname.split('/').pop())));
    const first = await listedIDs();
    await page.getByRole('button', { name: 'Next' }).click();
    await expect(page.locator('#wf-rows tbody tr')).toHaveCount(6);
    assert.deepEqual([...first, ...await listedIDs()].sort(), [...ready.matches].sort(),
      'complete real SDK candidate paging');
    await page.goto(`${base}/apps/${ready.app}/workflows/${ready.workflow}`);
    await page.locator('.workflow-inspect').click();
    await expectSelection(page, ready.workflow);
    for (const field of ['input', 'output']) {
      const value = await drawer(page).locator(`[data-workflow-fields] [data-field="${field}"] .field-value`).textContent();
      assert.equal(createHash('sha256').update(value).digest('hex'), ready[`${field}_sha256`],
        `complete ${field} differs from SDK digest`);
    }
    console.log('PASS real SDK/Postgres browser: nested traversal, complete substring paging and opaque SDK digests');
  }
  assert.deepEqual(errors, [], 'browser JavaScript errors');
} finally {
  for (const peer of peers) peer.close();
  await browser?.close();
  if (server && server.exitCode === null) {
    server.kill('SIGTERM');
    await until(() => server.exitCode !== null, 'owned maestro shutdown');
  }
}
