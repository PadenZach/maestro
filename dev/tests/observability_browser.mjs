// Opt-in observability browser acceptance; supplied tools only, no downloads.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import { createServer } from 'node:net';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const { PLAYWRIGHT_MODULE, CHROMIUM_BIN, MAESTRO_BIN, CONSOLE_SCREENSHOT_DIR } = process.env;
assert(PLAYWRIGHT_MODULE && CHROMIUM_BIN && MAESTRO_BIN, 'Set PLAYWRIGHT_MODULE, CHROMIUM_BIN and MAESTRO_BIN explicitly');
const modulePath = resolve(PLAYWRIGHT_MODULE);
const { chromium } = await import(pathToFileURL(modulePath));
const { expect } = await import(pathToFileURL(join(dirname(modulePath), 'test.mjs')));
const peers = new Set();
let browser, server;
const end = Math.floor(Date.now() / 30000) * 30000;
const cohortTime = Math.floor((end - 2 * 3600000) / 3600000) * 3600000 + 10000;

async function until(check, label, timeout = 10000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  throw new Error(`Timed out waiting for ${label}`);
}
async function freePort() {
  const listener = createServer();
  await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
  const port = listener.address().port;
  await new Promise(resolve => listener.close(resolve));
  return port;
}
function workflow(id, status = 'SUCCESS', created = end - 1000, app = 'browser') {
  return { WorkflowUUID: id, WorkflowName: id.startsWith('cohort-') ? 'bucket_job' : id,
    Status: status, CreatedAt: String(created), UpdatedAt: String(created + 500),
    ApplicationName: app, ApplicationVersion: 'browser-observability-v1', QueueName: null,
    ExecutorID: 'browser', ParentWorkflowID: null, CompletedAt: status === 'PENDING' ? null : String(created + 500),
    Priority: '0', WasForkedFrom: false, Input: 'fixture input', Output: 'fixture output', Error: null };
}
function step(id, name, child = null, error = null) {
  return { function_id: id, function_name: name, child_workflow_id: child,
    output: error ? null : 'fixture result', error,
    started_at_epoch_ms: String(end - 1000), completed_at_epoch_ms: String(end - 500) };
}
function fixture(app = 'browser', unsupported = false) {
  const rows = [...Array.from({ length: 31 }, (_, i) => workflow(`cohort-${String(i).padStart(2, '0')}`,
    i % 2 ? 'MAX_RECOVERY_ATTEMPTS_EXCEEDED' : 'ERROR', cohortTime, app)),
    workflow('root', 'SUCCESS', end - 1000, app),
    { ...workflow('failed-child', 'ERROR', end - 1000, app), ParentWorkflowID: 'root' },
    { ...workflow('healthy', 'SUCCESS', end - 1000, app), ParentWorkflowID: 'root' },
    { ...workflow('active-child', 'PENDING', end - 1000, app), ParentWorkflowID: 'root' },
    { ...workflow('grandchild', 'SUCCESS', end - 1000, app), ParentWorkflowID: 'failed-child' },
    workflow('paged', 'SUCCESS', end - 1000, app), workflow('unknown', 'FUTURE_STATUS', end - 1000, app),
    workflow('missing-status', null, end - 1000, app), workflow('cancelled', 'CANCELLED', end - 1000, app),
    { ...workflow('older-pending', 'PENDING', end - 3 * 86400000, app), QueueName: 'jobs' },
    { ...workflow('older-enqueued', 'ENQUEUED', end - 3 * 86400000, app), QueueName: 'jobs' },
    { ...workflow('older-delayed', 'DELAYED', end - 3 * 86400000, app), QueueName: 'jobs' },
    { ...workflow('refused', 'SUCCESS', end - 1000, app), ParentWorkflowID: 'root' },
    workflow('legacy-owner', 'SUCCESS', end - 1000, null), workflow('foreign', 'ERROR', end - 1000, 'foreign')];
  return { app, rows, reads: [], unsupported, activeStatus: 'PENDING', steps: {
    root: [step(1, 'failed_payment', 'failed-child'), step(2, 'retry_payment', 'failed-child'),
      step(3, 'DBOS.getResult', 'failed-child', 'handled payment failure'), step(4, 'healthy_call', 'healthy'),
      step(5, 'missing_call', 'missing'), step(6, 'refused_call', 'refused'), step(7, 'active_call', 'active-child')],
    'failed-child': [step(1, 'grandchild_call', 'grandchild'), step(2, 'payment_step', null, 'payment refused')],
    grandchild: [step(1, 'cycle_call', 'root'), step(2, 'self_reference', 'grandchild')], healthy: [step(1, 'healthy_step'), step(2, 'peer_reference', 'active-child')],
    'active-child': [step(1, 'active_step')], paged: Array.from({ length: 55 }, (_, i) => step(i, `paged_step_${i}`)),
  } };
}
function filteredRows(state, b = {}) {
  let rows = state.rows.map(row => row.WorkflowUUID === 'active-child' ? { ...row, Status: state.activeStatus } : row);
  rows = rows.filter(row => (!b.workflow_uuids || b.workflow_uuids.includes(row.WorkflowUUID))
    && (!b.workflow_name || b.workflow_name.includes(row.WorkflowName))
    && (!b.status || b.status.includes(row.Status))
    && (!b.queue_name || b.queue_name.includes(row.QueueName))
    && (b.has_parent == null || (row.ParentWorkflowID != null) === b.has_parent)
    && (!b.application_name || row.ApplicationName == null || b.application_name.includes(row.ApplicationName))
    && (!b.workflow_id_prefix || b.workflow_id_prefix.some(prefix => row.WorkflowUUID.startsWith(prefix)))
    && (!b.start_time || Number(row.CreatedAt) >= Date.parse(b.start_time))
    && (!b.end_time || Number(row.CreatedAt) <= Date.parse(b.end_time)));
  if (b.sort_desc) rows.sort((a, b) => Number(b.CreatedAt) - Number(a.CreatedAt));
  return rows;
}
function aggregates(state, b) {
  const groups = new Map();
  for (const row of filteredRows(state, b)) {
    const group = { status: row.Status };
    if (b.group_by_queue_name) group.queue_name = row.QueueName;
    if (b.time_bucket_size_ms) group.time_bucket = String(Math.floor(Number(row.CreatedAt) / b.time_bucket_size_ms) * b.time_bucket_size_ms);
    const key = JSON.stringify(group);
    if (!groups.has(key)) groups.set(key, { group, count: 0, min_created_at: null, max_queue_wait_ms: null, max_total_latency_ms: null });
    groups.get(key).count++;
  }
  return [...groups.values()];
}
async function fakeExecutor(base, state, executor = state.app) {
  const socket = new WebSocket(base.replace('http:', 'ws:') + `/websocket/${state.app}/fixture`);
  peers.add(socket);
  socket.addEventListener('close', () => peers.delete(socket));
  socket.addEventListener('message', event => {
    const request = JSON.parse(event.data), b = request.body || {}, id = request.workflow_id;
    // Recovery discovery is separate from Console read-cost assertions.
    if (request.type === 'get_workflow_aggregates' && b.group_by_executor_id && b.group_by_application_version) {
      assert.deepEqual(b.status, ['PENDING']);
      assert.deepEqual(b.application_name, [state.app]);
      socket.send(JSON.stringify({ type: request.type, request_id: request.request_id, output: [] }));
      return;
    }
    state.reads.push(request);
    let response;
    switch (request.type) {
      case 'executor_info': response = { executor_id: executor, application_version: 'browser-observability-v1', language: 'python', dbos_version: '3.1.0' }; break;
      case 'get_workflow': {
        if (id === 'refused') { response = { error_message: 'fixture workflow refusal' }; break; }
        const row = filteredRows(state, { workflow_uuids: [id] })[0];
        response = { output: row ? { ...row, Input: request.load_input ? row.Input : null, Output: request.load_output ? row.Output : null } : null };
        break;
      }
      case 'list_steps': {
        if (id === 'paged' && state.failFirstStepPage && !request.offset) {
          response = { error_message: 'fixture first step page refusal' }; break;
        }
        const rows = state.steps[id] || [];
        response = { output: rows.slice(request.offset || 0, (request.offset || 0) + (request.limit ?? rows.length))
          .map(row => request.load_output ? row : { ...row, output: null, error: null }) };
        break;
      }
      case 'list_workflows': {
        const rows = filteredRows(state, b);
        response = { output: rows.slice(b.offset || 0, (b.offset || 0) + (b.limit ?? rows.length))
          .map(row => ({ ...row, Input: b.load_input ? row.Input : null, Output: b.load_output ? row.Output : null })) };
        break;
      }
      case 'get_step_aggregates': response = { output: [{ group: { function_name: 'payment_step', status: 'ERROR' }, count: 1, max_duration_ms: null }] }; break;
      case 'get_workflow_aggregates': response = state.unsupported ? { error_message: 'fixture aggregates unsupported' } : { output: aggregates(state, b) }; break;
      case 'list_schedules': response = { output: b.application_name?.some(app => app !== state.app) || b.status && !b.status.includes('ACTIVE') ? [] : Array.from({ length: 3000 }, (_, i) => ({
        schedule_id: `schedule-${i}`, schedule_name: `schedule-${i}`, workflow_name: 'scheduled_job', workflow_class_name: null,
        schedule: '0 0 * * *', status: 'ACTIVE', context: null, last_fired_at: null, automatic_backfill: false,
        cron_timezone: 'UTC', queue_name: null, application_name: state.app,
      })) }; break;
      case 'get_workflow_events': response = { events: [] }; break;
      case 'get_workflow_notifications': response = { notifications: [] }; break;
      case 'get_workflow_streams': response = { streams: [] }; break;
      default: response = { error_message: `unexpected fixture command ${request.type}` };
    }
    socket.send(JSON.stringify({ ...response, type: request.type, request_id: request.request_id }));
  });
  await until(async () => (await (await fetch(base + '/api/executors')).json()).some(peer => peer.executor_id === executor), `${executor} registration`);
  return socket;
}
const panel = (page, name) => page.locator(`[data-panel="${name}"]`);
const node = (page, id) => page.locator(`[data-flow-node="${id}"]`);
async function screenshot(page, name) {
  if (CONSOLE_SCREENSHOT_DIR) { await mkdir(CONSOLE_SCREENSHOT_DIR, { recursive: true }); await page.screenshot({ path: join(CONSOLE_SCREENSHOT_DIR, name) }); }
}
async function loadedPanels(page) {
  for (const kind of ['activity', 'workload', 'recent', 'schedules']) {
    await expect(panel(page, kind).locator('[data-loaded="true"]')).toHaveCount(1);
    await expect(panel(page, kind)).not.toHaveAttribute('aria-busy', 'true');
  }
}
async function overview(page, base, state) {
  await page.goto(base + '/apps/browser');
  await loadedPanels(page);
  await expect(page.locator('.application-aggregates-link')).toHaveCount(0);
  const reads = state.reads.filter(r => ['get_workflow_aggregates', 'list_workflows', 'list_schedules'].includes(r.type));
  assert.equal(reads.filter(r => r.type === 'get_workflow_aggregates').length, 2, 'one activity and one workload aggregate, independent of executor count');
  assert.equal(reads.filter(r => r.type === 'list_workflows').length, 1, 'only one bounded recent list, no workflow scan to compute totals');
  assert.equal(reads.filter(r => r.type === 'list_schedules').length, 1);
  const activityRead = reads.find(r => r.type === 'get_workflow_aggregates' && r.body.start_time);
  const recentRead = reads.find(r => r.type === 'list_workflows');
  assert.equal(recentRead.body.limit, 10);
  assert.equal(recentRead.body.load_input, false); assert.equal(recentRead.body.load_output, false);
  assert.deepEqual(recentRead.body.application_name, ['browser']);
  assert.equal(activityRead.body.select_count, true);
  assert(activityRead.body.time_bucket_size_ms > 0, 'activity has bounded creation buckets');
  const cohort = filteredRows(state, activityRead.body);
  await expect(panel(page, 'activity').locator('.overview-count').first().locator('strong')).toHaveText(String(cohort.length));
  await expect(panel(page, 'activity').locator('.overview-count.failed strong')).toHaveText(String(cohort.filter(row => ['ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED'].includes(row.Status)).length));
  assert.equal(await panel(page, 'activity').locator('.overview-segment.failed').evaluateAll(items => items.reduce((sum, item) => sum + Number(item.textContent), 0)), 32, 'chart segments and failed count agree');
  await expect(panel(page, 'activity')).toContainText('missing statuses');
  await expect(panel(page, 'schedules').locator('.overview-schedule-count strong')).toHaveText('3000');
  await expect(panel(page, 'recent').locator('tbody tr')).toHaveCount(10);
  await expect(panel(page, 'recent')).not.toContainText('older-');
  await expect(panel(page, 'recent')).not.toContainText('foreign');
  await expect(panel(page, 'workload').locator('.overview-count').first().locator('strong')).toHaveText('4');
  const queued = panel(page, 'workload').getByRole('link', { name: '2 enqueued or delayed' });
  const queuedURL = new URL(await queued.getAttribute('href'), base);
  assert.deepEqual(queuedURL.searchParams.getAll('status'), ['ENQUEUED', 'DELAYED']);
  assert(!queuedURL.searchParams.has('start_time'), 'older queued work has no creation bound');
  await queued.click();
  await expect(page.locator('#wf-rows tbody tr')).toHaveCount(2);
  await expect(page.locator('#wf-rows')).toContainText('older-enqueued');
  await expect(page.locator('#wf-rows')).toContainText('older-delayed');
  await page.goBack();
  await loadedPanels(page);
  await screenshot(page, 'observability-overview.png');
  const bucket = panel(page, 'activity').locator('.overview-segment.failed').filter({ hasText: /^31$/ });
  const bucketURL = new URL(await bucket.getAttribute('href'), base);
  assert.deepEqual(bucketURL.searchParams.getAll('status'), ['ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED']);
  assert(bucketURL.searchParams.get('start_time').endsWith('Z') && bucketURL.searchParams.get('end_time').endsWith('Z'));
  await bucket.click();
  await expect(page.locator('#wf-rows tbody tr')).toHaveCount(25);
  await expect(page.locator('#wf-status input[value="ERROR"]')).toBeChecked();
  await expect(page.locator('#wf-status input[value="MAX_RECOVERY_ATTEMPTS_EXCEEDED"]')).toBeChecked();
  await expect(page.locator('#wf-start-wire')).toHaveValue(bucketURL.searchParams.get('start_time'));
  await expect(page.getByRole('checkbox', { name: 'Show child workflows' })).toBeChecked();
  await page.getByRole('button', { name: 'Next' }).click();
  await expect(page.locator('#wf-rows tbody tr')).toHaveCount(6);
  const pageTwo = new URL(page.url());
  assert.equal(pageTwo.pathname, '/apps/browser/workflows');
  assert.equal(pageTwo.searchParams.get('offset'), '25');
  assert.deepEqual(pageTwo.searchParams.getAll('status'), bucketURL.searchParams.getAll('status'));
  assert.equal(pageTwo.searchParams.get('end_time'), bucketURL.searchParams.get('end_time'));
  await page.reload();
  await expect(page.locator('#wf-rows tbody tr')).toHaveCount(6);
  await page.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(page.locator('#wf-rows tbody tr')).toHaveCount(6);
  await page.goBack();
  await expect(page.locator('#wf-rows tbody tr')).toHaveCount(25);
  await screenshot(page, 'observability-drilldown.png');
}

async function workflowFilters(page, base, state) {
  await page.goto(base + '/apps/browser/workflows');
  await expect(page.getByRole('checkbox', { name: 'Show child workflows' })).not.toBeChecked();
  await expect(page.locator('#wf-rows tbody')).not.toContainText('failed-child');
  const rootRead = state.reads.filter(r => r.type === 'list_workflows').at(-1);
  assert.equal(rootRead.body.has_parent, false, 'top-level filtering is pushed to the executor');
  await expect(page.locator('#wf-rows tbody')).toContainText(new Intl.DateTimeFormat('en-US', { timeZone: 'UTC', month: 'short', day: 'numeric', year: 'numeric' }).format(new Date(end - 1000)));
  await page.getByRole('checkbox', { name: 'Show child workflows' }).check();
  await expect(page.locator('#wf-rows tbody')).toContainText('failed-child');
  await expect(page.locator('#wf-status')).not.toHaveAttribute('open', '');
  await page.locator('#wf-status summary').click();
  await page.locator('#wf-status input[value="ERROR"]').check();
  await expect(page.locator('#wf-rows tbody')).toContainText('failed-child');
  await expect(page.locator('#wf-rows tbody')).not.toContainText('healthy');
  await page.keyboard.press('Escape');
  await expect(page.locator('#wf-status summary')).toBeFocused();
  const bound = new Date(end - 86400000).toISOString();
  await page.locator('#wf-start').fill(bound.replace(/\.000Z$/, '').replace(/:00$/, ''));
  await page.locator('#wf-start').press('Tab');
  await expect.poll(() => new URL(page.url()).searchParams.get('start_time')).toBe(bound);
  await page.getByRole('button', { name: 'Refresh', exact: true }).click();
  await expect(page.locator('#wf-start-wire')).toHaveValue(bound);
  await page.reload();
  await expect(page.locator('#wf-start-wire')).toHaveValue(bound);
  await expect(page.getByRole('checkbox', { name: 'Show child workflows' })).toBeChecked();
  await screenshot(page, 'observability-workflow-filters.png');
  await page.locator('.breadcrumb').getByRole('link', { name: 'browser', exact: true }).click();
  await expect(page).toHaveURL(base + '/apps/browser');
  await loadedPanels(page);
}
async function loadNode(page, id) {
  await node(page, id).locator('.flow-card-toggle').click();
  await expect(node(page, id).locator('.flow-card-toggle')).toHaveAttribute('aria-expanded', 'true');
  await expect(page.getByRole('button', { name: 'Refresh flow', exact: true })).toBeEnabled();
}
async function refreshFlow(page, manual = true) {
  if (manual) await page.getByRole('button', { name: 'Refresh flow', exact: true }).click();
  else await page.evaluate(async () => { await Alpine.$data(document.querySelector('#flow-panel')).refreshFlow(false); });
  await expect(page.getByRole('button', { name: 'Refresh flow', exact: true })).toBeEnabled();
}
async function edgesClearCards(page) {
  const crossings = await page.evaluate(() => {
    const boxes = [...document.querySelectorAll('.flow-node')].map(node => node.getBoundingClientRect());
    return [...document.querySelectorAll('.flow-connection')].flatMap(path => {
      const matrix = path.getScreenCTM(), length = path.getTotalLength();
      for (let i = 0; i <= 100; i++) {
        const point = path.getPointAtLength(length * i / 100).matrixTransform(matrix);
        if (boxes.some(box => point.x > box.left + 1 && point.x < box.right - 1 && point.y > box.top + 1 && point.y < box.bottom - 1)) return [path.parentElement.getAttribute('aria-label')];
      }
      return [];
    });
  });
  assert.deepEqual(crossings, [], 'connection paths stay outside every workflow card');
}
async function collapsedEdgeInspection(page, state) {
  await edgesClearCards(page);
  const edge = page.locator('.flow-edge[data-owner="root"][data-step="3"]');
  await expect(edge).toHaveAttribute('role', 'button');
  const point = await edge.locator('.flow-connection').evaluate(path => {
    const length=path.getTotalLength(), start=path.getPointAtLength(0), end=path.getPointAtLength(length);
    if(end.x >= start.x) throw new Error('Return must point back toward the parent');
    const point=path.getPointAtLength(length*.5).matrixTransform(path.getScreenCTM());
    return {x:point.x,y:point.y};
  });
  const before=state.reads.length;
  await page.mouse.click(point.x,point.y);
  const drawer=page.getByRole('dialog',{name:'Workflow inspection'});
  await expect(drawer.locator('[data-inspected-workflow]')).toHaveAttribute('data-inspected-workflow','root');
  await expect(drawer.locator('[data-inspected-step]')).toHaveAttribute('data-inspected-step','3');
  await expect(drawer).toContainText('handled payment failure');
  await expect(edge).toHaveAttribute('aria-expanded','true');
  await expect(node(page,'root').locator('.flow-card-toggle')).toHaveAttribute('aria-expanded','false');
  assert(!state.reads.slice(before).some(read=>read.type==='list_steps' && read.workflow_id!=='root'),'edge inspection does not load referenced workflows');
  await page.keyboard.press('Escape');
  await expect(edge).toBeFocused();
  await refreshFlow(page,false);
  await expect(edge).toBeFocused();
  await edge.press('Space');
  await expect(drawer.locator('[data-inspected-step]')).toHaveAttribute('data-inspected-step','3');
  await page.keyboard.press('Escape');
  const toggle=node(page,'root').locator('.flow-card-toggle');
  await node(page,'root').locator('.flow-node-identity').click();
  await expect(toggle).toHaveAttribute('aria-expanded','true');
  await toggle.press('Enter');
  await expect(toggle).toHaveAttribute('aria-expanded','false');
}
async function flow(page, base, state) {
  await page.goto(base + '/apps/browser/workflows/root');
  await expect(page.getByRole('tab', { name: 'Timeline' })).toHaveAttribute('aria-selected', 'true');
  await expect(page.locator('#flow-panel')).not.toBeVisible();
  const initial = state.reads.length;
  await page.getByRole('tab', { name: 'Flow', exact: true }).press('Enter');
  await expect(node(page, 'root').locator('.flow-node-status .badge')).toHaveText('SUCCESS');
  await expect(node(page, 'failed-child')).toHaveCount(1);
  try { await expect(node(page, 'failed-child')).not.toHaveClass(/flow-placeholder/); } catch (error) { console.error('Flow hydration diagnostics', await page.evaluate(() => Alpine.$data(document.querySelector('#flow-panel')).nodes.map(node => ({id:node.id,loaded:node.loaded,error:node.error,state:node.state}))),state.reads.slice(initial)); throw error; }
  await expect(node(page, 'failed-child').locator('.flow-node-status .badge')).toHaveText('ERROR');
  await expect(node(page, 'root').locator('.flow-step:visible')).toHaveCount(0);
  await expect(node(page, 'failed-child').locator('.flow-step:visible')).toHaveCount(0);
  await expect(page.locator('.flow-help')).not.toHaveAttribute('open', '');
  await screenshot(page, 'observability-flow-collapsed.png');
  const discovered = state.reads.slice(initial);
  const metadata = discovered.find(r => r.type === 'list_steps' && r.workflow_id === 'root');
  assert.equal(metadata.load_output, false, 'initial root discovery omits result/error blobs');
  assert.equal(discovered.filter(r => r.type === 'list_steps' && r.workflow_id !== 'root').length, 0, 'child steps load only on explicit Show steps');
  assert(discovered.some(r => r.type === 'list_workflows' && r.body.workflow_uuids.includes('failed-child')), 'child names and statuses hydrate in a metadata batch');
  assert.equal(state.reads.slice(initial).filter(r => r.type === 'get_workflow').length, 1, 'discovered references do not trigger recursive reads');
  await collapsedEdgeInspection(page,state);
  await loadNode(page, 'root');
  await loadNode(page, 'failed-child');
  await loadNode(page, 'grandchild');
  await loadNode(page, 'healthy');
  await loadNode(page, 'active-child');
  await expect(node(page, 'grandchild')).toContainText('Calls root · cycle');
  await expect(node(page, 'root')).toHaveCount(1);
  await expect(node(page, 'failed-child')).toHaveCount(1);
  await expect(node(page, 'root').locator('.flow-step[data-flow-step="3"]')).toContainText('Error return from failed-child');
  await expect(page.locator('.flow-connection.return.error')).toHaveCount(1);
  await expect(node(page, 'root')).not.toHaveClass(/flow-failed/);
  await expect(node(page, 'failed-child')).toHaveClass(/flow-failed/);
  await edgesClearCards(page);
  await page.getByRole('button', { name: 'Focus on failures' }).click();
  await expect(node(page, 'root')).toBeVisible();
  await expect(node(page, 'failed-child')).toBeVisible();
  await expect(node(page, 'healthy')).toHaveCount(0);
  await page.locator('.flow-toolbar').scrollIntoViewIfNeeded();
  await page.locator('.flow-viewport').evaluate(element => { element.scrollLeft = 0; element.scrollTop = 0; });
  await screenshot(page, 'observability-flow-failure-focus.png');
  await page.getByRole('button', { name: 'Focus on failures' }).click();
  await expect(node(page, 'healthy')).toBeVisible();
  const childStep = node(page, 'failed-child').locator('[data-flow-step="2"] .flow-step-inspect');
  await page.evaluate(() => {
    window.fixtureOriginalFetch = window.fetch;
    window.fetch = function(input, options) {
      if (String(input).includes('/flow-status?')) window.fixtureFlowSignal = options.signal;
      return window.fixtureOriginalFetch.call(this, input, options);
    };
  });
  let releaseStatus, statusHeld = false;
  const statusBarrier = new Promise(resolve => { releaseStatus = resolve; });
  const statusPattern = '**/workflows/root/flow-status?*';
  await page.route(statusPattern, async route => {
    const response = await route.fetch();
    statusHeld = true;
    await statusBarrier;
    await route.fulfill({ response });
  });
  await page.getByRole('button', { name: 'Refresh flow', exact: true }).click();
  await until(() => statusHeld, 'held Flow status response');
  await childStep.press('Enter');
  await expect(node(page,'failed-child').locator('.flow-card-toggle')).toHaveAttribute('aria-expanded','true');
  const drawer = page.getByRole('dialog', { name: 'Workflow inspection' });
  await expect(drawer).toBeVisible();
  await expect(drawer.locator('[data-inspected-workflow]')).toHaveAttribute('data-inspected-workflow', 'failed-child');
  await expect(drawer.locator('[data-inspected-step]')).toHaveAttribute('data-inspected-step', '2');
  await expect(drawer).toContainText('payment refused');
  assert.equal(await page.evaluate(() => window.fixtureFlowSignal.aborted), false, 'opening inspection does not abort an overlapping Flow read');
  releaseStatus();
  await expect(page.getByRole('button', { name: 'Refresh flow', exact: true })).toBeEnabled();
  await expect(page.locator('.flow-progress')).toContainText('Statuses refreshed');
  await expect(drawer.locator('[data-inspected-workflow]')).toHaveAttribute('data-inspected-workflow', 'failed-child');
  await expect(drawer.locator('[data-inspected-step]')).toHaveAttribute('data-inspected-step', '2');
  await page.unroute(statusPattern);
  await page.evaluate(() => { window.fetch = window.fixtureOriginalFetch; delete window.fixtureOriginalFetch; delete window.fixtureFlowSignal; });
  await page.keyboard.press('Escape');
  await node(page, 'healthy').locator('.flow-card-toggle').click();
  const viewport = page.locator('.flow-viewport');
  await viewport.evaluate(element => { element.scrollLeft = 90; element.scrollTop = 55; });
  const before = await viewport.evaluate(element => [element.scrollLeft, element.scrollTop]);
  await refreshFlow(page);
  await expect(node(page, 'healthy').locator('.flow-card-toggle')).toHaveAttribute('aria-expanded', 'false');
  assert.deepEqual(await viewport.evaluate(element => [element.scrollLeft, element.scrollTop]), before, 'refresh preserves map viewport');
  await expect(node(page, 'grandchild')).toHaveCount(1);
  const budget = await page.evaluate(() => { const data = Alpine.$data(document.querySelector('#flow-panel')); return [data.nodes.length, Math.max(...data.nodes.map(node => node.depth)), data.stepCount]; });
  assert(budget[0] <= 25 && budget[1] <= 8 && budget[2] <= 1000, 'bounded graph traversal');
  await node(page, 'missing').getByRole('button', { name: 'Retry', exact: true }).click();
  await expect(node(page, 'missing')).toContainText('Workflow not found.');
  await node(page, 'refused').locator('.flow-card-toggle').click();
  await expect(node(page, 'refused')).toContainText('fixture workflow refusal');
  const beforeStatus = state.reads.length;
  state.activeStatus = 'SUCCESS';
  await refreshFlow(page, false);
  await expect(node(page, 'active-child').locator('.flow-node-status .badge')).toHaveText('SUCCESS');
  const batch = state.reads.slice(beforeStatus).find(r => r.type === 'list_workflows' && r.body.workflow_uuids);
  assert(batch?.body.workflow_uuids.includes('active-child') && batch.body.workflow_uuids.includes('root'), 'terminal parent retains child status polling');
  assert.equal(batch.body.load_input, false); assert.equal(batch.body.load_output, false);
  assert(batch.body.limit <= 25, 'bounded status batch');
  await page.locator('.flow-toolbar').scrollIntoViewIfNeeded();
  await viewport.evaluate(element => { element.scrollLeft = 0; element.scrollTop = 0; });
  await screenshot(page, 'observability-flow.png');
  await expandedFlow(page);
  const svg=page.locator('.flow-connections');
  const expandedHeight=Number(await svg.getAttribute('height'));
  for(const id of ['root','failed-child','grandchild','active-child']) { const toggle=node(page,id).locator('.flow-card-toggle'); if(await toggle.getAttribute('aria-expanded') === 'true') await toggle.click(); }
  await expect.poll(async () => Number(await svg.getAttribute('height'))).toBeLessThan(expandedHeight);
  assert(await viewport.evaluate(element => Math.abs(element.scrollHeight-element.clientHeight)<=1),'collapsed SVG cannot create a vertical scroll trap');
  await page.getByRole('tab', { name: 'Timeline' }).press('Enter');
  await expect(page.locator('.timeline')).toBeVisible();
  await page.getByRole('tab', { name: 'Flow', exact: true }).press('Enter');
  await expect(node(page, 'grandchild')).toHaveCount(1);
  await page.goto(base + '/apps/browser/workflows/paged');
  await page.getByRole('tab', { name: 'Flow', exact: true }).click();
  await expect(node(page, 'paged').locator('.flow-step:visible')).toHaveCount(0);
  await loadNode(page, 'paged');
  await expect(node(page, 'paged').locator('.flow-step:visible')).toHaveCount(50);
  await node(page, 'paged').getByRole('button', { name: 'Load more steps' }).click();
  await expect(node(page, 'paged').locator('.flow-step')).toHaveCount(55);
  await node(page, 'paged').locator('[data-flow-step="54"] .flow-step-inspect').press('Enter');
  await expect(drawer.locator('[data-inspected-workflow]')).toHaveAttribute('data-inspected-workflow', 'paged');
  await expect(drawer.locator('[data-inspected-step]')).toHaveAttribute('data-inspected-step', '54');
  const pagedReads = state.reads.filter(r => r.type === 'list_steps' && r.workflow_id === 'paged' && r.limit);
  assert(pagedReads.some(r => r.offset === 50), 'steps paginate with concrete offsets');
  assert(pagedReads.every(r => r.limit <= 51), 'step pages preserve bounded size');
  assert(pagedReads.filter(r => r.offset === 50).every(r => r.load_output === true), 'explicit later pages preserve failure evidence');
  assert(pagedReads.some(r => r.offset === 0 && r.load_output === true), 'Show steps requests first-page failure evidence');
  assert(pagedReads.some(r => r.offset === 0 && r.load_output === false), 'initial page discovery remains metadata only');
  await page.keyboard.press('Escape');
  state.failFirstStepPage = true;
  const beforeFailedRefresh = state.reads.length;
  await refreshFlow(page);
  const refreshedPages = state.reads.slice(beforeFailedRefresh).filter(r => r.type === 'list_steps' && r.workflow_id === 'paged' && r.limit === 51);
  assert.deepEqual(refreshedPages.map(r => r.offset), [0, 50], 'later successful page follows the failed first page');
  await expect(node(page, 'paged')).toHaveClass(/flow-stale/);
  await expect(node(page, 'paged')).toContainText('fixture first step page refusal');
  await expect(node(page, 'paged').locator('.flow-step')).toHaveCount(55);
  state.failFirstStepPage = false;
  const beforeRetry = state.reads.length;
  await node(page, 'paged').getByRole('button', { name: 'Retry', exact: true }).click();
  await expect(node(page, 'paged')).not.toHaveClass(/flow-stale/);
  assert(state.reads.slice(beforeRetry).some(r => r.type === 'list_steps' && r.workflow_id === 'paged' && r.offset === 0), 'retry reads the failed page');
  await expect(node(page, 'paged').locator('.flow-node-notice[role="status"]:visible')).toHaveCount(0);
}

async function mobile(page, base) {
  await page.setViewportSize({ width: 390, height: 844 });
  const fitsPage = () => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth);
  await page.goto(base + '/apps/browser');
  await loadedPanels(page);
  assert(await fitsPage(), 'mobile overview has no page-level horizontal overflow');
  await screenshot(page, 'observability-overview-mobile.png');
  await page.goto(base + '/apps/browser/workflows/root');
  await expect(page.getByRole('tab', { name: 'Timeline' })).toBeVisible();
  await page.getByRole('tab', { name: 'Flow', exact: true }).press('Enter');
  await expect(node(page, 'root').locator('.flow-node-status .badge')).toHaveText('SUCCESS');
  await loadNode(page, 'root');
  await loadNode(page, 'failed-child');
  assert(await fitsPage(), 'mobile Flow has no page-level horizontal overflow');
  const map = page.locator('.flow-viewport');
  assert(await map.evaluate(element => element.scrollWidth > element.clientWidth), 'wide graph scrolls inside its map');
  await map.evaluate(element => { element.scrollLeft = 90; });
  assert.equal(await map.evaluate(element => element.scrollLeft), 90, 'map supports horizontal exploration');
  assert.equal(await page.evaluate(() => scrollX), 0, 'map scrolling does not move the page horizontally');
  await map.evaluate(element => { element.scrollLeft = 0; });
  await page.locator('.flow-toolbar').scrollIntoViewIfNeeded();
  await screenshot(page, 'observability-flow-mobile.png');
  await node(page, 'root').locator('[data-flow-step="3"] .flow-step-inspect').press('Enter');
  const drawer = page.getByRole('dialog', { name: 'Workflow inspection' });
  await expect(drawer).toBeVisible();
  await expect(drawer.locator('[data-inspected-step]')).toHaveAttribute('data-inspected-step', '3');
  await expect(drawer).toHaveCSS('transform', 'none');
  const box = await drawer.boundingBox();
  assert(box.x >= 0 && box.x + box.width <= 390, 'mobile inspection drawer fits the viewport');
  await expect(drawer.getByRole('button', { name: 'Close details' })).toBeVisible();
  await page.keyboard.press('Escape');
  await page.getByRole('tab', { name: 'Timeline' }).press('Enter');
  await expect(page.locator('.timeline')).toBeVisible();
}

async function expandedFlow(page) {
  const viewport = page.locator('.flow-viewport');
  await expect(viewport).not.toHaveCSS('max-height', /px/);
  await page.locator('.flow-toolbar').scrollIntoViewIfNeeded();
  const before = await page.evaluate(() => { const data=Alpine.$data(document.querySelector('#flow-panel')); return {nodes:data.nodes.map(node => [node.id,node.pages,node.collapsed]),overflow:document.body.style.getPropertyValue('overflow'),priority:document.body.style.getPropertyPriority('overflow')}; });
  const box=await viewport.boundingBox();
  await page.mouse.move(box.x + Math.min(100,box.width/2), Math.min(box.y+100,page.viewportSize().height-30));
  const initialScroll=await page.evaluate(() => scrollY);
  await page.mouse.wheel(0,350);
  await expect.poll(() => page.evaluate(() => scrollY)).toBeGreaterThan(initialScroll);
  assert.equal(await viewport.evaluate(element => element.scrollTop),0,'normal Flow vertical wheel scroll belongs to page');
  const expand=page.getByRole('button',{name:'Expand',exact:true});
  await expand.click();
  const surface=page.getByRole('dialog',{name:'Expanded workflow execution map',exact:true});
  await expect(surface).toBeVisible();
  await expect(surface).toHaveAttribute('aria-modal','true');
  await expect(surface.getByRole('button',{name:'Close expanded view'})).toBeFocused();
  const after=await page.evaluate(() => { const data=Alpine.$data(document.querySelector('#flow-panel')); return data.nodes.map(node => [node.id,node.pages,node.collapsed]); });
  assert.deepEqual(after,before.nodes,'Expand preserves loaded nodes, pages and folding');
  assert.equal(await page.evaluate(() => document.body.style.overflow),'hidden','expanded modal locks page scroll');
  assert.equal(await page.locator('.topbar').evaluate(element => element.inert),true,'background becomes inert');
  const first=surface.getByRole('button',{name:'Focus on failures'});
  const last=node(page,'grandchild').locator('.flow-reference button').last();
  await last.focus(); await page.keyboard.press('Tab'); await expect(first).toBeFocused();
  await first.focus(); await page.keyboard.press('Shift+Tab'); await expect(last).toBeFocused();
  await screenshot(page,'observability-flow-expanded.png');
  await node(page,'root').locator('[data-flow-step="3"] .flow-step-inspect').press('Enter');
  const drawer=page.getByRole('dialog',{name:'Workflow inspection'});
  await expect(drawer).toBeVisible();
  await expect(surface).toHaveAttribute('aria-owns','workflow-drawer');
  await expect(drawer.locator('[data-inspected-step]')).toHaveAttribute('data-inspected-step','3');
  await page.keyboard.press('Escape');
  await expect(drawer).not.toBeVisible();
  await expect(surface).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(surface).toHaveCount(0);
  await expect(expand).toBeFocused();
  assert.deepEqual(await page.evaluate(() => [document.body.style.getPropertyValue('overflow'),document.body.style.getPropertyPriority('overflow')]),[before.overflow,before.priority],'Close restores exact body overflow');
  assert.equal(await page.locator('.topbar').evaluate(element => element.inert),false,'background inert state restores');
}
async function aggregateGateOff(base,state) {
  const before=state.reads.length;
  for(const path of ['/apps/browser/aggregates/workflows','/apps/browser/aggregates/steps']) {
    const response=await fetch(base+path);assert.equal(response.status,404,'aggregate Console is opt-in');
  }
  for(const [path,method] of [['/v2/orgs/local/apps/browser/workflows/aggregates','POST'],['/v2/orgs/local/apps/browser/steps/aggregates','POST']]) {
    const response=await fetch(base+path,{method,...(method==='POST'?{headers:{'Content-Type':'application/json'},body:'{"selectCount":true}'}:{})});
    assert.equal(response.status,404,'aggregate API is opt-in');
  }
  assert.equal(state.reads.length,before,'disabled aggregate routes issue no executor query');
}
async function aggregateViewer(page,base,state) {
  await page.setViewportSize({width:1280,height:960});
  await page.goto(base+'/apps/browser');await loadedPanels(page);
  await expect(page.locator('.application-aggregates-link')).toBeVisible();
  await page.locator('.application-aggregates-link').click();
  await expect(page.locator('.aggregate-advanced')).not.toHaveAttribute('open','');
  await expect(page.locator('.aggregate-basics input[name="groupByStatus"]')).toBeChecked();
  await expect(page.locator('.aggregate-basics input[name="selectCount"]')).toBeChecked();
  await expect(page.locator('.aggregate-results th')).toHaveText(['Group','Count']);
  await expect(page.locator('.aggregate-group')).not.toHaveCount(0);
  await expect(page.locator('.aggregate-results')).not.toContainText('Not selected');
  await expect(page.locator('.aggregate-results')).not.toContainText('"status":');
  await expect(page.locator('.aggregate-results')).toContainText('Unknown');
  await screenshot(page,'observability-aggregates.png');
  await page.locator('.aggregate-advanced summary').click();
  await page.locator('input[name="timeBucketSeconds"]').fill('1.25');
  await page.locator('.aggregate-status-options input[value="ERROR"]').check();
  const bound=new Date(end-86400000).toISOString();
  await page.locator('input[name="startTime"]').fill(bound.replace(/\.000Z$/, '').replace(/:00$/, ''));
  await page.getByRole('button',{name:'Apply',exact:true}).click();
  await expect.poll(() => state.reads.filter(r => r.type==='get_workflow_aggregates').at(-1)?.body.time_bucket_size_ms).toBe(1250);
  await expect(page.locator('.aggregate-results th')).toHaveText(['Group','Count']);
  const request=state.reads.filter(r => r.type==='get_workflow_aggregates').at(-1);
  assert.deepEqual(request.body.status,['ERROR']);
  assert.equal(Date.parse(request.body.start_time),Date.parse(bound));
  await expect.poll(() => new URL(page.url()).searchParams.get('timeBucketSeconds'),{message:'HTMX preserves seconds query'}).toBe('1.25');
  await expect(page.locator('input[name="timeBucketSeconds"]')).toHaveValue('1.25');
  await expect(page.locator('.aggregate-status-options input[value="ERROR"]')).toBeChecked();
  await expect(page.locator('.aggregate-results')).toContainText('Bucket (UTC)');
  await expect(page.locator('.aggregate-results')).toContainText('UTC');
  await screenshot(page,'observability-aggregates-advanced.png');
  await page.setViewportSize({width:390,height:844});
  assert(await page.evaluate(() => document.documentElement.scrollWidth<=innerWidth),'aggregate viewer contains mobile horizontal overflow');
  await screenshot(page,'observability-aggregates-mobile.png');
  await page.getByRole('link',{name:'Steps',exact:true}).click();
  await page.locator('.aggregate-advanced summary').click();
  await expect(page.locator('.aggregate-status-options input')).toHaveCount(2);
  await expect(page.locator('.aggregate-status-options')).toContainText('SUCCESS');
  await expect(page.locator('.aggregate-status-options')).toContainText('ERROR');
  await expect(page.locator('.aggregate-status-options')).not.toContainText('PENDING');
}

try {
  const port = await freePort(), base = `http://127.0.0.1:${port}`;
  server = spawn(MAESTRO_BIN, ['--listen', `127.0.0.1:${port}`], { stdio: 'ignore', env: { PATH: process.env.PATH } });
  await until(async () => { assert.equal(server.exitCode, null, 'maestro exited before readiness'); try { return (await fetch(base + '/healthz')).ok; } catch { return false; } }, 'maestro readiness');
  const state = fixture(), unsupported = fixture('unsupported', true);
  let peer = await fakeExecutor(base, state);
  const second = await fakeExecutor(base, state, 'browser-second');
  await fakeExecutor(base, fixture('other'));
  await fakeExecutor(base, unsupported);
  browser = await chromium.launch({ executablePath: CHROMIUM_BIN, headless: true });
  const context = await browser.newContext({ viewport: { width: 1280, height: 960 } });
  const page = await context.newPage(), errors = [];
  page.on('pageerror', error => { errors.push(error.message); console.error('Browser error:', error.message); });
  page.setDefaultTimeout(10000);
  await overview(page, base, state);
  await workflowFilters(page, base, state);
  await flow(page, base, state);
  await page.goto(base + '/apps/unsupported');
  await expect(panel(page, 'activity')).toContainText('fixture aggregates unsupported');
  await expect(panel(page, 'workload')).toContainText('fixture aggregates unsupported');
  await expect(panel(page, 'activity').locator('[data-loaded="true"]')).toHaveCount(0);
  await expect(panel(page, 'recent').locator('tbody tr')).toHaveCount(10);
  await expect(panel(page, 'schedules').locator('.overview-schedule-count strong')).toHaveText('3000');
  await screenshot(page, 'observability-independent-panels.png');
  await page.goto(base + '/apps/browser');
  await loadedPanels(page);
  const count = await panel(page, 'activity').locator('.overview-count').first().locator('strong').textContent();
  peer.close(); second.close();
  await until(async () => !(await (await fetch(base + '/api/executors')).json()).some(peer => peer.app === 'browser'), 'selected app disconnect');
  await page.locator('.overview-controls').getByRole('button', { name: 'Refresh' }).click();
  for (const kind of ['activity', 'workload', 'recent', 'schedules']) {
    try { await expect(panel(page, kind).locator('.overview-freshness')).toContainText('Stale'); }
    catch (error) { console.error(await page.evaluate(() => ({ busy: Alpine.$data(document.querySelector('.application-overview')).busy, availability: document.querySelector('[data-overview-availability]').textContent }))); throw error; }
    await expect(panel(page, kind)).toContainText('Showing retained data');
  }
  await expect(panel(page, 'activity').locator('.overview-count').first().locator('strong')).toHaveText(count);
  await expect(page.locator('[data-overview-availability]')).toContainText('No connected executors for this application');
  await expect(page.locator('.application-status .status-dot')).toHaveClass(/status-green/);
  await screenshot(page, 'observability-stale-disconnect.png');
  peer = await fakeExecutor(base, state);
  await page.goto(base + '/apps/browser');
  await loadedPanels(page);
  await expect(page.locator('.status-row')).toContainText('Available');
  await mobile(page, base);
  await aggregateGateOff(base, state);
  for (const peer of peers) peer.close();
  server.kill('SIGTERM');
  await until(() => server.exitCode !== null, 'default fixture shutdown');
  const enabledPort = await freePort(), enabledBase = `http://127.0.0.1:${enabledPort}`;
  server = spawn(MAESTRO_BIN, ['--listen', `127.0.0.1:${enabledPort}`, '--enable-aggregates'], { stdio: 'ignore', env: { PATH: process.env.PATH } });
  await until(async () => { try { return (await fetch(enabledBase + '/healthz')).ok; } catch { return false; } }, 'enabled fixture readiness');
  const enabledState = fixture();
  await fakeExecutor(enabledBase, enabledState);
  await aggregateViewer(page, enabledBase, enabledState);
  assert.deepEqual(errors, [], 'browser JavaScript errors');
  console.log('PASS observability browser: overview counts/read cost, workflow filters/drilldown history, metadata-first bounded Flow, pagination/stale retry, failure focus/drawer, modal focus/inert/scroll restoration, page wheel/SVG collapse, independent panels/disconnect, default aggregate gate and enabled seconds/status/date HTMX form, desktop/mobile containment');
} finally {
  for (const peer of peers) peer.close();
  await browser?.close();
  if (server && server.exitCode === null) { server.kill('SIGTERM'); await until(() => server.exitCode !== null, 'owned maestro shutdown'); }
}
