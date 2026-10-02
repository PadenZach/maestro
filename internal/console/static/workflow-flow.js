"use strict";

// The graph contains only records the operator has chosen to load. These pure
// helpers share identity, reachability and failure semantics with browser tests.
const MaestroFlow = {
  limits: { workflows: 25, depth: 8, steps: 1000, page: 50 },
  failed(status) { return status === "ERROR" || status === "MAX_RECOVERY_ATTEMPTS_EXCEEDED"; },
  active(status) { return ["PENDING", "ENQUEUED", "DELAYED"].includes(status); },
  edges(nodes) {
    return nodes.flatMap(node => node.steps.filter(step => step.childWorkflowId).map(step => ({
      key: JSON.stringify([node.id, step.key]), owner: node.id, target: step.childWorkflowId,
      step: step.key, kind: step.relationship || "reference", error: step.hasError,
    })));
  },
  descendants(id, nodes) {
    const seen = new Set([id]);
    const adjacency = new Map();
    for (const edge of this.edges(nodes)) {
      if (!adjacency.has(edge.owner)) adjacency.set(edge.owner, []);
      adjacency.get(edge.owner).push(edge.target);
    }
    const queue = [id];
    for (let i = 0; i < queue.length; i++) {
      for (const target of adjacency.get(queue[i]) || []) {
        if (!seen.has(target)) {
          seen.add(target); queue.push(target);
        }
      }
    }
    return nodes.filter(node => seen.has(node.id));
  },
  focus(root, nodes) {
    const keep = new Set([root]);
    const queue = nodes.filter(node => this.failed(node.status) || node.steps.some(step => step.hasError)).map(node => node.id);
    const edges = this.edges(nodes);
    for (const id of queue) keep.add(id);
    for (let i = 0; i < queue.length; i++) {
      for (const edge of edges) {
        if (edge.target === queue[i] && !keep.has(edge.owner)) {
          keep.add(edge.owner); queue.push(edge.owner);
        }
      }
    }
    // Keep unresolved references attached to visible context: no failure found
    // in loaded data is not evidence that every unexpanded branch succeeded.
    for (const edge of edges) {
      if (keep.has(edge.owner) && nodes.some(node => node.id === edge.target && (!node.loaded || !node.detailsLoaded))) keep.add(edge.target);
    }
    return keep;
  },
  cycle(owner, target, nodes) {
    return owner === target || this.descendants(target, nodes).some(node => node.id === owner);
  },
  roundedPath(points) {
    let d = `M ${points[0].x} ${points[0].y}`;
    for (let i = 1; i < points.length - 1; i++) {
      const a = points[i - 1], b = points[i], c = points[i + 1];
      const before = Math.hypot(b.x - a.x, b.y - a.y), after = Math.hypot(c.x - b.x, c.y - b.y);
      if (!before || !after) continue;
      const radius = Math.min(8, before / 2, after / 2);
      d += ` L ${b.x + (a.x - b.x) * radius / before} ${b.y + (a.y - b.y) * radius / before}`;
      d += ` Q ${b.x} ${b.y} ${b.x + (c.x - b.x) * radius / after} ${b.y + (c.y - b.y) * radius / after}`;
    }
    return d + ` L ${points.at(-1).x} ${points.at(-1).y}`;
  },
};

document.addEventListener("alpine:init", () => {
  Alpine.data("workflowFlow", () => ({
    flowRoot: null, rootID: "", baseURL: "", nodes: [], paths: [],
    busy: false, started: false, failureFocus: false, message: "", lastRefresh: "",
    timer: null, resizeObserver: null, drawFrame: null, flowController: null,
    refreshCursor: 0, canvasWidth: 0, canvasHeight: 0, refreshDeadline: 0,
    expandedGraph: false, expandTrigger: null, inertElements: [], bodyOverflow: null,
    init() {
      this.flowRoot = this.$el;
      this.rootID = this.flowRoot.dataset.workflowId;
      this.baseURL = this.flowRoot.dataset.workflowBase;
      this.$watch("view", value => {
        if (value === "flow") {
          if (!this.started) this.start();
          this.redraw();
        } else if (this.expandedGraph) this.closeExpanded();
      });
      this.$watch("selection", () => this.redraw());
      this.$watch("drawerOpen", () => this.redraw());
      this.resizeObserver = new ResizeObserver(() => this.redraw());
      this.resizeObserver.observe(this.$refs.board);
      this.timer = setInterval(() => {
        if (this.view === "flow" && !document.hidden && !this.busy && this.started) this.refreshFlow();
      }, 10000);
    },
    async start() {
      this.started = true;
      this.nodes.push(this.placeholder(this.rootID, 0));
      await this.loadNode(this.nodes[0], 0, false);
    },
    placeholder(id, depth) {
      return { id, depth, name: id, status: null, state: "unloaded", loaded: false, detailsLoaded: false, metadataLoaded: false,
        steps: [], pages: [], pageReads: {}, pageErrors: {}, collapsed: true, hasMore: false, limited: false,
        stale: false, error: "", metadataError: "", referenceLimit: "", readAt: "", nextOffset: 0,
        workflowUrl: this.url(id), inspectUrl: this.url(id) + "/inspect" };
    },
    url(id) { return this.baseURL + encodeURIComponent(id).replace(/\./g, "%2E"); },
    statusClass(status) {
      return MaestroFlow.failed(status) ? "err" : status === "SUCCESS" ? "ok"
        : MaestroFlow.active(status) ? "running" : "muted";
    },
    get visibleNodes() {
      const keep = this.failureFocus ? MaestroFlow.focus(this.rootID, this.nodes) : null;
      return this.nodes.filter(node => !keep || keep.has(node.id));
    },
    get lanes() {
      const nodes = this.visibleNodes;
      return [...new Set(nodes.map(node => node.depth))].sort((a, b) => a - b)
        .map(depth => ({ depth, nodes: nodes.filter(node => node.depth === depth) }));
    },
    get stepCount() { return this.nodes.reduce((sum, node) => sum + node.steps.length, 0); },
    get loadedCount() { return this.nodes.filter(node => node.loaded).length; },
    get errorCount() { return this.nodes.reduce((sum, node) => sum + node.steps.filter(step => step.hasError).length, 0); },
    get failedCount() { return this.nodes.filter(node => MaestroFlow.failed(node.status)).length; },
    get incomplete() {
      return this.nodes.some(node => !node.detailsLoaded || node.hasMore || node.limited || node.error || node.stale)
        || MaestroFlow.edges(this.nodes).some(edge => !this.nodes.some(node => node.id === edge.target));
    },
    stamp(value) { return value ? new Date(value).toLocaleTimeString([], { timeZone: "UTC" }) + " UTC" : "Not refreshed"; },
    summary(node) {
      const branch = MaestroFlow.descendants(node.id, this.nodes);
      const loaded = branch.filter(item => item.loaded);
      const failed = loaded.filter(item => MaestroFlow.failed(item.status)).length;
      const errors = loaded.reduce((sum, item) => sum + item.steps.filter(step => step.hasError).length, 0);
      const incomplete = branch.some(item => !item.detailsLoaded || item.hasMore || item.limited || item.error || item.stale)
        || MaestroFlow.edges(branch).some(edge => !branch.some(item => item.id === edge.target));
      return `${failed} failed workflows · ${errors} step errors · ${loaded.length} loaded${incomplete ? " · incomplete" : ""}`;
    },
    async request(url) {
      const controller = new AbortController();
      this.flowController = controller;
      const timeout = this.refreshDeadline ? Math.max(1, Math.min(12000, this.refreshDeadline - Date.now())) : 12000;
      const deadline = setTimeout(() => controller.abort(), timeout);
      try {
        const response = await fetch(url, { signal: controller.signal, headers: { Accept: "application/json" } });
        if (!response.ok) throw new Error("The workflow read failed. Retry to refresh.");
        return await response.json();
      } finally { clearTimeout(deadline); }
    },
    async loadNode(node, offset = 0, details = true) {
      if (this.busy) return;
      if (this.stepCount >= MaestroFlow.limits.steps && !node.pages.includes(offset)) {
        this.message = "The view has reached 1,000 loaded steps. Open a workflow separately to continue.";
        return;
      }
      this.busy = true;
      this.message = "";
      try { await this.readPage(node, offset, details); }
      catch (error) {
        node.stale = node.loaded;
        node.error = error.name === "AbortError" ? "The read timed out. Retry to refresh." : error.message;
        node.pageErrors[details ? offset : "metadata"] = node.error;
        if (!node.loaded) node.state = "error";
      } finally { this.busy = false; this.redraw(); }
    },
    applyMetadata(node, data) {
      if (data.workflowLoaded || data.state === "ready") {
        node.name = data.name || node.id; node.status = data.status;
        node.parentId = data.parentId; node.loaded = true;
        if (node.id === this.rootID) {
          this.reportedStatus = node.status; this.reportedStatusKnown = true; this.reportedStatusStale = false;
        }
      }
    },
    async hydrateReferences(nodes) {
      if (!nodes.length) return;
      const query = new URLSearchParams();
      for (const node of nodes.slice(0, MaestroFlow.limits.workflows)) query.append("workflow_id", node.id);
      let data;
      try { data = await this.request(this.url(this.rootID) + "/flow-status?" + query); }
      catch (error) {
        for (const node of nodes) {
          node.state = "unavailable"; node.metadataError = error.name === "AbortError" ? "The metadata read timed out. Retry to refresh." : error.message;
          node.error = node.metadataError; node.stale = node.loaded;
          if (node.id === this.rootID) this.reportedStatusStale = true;
        }
        throw error;
      }
      if (!this.flowRoot.isConnected) return;
      for (const node of nodes) {
        const fresh = data.workflows?.find(item => item.id === node.id);
        if (data.state !== "ready" || !fresh || fresh.state !== "ready") {
          node.state = fresh?.state || (data.state !== "ready" ? data.state : "missing");
          node.metadataError = fresh?.error || data.error || "Workflow not found.";
          node.error = node.metadataError; node.stale = node.loaded;
          if (node.id === this.rootID) this.reportedStatusStale = true;
        } else {
          this.applyMetadata(node, fresh);
          node.state = "ready"; node.metadataError = "";
          node.error = Object.values(node.pageErrors)[0] || ""; node.stale = !!node.error;
          if (!node.detailsLoaded) node.readAt = data.readAt;
        }
      }
    },
    async readPage(node, offset, details = true) {
      const data = await this.request(this.url(node.id) + "/flow?offset=" + offset + "&details=" + details);
      if (!this.flowRoot.isConnected) return;
      if (data.id !== node.id) throw new Error("The executor returned a different workflow.");
      this.applyMetadata(node, data);
      if (data.state !== "ready") {
        node.state = data.state;
        node.error = data.error || (data.state === "missing" ? "Workflow not found." : "Workflow data is unavailable.");
        node.pageErrors[details ? offset : "metadata"] = node.error;
        node.stale = node.loaded;
        if (node.id === this.rootID && !data.workflowLoaded) this.reportedStatusStale = true;
        return;
      }
      node.state = "ready";
      node.loaded = true;
      // A step-read failure retains the previous page as visibly stale.
      if (data.error) { node.pageErrors[details ? offset : "metadata"] = data.error; node.error = data.error; node.stale = true; return; }
      delete node.pageErrors[details ? offset : "metadata"];
      if (details && offset === 0) delete node.pageErrors.metadata;
      node.metadataError = "";
      node.error = Object.values(node.pageErrors)[0] || "";
      node.stale = !!node.error;
      if (details) node.pageReads[offset] = data.readAt;
      // A later page refresh must not make older loaded pages appear newer.
      node.readAt = Object.values(node.pageReads).sort()[0] || data.readAt;
      const others = node.steps.filter(step => step.page !== offset);
      const budget = MaestroFlow.limits.steps - (this.stepCount - node.steps.length + others.length);
      const incoming = data.steps.slice(0, Math.max(0, budget)).map((step, index) => ({ ...step,
        key: step.id == null ? `unknown-${offset + index}` : String(step.id), page: offset,
      }));
      const byKey = new Map([...others, ...incoming].map(step => [step.key, step]));
      node.steps = [...byKey.values()].sort((a, b) => (a.id ?? Infinity) - (b.id ?? Infinity));
      if (details && !node.pages.includes(offset)) node.pages.push(offset);
      node.pages.sort((a, b) => a - b);
      node.metadataLoaded = true;
      if (details) node.detailsLoaded = data.detailsLoaded !== false;
      if (!details || offset === node.pages.at(-1)) {
        node.nextOffset = data.nextOffset;
        node.hasMore = data.hasMore;
      }
      node.limited = data.limited || incoming.length !== data.steps.length;
      // Resolve discovered identities in one metadata batch. Their steps remain
      // untouched until the operator selects Show steps on that workflow.
      const discovered = [];
      node.referenceLimit = "";
      for (const step of node.steps) {
        if (!step.childWorkflowId || this.nodes.some(item => item.id === step.childWorkflowId)) continue;
        if (this.nodes.length < MaestroFlow.limits.workflows && node.depth < MaestroFlow.limits.depth) {
          const reference = this.placeholder(step.childWorkflowId, node.depth + 1);
          this.nodes.push(reference);
          // Mutate Alpine's reactive proxy, not the raw object pushed above.
          discovered.push(this.nodes.at(-1));
        } else {
          node.referenceLimit = node.depth >= MaestroFlow.limits.depth ? "Depth limit reached. Open a referenced workflow to continue." : "Workflow limit reached. Open a referenced workflow to continue.";
        }
      }
      if (discovered.length) {
        try { await this.hydrateReferences(discovered); }
        catch (_) { /* Metadata failures stay on the affected reference cards. */ }
      }
      if (details && this.drawerOpen && this.selection?.url.startsWith(this.url(node.id) + "/inspect") && !this.loading) this.loadInspection();
    },
    referenceLabel(node, step) {
      if (!step.childWorkflowId) return "";
      const relation = step.relationship === "return" ? (step.hasError ? "Error return from" : "Return from")
        : step.relationship === "invocation" ? "Calls" : "References";
      const cycle = MaestroFlow.cycle(node.id, step.childWorkflowId, this.nodes);
      const missing = !this.nodes.some(item => item.id === step.childWorkflowId);
      const target = this.nodes.find(item => item.id === step.childWorkflowId);
      return `${relation} ${target?.name || step.childWorkflowId}${cycle ? " · cycle" : ""}${missing ? " · view limit reached" : ""}`;
    },
    follow(id) {
      const node = this.nodes.find(item => item.id === id);
      if (!node) { this.message = "This reference exceeds the view limit. Open its workflow to continue."; return; }
      const element = [...this.flowRoot.querySelectorAll("[data-flow-node]")].find(item => item.dataset.flowNode === id);
      if (element) { element.scrollIntoView({ block: "nearest", inline: "nearest", behavior: "auto" }); element.focus({ preventScroll: true }); }
    },
    toggleFocus() { this.failureFocus = !this.failureFocus; this.redraw(); },
    toggleCard(node, event) {
      if (!node.loaded || node.metadataError || this.busy || event.defaultPrevented
        || event.target.closest('a, button, input, .flow-steps') || window.getSelection()?.toString()) return;
      this.toggleNode(node);
    },
    async toggleNode(node) {
      if (this.busy || node.metadataError) return;
      if (node.collapsed && !node.detailsLoaded) await this.loadNode(node, 0, true);
      if (node.detailsLoaded) node.collapsed = !node.collapsed;
      this.redraw();
    },
    async retryNode(node) {
      if (node.id === this.rootID && !node.detailsLoaded && node.metadataError) {
        await this.loadNode(node, 0, false);
        return;
      }
      if (node.metadataError || (!node.metadataLoaded && node.id !== this.rootID && !Object.keys(node.pageErrors).length)) {
        if (this.busy) return;
        this.busy = true;
        try { await this.hydrateReferences([node]); }
        catch (error) { node.error = error.message; node.stale = node.loaded; }
        finally { this.busy = false; this.redraw(); }
        return;
      }
      const failed = Object.keys(node.pageErrors)[0];
      await this.loadNode(node, failed && failed !== "metadata" ? Number(failed) : 0, failed ? failed !== "metadata" : node.detailsLoaded);
    },
    expandGraph(source) {
      if (this.expandedGraph) return;
      this.expandTrigger = source;
      this.bodyOverflow = { value: document.body.style.getPropertyValue("overflow"), priority: document.body.style.getPropertyPriority("overflow") };
      document.body.style.setProperty("overflow", "hidden");
      const drawer = document.getElementById("workflow-drawer");
      const isolate = element => {
        if (element === this.flowRoot || element === drawer) return;
        if (element.contains(this.flowRoot) || (drawer && element.contains(drawer))) {
          for (const child of element.children) isolate(child);
        } else {
          this.inertElements.push({ element, inert: element.inert });
          element.inert = true;
        }
      };
      for (const child of document.body.children) isolate(child);
      this.expandedGraph = true;
      this.$nextTick(() => { this.$refs.closeExpanded.focus({ preventScroll: true }); this.redraw(); });
    },
    restoreExpanded() {
      for (const item of this.inertElements) item.element.inert = item.inert;
      this.inertElements = [];
      if (this.bodyOverflow) {
        if (this.bodyOverflow.value) document.body.style.setProperty("overflow", this.bodyOverflow.value, this.bodyOverflow.priority);
        else document.body.style.removeProperty("overflow");
        this.bodyOverflow = null;
      }
    },
    closeExpanded() {
      this.restoreExpanded();
      this.expandedGraph = false;
      this.$nextTick(() => { this.expandTrigger?.focus({ preventScroll: true }); this.redraw(); });
    },
    expandedEscape(event) {
      if (this.expandedGraph && !this.drawerOpen && !event.defaultPrevented) { event.preventDefault(); this.closeExpanded(); }
    },
    expandedTab(event) {
      if (!this.expandedGraph) return;
      const roots = [this.$refs.surface];
      if (this.drawerOpen) roots.push(document.getElementById("workflow-drawer"));
      const selector = 'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])';
      const focusable = roots.filter(Boolean).flatMap(root => [...root.querySelectorAll(selector)])
        .filter(element => element.getClientRects().length && !element.closest("[inert]"));
      if (!focusable.length) return;
      const index = focusable.indexOf(document.activeElement);
      if (index < 0 || (!event.shiftKey && index === focusable.length - 1) || (event.shiftKey && index === 0)) {
        event.preventDefault();
        focusable[event.shiftKey ? focusable.length - 1 : 0].focus({ preventScroll: true });
      }
    },
    async refreshFlow(manual = false) {
      if (this.busy || !this.started) return;
      this.busy = true;
      this.message = "";
      this.refreshDeadline = Date.now() + 10000;
      try {
        const previouslyActive = new Set(this.nodes.filter(node => MaestroFlow.active(node.status)).map(node => node.id));
        await this.hydrateReferences(this.nodes);
        // Refresh a bounded number of already-loaded pages. A terminal root
        // never stops status updates for its still-active children.
        const pages = this.nodes.filter(node => node.loaded && node.state === "ready"
          && (manual || MaestroFlow.active(node.status) || previouslyActive.has(node.id) || node.stale))
          .flatMap(node => node.pages.map(offset => ({ node, offset, details: true })));
        const root = this.nodes.find(node => node.id === this.rootID);
        if (root && !root.detailsLoaded && (manual || MaestroFlow.active(root.status) || previouslyActive.has(root.id) || root.stale)) {
          pages.unshift({ node: root, offset: 0, details: false });
        }
        const count = Math.min(6, pages.length);
        for (let i = 0; i < count; i++) {
          if (!this.flowRoot.isConnected || this.view !== "flow" || document.hidden || Date.now() >= this.refreshDeadline) break;
          const page = pages[(this.refreshCursor + i) % pages.length];
          try { await this.readPage(page.node, page.offset, page.details); }
          catch (error) {
            page.node.pageErrors[page.details ? page.offset : "metadata"] = error.name === "AbortError" ? "The read timed out. Retry to refresh." : error.message;
            page.node.error = page.node.pageErrors[page.details ? page.offset : "metadata"];
            throw error;
          }
        }
        if (pages.length) this.refreshCursor = (this.refreshCursor + count) % pages.length;
        if (manual && pages.length > count) this.message = `${count} of ${pages.length} loaded step pages refreshed. Refresh again to continue through the remaining pages.`;
        this.lastRefresh = new Date().toISOString();
      } catch (error) {
        this.message = error.name === "AbortError" ? "Refresh timed out. Loaded records are stale." : error.message;
        for (const node of this.nodes.filter(item => item.loaded)) node.stale = true;
        this.reportedStatusStale = true;
      } finally { this.refreshDeadline = 0; this.busy = false; this.redraw(); }
    },
    redraw() {
      this.$nextTick(() => {
        if (this.drawFrame) cancelAnimationFrame(this.drawFrame);
        this.drawFrame = requestAnimationFrame(() => this.drawEdges());
      });
    },
    drawEdges() {
      const board = this.$refs.board;
      if (!board || !board.getClientRects().length || this.view !== "flow") return;
      const base = board.getBoundingClientRect();
      const elements = [...board.querySelectorAll("[data-flow-node]")];
      const boxes = new Map(elements.map(element => [element.dataset.flowNode, element]));
      const rect = element => {
        const r = element.getBoundingClientRect();
        return { left: r.left - base.left, right: r.right - base.left, top: r.top - base.top, height: r.height, y: r.top - base.top + r.height / 2 };
      };
      // Give every relationship its own port, including when its steps are
      // collapsed. Calls and returns then remain distinct at both card edges.
      const ports = new Map(), routes = [];
      const anchor = (element, side, peerY) => {
        if (!ports.has(element)) ports.set(element, { left: [], right: [] });
        const point = { side, peerY };
        ports.get(element)[side].push(point);
        return point;
      };
      for (const edge of MaestroFlow.edges(this.nodes)) {
        const owner = boxes.get(edge.owner), target = boxes.get(edge.target);
        if (!owner || !target) continue;
        const step = [...owner.querySelectorAll("[data-flow-step]")].find(element => element.dataset.flowStep === edge.step && element.getClientRects().length);
        const from = step || owner.querySelector(".flow-node-head"), to = target.querySelector(".flow-node-head");
        const a = rect(from), b = rect(to);
        const ownerNode = this.nodes.find(node => node.id === edge.owner), targetNode = this.nodes.find(node => node.id === edge.target);
        const depth = targetNode.depth - ownerNode.depth;
        const ownerPort = anchor(from, depth < 0 ? "left" : "right", b.y);
        const targetPort = anchor(to, depth > 0 ? "left" : "right", a.y);
        const record = ownerNode.steps.find(step => step.key === edge.step);
        routes.push({ ...edge, depth, ownerPort, targetPort, record, ownerNode, targetNode });
      }
      for (const [element, sides] of ports) {
        const box = rect(element);
        for (const [side, points] of Object.entries(sides)) {
          points.sort((a, b) => a.peerY - b.peerY);
          const spacing = Math.min(14, Math.max(0, box.height - 28) / Math.max(1, points.length - 1));
          points.forEach((point, index) => {
            point.x = side === "left" ? box.left - 4 : box.right + 4;
            point.y = box.y + (index - (points.length - 1) / 2) * spacing;
          });
        }
      }
      const gap = parseFloat(getComputedStyle(board.querySelector(".flow-lanes")).columnGap);
      const paths = routes.map((edge, index) => {
        const returns = edge.kind === "return";
        const source = returns ? edge.targetPort : edge.ownerPort, dest = returns ? edge.ownerPort : edge.targetPort;
        const sourceDirection = source.side === "right" ? 1 : -1, destDirection = dest.side === "right" ? 1 : -1;
        let d;
        if (Math.abs(edge.depth) === 1) {
          const bend = Math.abs(dest.x - source.x) / 2;
          d = `M ${source.x} ${source.y} C ${source.x + sourceDirection * bend} ${source.y}, ${dest.x + destDirection * bend} ${dest.y}, ${dest.x} ${dest.y}`;
        } else if (edge.depth === 0) {
          const x = Math.max(source.x, dest.x) + 20 + index % 4 * 5;
          d = MaestroFlow.roundedPath([source, { x, y: source.y }, { x, y: dest.y }, dest]);
        } else {
          // Cross-column references and cycles use the top margin and column
          // gaps; a direct curve would pass behind intermediate cards.
          const exit = source.x + sourceDirection * gap / 3, enter = dest.x + destDirection * gap / 3;
          const rail = 8 + index % 4 * 4;
          d = MaestroFlow.roundedPath([source, { x: exit, y: source.y }, { x: exit, y: rail }, { x: enter, y: rail }, { x: enter, y: dest.y }, dest]);
        }
        const relation = returns ? (edge.error ? "Error return" : "Result return") : edge.kind === "invocation" ? "Invocation" : "Reference";
        const sourceName = returns ? edge.targetNode.name : edge.ownerNode.name, destName = returns ? edge.ownerNode.name : edge.targetNode.name;
        const label = `${relation}: ${sourceName} to ${destName}. ${edge.record.id == null ? "Inspect workflow" : `Inspect step ${edge.record.id}: ${edge.record.name}`}.`;
        return { ...edge, d, label, inspectUrl: edge.record.inspectUrl || edge.ownerNode.inspectUrl,
          marker: `url(#flow-arrow-${edge.error ? "error" : returns ? "return" : edge.kind === "reference" ? "reference" : "call"})` };
      });
      // The absolute SVG's previous dimensions can inflate scrollWidth/Height
      // after a branch collapses. Measure natural lane content instead.
      const lanes = board.querySelector(".flow-lanes").getBoundingClientRect();
      const padding = getComputedStyle(board);
      this.canvasWidth = Math.ceil(Math.max(base.width, lanes.right - base.left + parseFloat(padding.paddingRight)));
      this.canvasHeight = Math.ceil(Math.max(base.height, lanes.bottom - base.top + parseFloat(padding.paddingBottom)));
      this.paths = paths;
      // Keep keyed SVG controls across redraws so polling and drawer resize
      // preserve keyboard focus. Executor text is assigned only as plain text.
      const existing = new Map([...this.$refs.connections.children].map(element => [element.dataset.edgeKey, element]));
      const svg = name => document.createElementNS("http://www.w3.org/2000/svg", name);
      for (const path of paths) {
        let element = existing.get(path.key);
        if (!element) {
          element = svg("g");
          element.setAttribute("class", "flow-edge");
          element.setAttribute("role", "button");
          element.setAttribute("tabindex", "0");
          element.setAttribute("aria-controls", "workflow-drawer");
          element.dataset.edgeKey = path.key;
          const hit = svg("path"), line = svg("path");
          hit.setAttribute("class", "flow-edge-hit");
          hit.setAttribute("aria-hidden", "true");
          line.setAttribute("aria-hidden", "true");
          element.append(svg("title"), hit, line);
          element.addEventListener("click", () => this.inspect(element));
          element.addEventListener("keydown", event => {
            if (event.key === "Enter" || event.key === " ") { event.preventDefault(); this.inspect(element); }
          });
          this.$refs.connections.append(element);
        }
        existing.delete(path.key);
        element.dataset.selectionKey = "flow-edge:" + path.key;
        element.dataset.inspectUrl = path.inspectUrl;
        element.dataset.inspectLabel = path.label;
        element.dataset.owner = path.owner;
        element.dataset.step = path.step;
        element.setAttribute("aria-label", path.label);
        element.setAttribute("aria-expanded", String(this.drawerOpen && this.selection?.key === element.dataset.selectionKey));
        element.querySelector("title").textContent = path.label;
        const [hit, line] = element.querySelectorAll("path");
        hit.setAttribute("d", path.d);
        line.setAttribute("d", path.d);
        line.setAttribute("class", `flow-connection ${path.kind}${path.error ? " error" : ""}`);
        line.setAttribute("marker-end", path.marker);
      }
      for (const element of existing.values()) element.remove();
    },
    destroy() {
      this.restoreExpanded();
      clearInterval(this.timer); this.flowController?.abort(); this.resizeObserver?.disconnect();
      if (this.drawFrame) cancelAnimationFrame(this.drawFrame);
    },
  }));
});
