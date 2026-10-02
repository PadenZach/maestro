"use strict";

document.addEventListener("htmx:beforeSwap", event => {
  if (event.detail.target?.id === "wf-rows" && event.detail.xhr.status === 400) {
    event.detail.shouldSwap = true;
    event.detail.isError = false;
  }
});

document.addEventListener("alpine:init", () => {
  Alpine.data("versionControl", () => ({
    versionRoot: null,
    visible: false,
    copyFailed: false,
    feedback: "",
    copyRevision: 0,
    position: { left: "0px", top: "0px" },
    init() { this.versionRoot = this.$el; },
    show() {
      if (!this.versionRoot.isConnected) return;
      this.visible = true;
      // x-show reveals on the next frame; measure after it has a width.
      this.$nextTick(() => requestAnimationFrame(() => this.place()));
    },
    place() {
      if (!this.visible || !this.versionRoot.isConnected) return;
      const anchor = this.$refs.copy.getBoundingClientRect();
      const tip = this.$refs.tooltip.getBoundingClientRect();
      const left = Math.max(12, Math.min(anchor.left, innerWidth - tip.width - 12));
      const below = anchor.bottom + 6;
      const top = below + tip.height < innerHeight - 12 ? below : Math.max(12, anchor.top - tip.height - 6);
      this.position = { left: `${left}px`, top: `${top}px` };
    },
    leave() {
      if (!this.copyFailed && !this.versionRoot.contains(document.activeElement) && !this.versionRoot.matches(":hover")) {
        this.visible = false;
      }
    },
    dismiss(event) {
      if (!this.visible) return;
      event.preventDefault();
      ++this.copyRevision;
      this.visible = false;
      this.copyFailed = false;
    },
    async copy() {
      const revision = ++this.copyRevision;
      let failed = false;
      try {
        if (!navigator.clipboard?.writeText) throw new Error("Clipboard unavailable");
        await navigator.clipboard.writeText(this.versionRoot.dataset.version);
      } catch {
        failed = true;
      }
      if (revision !== this.copyRevision || !this.versionRoot.isConnected) return;
      this.copyFailed = failed;
      this.feedback = failed ? "Could not copy. Select the full version below and copy it manually." : "Application version copied.";
      this.show();
    },
    destroy() { ++this.copyRevision; },
  }));

  // The owner stays outside the live fragment. Only identities and UI state are
  // retained across swaps; payloads are never stored in browser persistence.
  Alpine.data("workflowInspector", () => ({
    inspectorRoot: null,
    expanded: {},
    drawerOpen: false,
    selection: null,
    loading: false,
    drawerHTML: "",
    drawerError: "",
    revision: 0,
    controller: null,
    init() { this.inspectorRoot = this.$el; },
    inspect(source) {
      this.selection = { key: source.dataset.selectionKey, url: source.dataset.inspectUrl, label: source.dataset.inspectLabel };
      this.drawerHTML = "";
      this.drawerError = "";
      this.drawerOpen = true;
      this.$nextTick(() => {
        if (this.drawerOpen && this.inspectorRoot.isConnected) {
          this.inspectorRoot.querySelector(".drawer-close").focus({ preventScroll: true });
        }
      });
      this.loadInspection();
    },
    async loadInspection() {
      if (!this.drawerOpen || !this.selection) return;
      this.controller?.abort();
      const controller = new AbortController();
      this.controller = controller;
      const revision = ++this.revision;
      const url = this.selection.url;
      this.loading = true;
      this.drawerError = "";
      try {
        const response = await fetch(url, { signal: controller.signal, headers: { "HX-Request": "true" } });
        if (!response.ok) throw new Error("Inspection read failed");
        const html = await response.text();
        if (revision === this.revision && this.drawerOpen) this.drawerHTML = html;
      } catch (error) {
        if (error.name !== "AbortError" && revision === this.revision && this.drawerOpen) {
          this.drawerHTML = "";
          this.drawerError = "Could not load details. Try again.";
        }
      } finally {
        if (revision === this.revision) this.loading = false;
      }
    },
    refresh(event) {
      // A slow read must be allowed to finish even when the timeline polls.
      if (event.detail.target.id === "wf-live" && this.drawerOpen && !this.loading) this.loadInspection();
    },
    close() {
      this.controller?.abort();
      ++this.revision;
      const key = this.selection?.key;
      this.drawerOpen = false;
      this.selection = null;
      this.drawerHTML = "";
      this.drawerError = "";
      this.loading = false;
      this.$nextTick(() => {
        const trigger = [...this.inspectorRoot.querySelectorAll("[data-selection-key]")]
          .find(node => node.dataset.selectionKey === key && node.getClientRects().length);
        (trigger || this.inspectorRoot).focus({ preventScroll: true });
      });
    },
    escape(event) {
      // Tooltip listeners dismiss first; the next Escape closes the drawer.
      if (document.querySelector('[role="tooltip"][data-open="true"]')) return;
      if (this.drawerOpen && !event.defaultPrevented) {
        event.preventDefault();
        this.close();
      }
    },
    destroy() { this.controller?.abort(); },
  }));

  // Nested under workflowInspector, so every branch uses its ancestry key in
  // that persistent owner's expanded map. Alpine initializes swapped branches.
  Alpine.data("workflowBranch", () => ({
    branchRoot: null,
    branchHTML: "",
    branchLoading: false,
    branchError: "",
    branchController: null,
    init() {
      this.branchRoot = this.$el;
      // Parent x-html initializes this component inside its render effect.
      // Defer state reads so nested folding cannot retrigger that render.
      this.$nextTick(() => {
        if (this.branchRoot.isConnected && this.branchOpen) this.loadBranch();
      });
    },
    get branchOpen() { return this.expanded[this.branchRoot.dataset.branchKey] || false; },
    toggle() {
      if (!this.branchRoot.isConnected) return;
      this.expanded[this.branchRoot.dataset.branchKey] = !this.branchOpen;
      if (this.branchOpen) this.loadBranch();
    },
    async loadBranch() {
      if (this.branchLoading) return;
      const branchController = new AbortController();
      this.branchController = branchController;
      this.branchLoading = true;
      this.branchError = "";
      try {
        const response = await fetch(this.branchRoot.dataset.childUrl, {
          signal: branchController.signal, headers: { "HX-Request": "true" },
        });
        if (!response.ok) throw new Error("Child read failed");
        const branchHTML = await response.text();
        if (this.branchRoot.isConnected) this.branchHTML = branchHTML;
      } catch (branchError) {
        if (branchError.name !== "AbortError" && this.branchRoot.isConnected) {
          this.branchError = "Child workflow unavailable. Collapse and expand to retry.";
        }
      } finally {
        this.branchLoading = false;
      }
    },
    destroy() { this.branchController?.abort(); },
  }));
});
