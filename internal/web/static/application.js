/* Independent overview reads share concrete UTC bounds and pause in hidden tabs. */
document.addEventListener('alpine:init', () => {
  Alpine.data('applicationOverview', () => ({
    overviewRoot: null, busy: {}, controllers: {}, destroyed: false, timer: null, tick: 0, visibilityHandler: null,
    init() {
      this.overviewRoot = this.$el;
      this.visibilityHandler = () => { if (!document.hidden) this.refresh(); };
      document.addEventListener('visibilitychange', this.visibilityHandler);
      this.refresh();
      this.timer = setInterval(() => { if (!document.hidden) this.refresh(false); }, 30000);
    },
    destroy() { this.destroyed = true; Object.values(this.controllers).forEach(controller => controller.abort()); clearInterval(this.timer); document.removeEventListener('visibilitychange', this.visibilityHandler); },
    refresh(manual = true) {
      if (this.destroyed || !this.overviewRoot) return;
      const range = this.overviewRoot.dataset.range;
      const days = range === '30d' ? 30 : range === '7d' ? 7 : 1;
      const end = new Date(Math.floor(Date.now() / 30000) * 30000);
      const start = new Date(end.getTime() - days * 86400000);
      const query = new URLSearchParams({range, start_time: start.toISOString(), end_time: end.toISOString()});
      const panels = ['workload'];
      if (!this.busy.activity && !this.busy.recent) panels.push('activity', 'recent');
      if (manual || ++this.tick % 2 === 0) panels.push('schedules');
      panels.forEach(panel => this.load(panel, query));
    },
    async load(panel, query) {
      if (this.busy[panel]) return;
      this.busy[panel] = true;
      let target, timeout;
      try {
        target = this.overviewRoot?.querySelector(`[data-panel="${panel}"]`);
        if (!target || this.destroyed) return;
        target.setAttribute('aria-busy', 'true');
        const controller = new AbortController();
        this.controllers[panel] = controller;
        timeout = setTimeout(() => controller.abort(), 25000);
        const response = await fetch(this.overviewRoot.dataset.overviewUrl + panel + '?' + query, {signal: controller.signal});
        if (!response.ok) throw new Error('Read failed (' + response.status + ').');
        const html = await response.text();
        if (this.destroyed) return;
        const parsed = new DOMParser().parseFromString(html, 'text/html');
        const result = parsed.querySelector('.overview-result');
        if (!result) throw new Error('Overview response unavailable.');
        const availability = this.overviewRoot.querySelector('[data-overview-availability]');
        if (availability) {
          const available = result.dataset.appAvailable === 'true';
          availability.className = available ? 'status-row' : 'empty';
          if (available) {
            const badge = document.createElement('span'), peers = document.createElement('span');
            badge.className = 'badge ok'; badge.textContent = 'Available';
            peers.className = 'subtle'; peers.textContent = result.dataset.connected + ' connected executor' + (result.dataset.connected === '1' ? '' : 's');
            availability.replaceChildren(badge, peers);
          } else availability.textContent = 'No connected executors for this application.';
        }
        const existing = target.querySelector('[data-loaded="true"]');
        if (result.dataset.error && result.dataset.loaded !== 'true' && existing) {
          this.markStale(target, result.dataset.error);
        } else { target.innerHTML = html; }
      } catch (error) { if (!this.destroyed && target) this.markStale(target, error.name === 'AbortError' ? 'Read timed out.' : error.message); }
      finally { clearTimeout(timeout); target?.removeAttribute('aria-busy'); this.busy[panel] = false; delete this.controllers[panel]; }
    },
    markStale(target, message) {
      const existing = target.querySelector('[data-loaded="true"]');
      if (existing) {
        const freshness = target.querySelector('.overview-freshness');
        if (!freshness.textContent.startsWith('Stale')) freshness.prepend('Stale · ');
        const error = target.querySelector('.overview-error');
        error.hidden = false; error.textContent = message + ' · Showing retained data.';
      } else {
        let error = target.querySelector('.overview-error');
        if (!error) { error = document.createElement('div'); error.className = 'overview-error'; target.append(error); }
        error.textContent = message;
        const loading = target.querySelector('.empty'); if (loading) loading.textContent = 'Data unavailable';
      }
    }
  }));
});
