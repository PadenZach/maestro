/* UTC picker values are local-shaped strings, interpreted explicitly as UTC. */
(() => {
  const initialized = new WeakSet();
  function initialize() {
    document.querySelectorAll('[data-workflow-filters]').forEach(form => {
      if (initialized.has(form)) return;
      initialized.add(form);
      const statuses = form.querySelector('.workflow-status-filter');
      const summary = statuses.querySelector('summary');
      function statusLabel() {
        const selected = [...statuses.querySelectorAll('input:checked')];
        const label = selected.length === 0 ? 'All statuses' : selected.length === 1 ? selected[0].value : selected.length + ' statuses';
        statuses.querySelector('[data-status-label]').textContent = label;
        summary.setAttribute('aria-label', 'Status: ' + label);
        statuses.querySelectorAll('input[name="status"]').forEach(input => input.toggleAttribute('checked', input.checked));
      }
      function syncBound(input) {
        if (!input.dataset.timeBound) return;
        const hidden = form.querySelector('#' + input.dataset.timeBound);
        const value = input.value;
        if (!value) hidden.value = '';
        else {
          const date = new Date(value + 'Z');
          if (!Number.isFinite(date.getTime())) return;
          hidden.value = date.toISOString();
        }
        // HTMX history snapshots serialize markup as well as control properties.
        hidden.setAttribute('value', hidden.value);
        input.setAttribute('value', value);
      }
      function changed(event) {
        syncBound(event.target);
        if (event.target.name === 'status') statusLabel();
        if (event.target.name === 'children') event.target.toggleAttribute('checked', event.target.checked);
      }
      // Capture runs before HTMX collects the form's hidden query values.
      form.addEventListener('input', changed, true);
      form.addEventListener('change', changed, true);
      statuses.querySelector('[data-clear-status]').addEventListener('click', () => {
        statuses.querySelectorAll('input[name="status"]').forEach(input => { input.checked = false; });
        statusLabel(); statuses.open = false; summary.focus();
        form.dispatchEvent(new Event('change', { bubbles: true }));
      });
      form.addEventListener('keydown', event => {
        if (event.key === 'Escape' && statuses.open) {
          event.preventDefault(); statuses.open = false; summary.focus();
        }
      });
      statusLabel();
    });
  }
  document.addEventListener('click', event => {
    document.querySelectorAll('.workflow-status-filter[open]').forEach(statuses => {
      if (!statuses.contains(event.target)) statuses.open = false;
    });
  });
  document.addEventListener('DOMContentLoaded', initialize);
  document.addEventListener('htmx:historyRestore', initialize);
  document.addEventListener('htmx:load', initialize);
})();
