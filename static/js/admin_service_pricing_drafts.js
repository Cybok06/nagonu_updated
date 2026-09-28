(() => {
  'use strict';
  const config = JSON.parse(document.getElementById('pricing-draft-config').textContent);
  document.querySelectorAll('form[data-pricing-service]').forEach(form => {
    const sid = form.dataset.pricingService;
    const key = `admin-service-pricing:v1:${config.userId}:${sid}`;
    const status = form.querySelector('[data-pricing-draft-status]');
    const tokenInput = form.elements.namedItem('pricing_draft_token');
    const tables = {
      default: document.getElementById(`offers_default_${sid}`),
      store: document.getElementById(`offers_store_${sid}`),
    };
    let dirty = false;
    const readRows = table => Array.from(table.querySelectorAll('.offer-row'), row =>
      Array.from(row.querySelectorAll('input'), input => input.value));
    const validRows = rows => Array.isArray(rows) && rows.every(row =>
      Array.isArray(row) && row.length === 3 && row.every(value => typeof value === 'string'));
    try {
      const draft = JSON.parse(localStorage.getItem(key) || 'null');
      if (draft && config.saved?.service_id === sid && config.saved.token === draft.token) {
        localStorage.removeItem(key);
      } else if (draft && validRows(draft.default) && validRows(draft.store)) {
        Object.entries(tables).forEach(([kind, table]) => {
          table.replaceChildren();
          draft[kind].forEach(values => {
            addOfferRow(table, kind === 'store' ? 'store_offers' : 'offers');
            table.lastElementChild.querySelectorAll('input').forEach((input, i) => {
              input.value = values[i];
            });
          });
        });
        tokenInput.value = draft.token;
        dirty = true;
        status.textContent = 'Unsaved price edits restored. Press Update Service to apply them.';
      }
    } catch (error) {
      status.textContent = 'Browser draft storage is unavailable. Keep this page open until you update the service.';
    }

    function saveDraft() {
      if (!dirty) return;
      const token = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
      tokenInput.value = token;
      try {
        localStorage.setItem(key, JSON.stringify({
          token, default: readRows(tables.default), store: readRows(tables.store),
        }));
        status.textContent = 'Price edits saved in this browser. Press Update Service to apply them.';
      } catch (error) {
        status.textContent = 'Could not save the browser draft. Keep this page open until you update the service.';
      }
    }
    function changed() {
      dirty = true;
      saveDraft();
    }
    Object.values(tables).forEach(table => {
      table.addEventListener('input', changed);
      table.addEventListener('change', changed);
      // Includes Add, Remove, Paste CSV and Copy Pricing actions.
      new MutationObserver(changed).observe(table, {childList: true, subtree: true});
    });
    // Removing the only row clears its input values without changing the DOM.
    form.addEventListener('click', event => {
      if (event.target.closest('.remove-offer-btn')) queueMicrotask(changed);
    });
    form.addEventListener('hide.bs.tab', saveDraft);
    form.addEventListener('submit', saveDraft);
  });
})();
