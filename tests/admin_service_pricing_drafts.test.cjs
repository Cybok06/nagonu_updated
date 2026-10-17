const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const vm = require('node:vm');
const source = readFileSync(require('node:path').join(__dirname, '../static/js/admin_service_pricing_drafts.js'), 'utf8');
const key = 'admin-service-pricing:v1:admin:service';

function load(storage, saved = null) {
  const row = values => ({
    inputs: values.map(value => ({value})),
    querySelectorAll() { return this.inputs; },
  });
  const table = values => ({
    rows: [row(values)], handlers: {},
    querySelectorAll() { return this.rows; },
    replaceChildren() { this.rows = []; },
    addEventListener(event, fn) { this.handlers[event] = fn; },
    get lastElementChild() { return this.rows.at(-1); },
  });
  const tables = {default: table(['10', '12', '1GB']), store: table(['10', '11', '1GB'])};
  const status = {};
  const token = {value: ''};
  const form = {
    dataset: {pricingService: 'service'}, handlers: {},
    elements: {namedItem: () => token},
    querySelector: () => status,
    addEventListener(event, fn) { this.handlers[event] = fn; },
  };
  const context = {
    document: {
      querySelectorAll: () => [form],
      getElementById(id) {
        if (id === 'pricing-draft-config') return {textContent: JSON.stringify({userId: 'admin', saved})};
        return tables[id.includes('default') ? 'default' : 'store'];
      },
    },
    localStorage: {
      getItem: k => storage.get(k),
      setItem: (k, v) => storage.set(k, v),
      removeItem: k => storage.delete(k),
    },
    MutationObserver: class {constructor(fn) {this.fn = fn;} observe(t) {t.mutate = this.fn;}},
    addOfferRow: t => t.rows.push(row(['', '', ''])),
    queueMicrotask,
  };
  vm.runInNewContext(source, context);
  return {tables, form, token, status};
}

test('both price tabs survive switching and reload; successful save clears only its draft', () => {
  const storage = new Map();
  const ui = load(storage);
  ui.tables.store.rows[0].inputs[1].value = '15';
  ui.tables.store.handlers.input();
  ui.form.handlers['hide.bs.tab']();
  ui.tables.default.rows[0].inputs[1].value = '18';
  ui.tables.default.handlers.input();
  const restored = load(storage);
  assert.equal(restored.tables.store.rows[0].inputs[1].value, '15');
  assert.equal(restored.tables.default.rows[0].inputs[1].value, '18');
  restored.form.handlers.submit();
  assert.ok(storage.has(key), 'submission alone must not delete the draft');
  storage.set('another-service', 'untouched');
  load(storage, {service_id: 'service', token: restored.token.value});
  assert.equal(storage.has(key), false);
  assert.equal(storage.get('another-service'), 'untouched');
});

test('added and removed rows persist, and an older save receipt does not discard newer edits', () => {
  const storage = new Map();
  const ui = load(storage);
  ui.tables.store.rows.push(ui.tables.default.rows[0]);
  ui.tables.store.mutate();
  const previousToken = ui.token.value;
  ui.tables.default.rows = [];
  ui.tables.default.mutate();
  const restored = load(storage, {service_id: 'service', token: previousToken});
  assert.equal(restored.tables.store.rows.length, 2);
  assert.equal(restored.tables.default.rows.length, 0);
  assert.ok(storage.has(key));
});

test('malformed or unavailable storage does not prevent editing', () => {
  const storage = new Map([[key, '{invalid']]);
  const ui = load(storage);
  assert.equal(ui.tables.default.rows.length, 1);
  ui.tables.default.handlers.input();
  assert.doesNotThrow(() => JSON.parse(storage.get(key)));
  const unavailable = {get() {throw Error('Storage blocked');}};
  const blocked = load(unavailable);
  assert.doesNotThrow(() => blocked.tables.store.handlers.input());
  assert.match(blocked.status.textContent, /Could not save/);
});
