const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const html = fs.readFileSync(path.join(__dirname, '../templates/store_page.html'), 'utf8');
const start = html.indexOf('    async function verifyExistingOrderPhone(');
const end = html.indexOf('\n    }', start) + 6;
const source = html.slice(start, end);
function setup(enabled, fetch) {
  let shown = 0, hidden = 0, timer;
  const context = {
    normalizePhone: x => x, needsExistingOrderVerification: () => enabled,
    verificationNotNeeded: () => true, setPhoneVerificationState: () => {},
    phoneVerifyCache: {}, fetch, AbortController,
    showPhoneVerifyLoader: () => shown++, hidePhoneVerifyLoader: () => hidden++,
    setTimeout: fn => {timer = fn; return 1;}, clearTimeout: () => {},
    showDeliveryDelayWarning: async () => {assert.equal(hidden, 1); return true;},
  };
  vm.createContext(context);
  vm.runInContext(source, context);
  return {run: () => context.verifyExistingOrderPhone('0241234567', {}, {}, {}, {}, {showWarningModal: true}),
          expire: () => timer(), counts: () => [shown, hidden]};
}
test('disabled verification sends no request or loader', async () => {
  const ui = setup(false, () => {throw Error('must not fetch');});
  assert.equal(await ui.run(), true);
  assert.deepEqual(ui.counts(), [0, 0]);
});
test('timeout releases the loader and allows a retry', async () => {
  const ui = setup(true, (_, options) => new Promise((resolve, reject) => {
    options.signal.addEventListener('abort', () => reject(Error('timeout')));
  }));
  const result = ui.run(); ui.expire();
  assert.equal(await result, false);
  assert.deepEqual(ui.counts(), [1, 1]);
  const retry = ui.run(); ui.expire();
  assert.equal(await retry, false);
  assert.deepEqual(ui.counts(), [2, 2]);
});
test('loader closes before delivery warning is awaited', async () => {
  const ui = setup(true, async () => ({ok: true, json: async () => ({allow_order: true, warning: true})}));
  assert.equal(await ui.run(), true);
  assert.deepEqual(ui.counts(), [1, 1]);
});
