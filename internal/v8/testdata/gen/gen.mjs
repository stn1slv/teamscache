// Writes V8-serialized test values to internal/v8/testdata/corpus/*.bin.
// Run: node internal/v8/testdata/gen/gen.mjs
// Then make the golden files with golden.py (see README in this folder).
//
// Values avoid two known ccl_chromium_reader quirks, which have their own Go
// tests: negative Int32 values and non-ASCII one-byte (Latin-1) strings.
import { serialize } from "node:v8";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const out = join(dirname(fileURLToPath(import.meta.url)), "..", "corpus");
mkdirSync(out, { recursive: true });

// Small deterministic PRNG (mulberry32).
let seed = 20261120;
function rand() {
  seed |= 0;
  seed = (seed + 0x6d2b79f5) | 0;
  let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
  t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}
const pick = (xs) => xs[Math.floor(rand() * xs.length)];

const words = ["", "a", "hello", "Teams", "<p>Hi&amp;bye</p>", "ünïcödé ✓", "emoji 😀👍", "日本語", "x".repeat(300), "two\nlines"];

function randomValue(depth) {
  const leaf = [
    () => null,
    () => undefined,
    () => rand() < 0.5,
    () => Math.floor(rand() * 1000), // Int32 (Smi)
    () => 2 ** 31 + Math.floor(rand() * 1000), // Double holding an integer
    () => 1768557031836 + Math.floor(rand() * 1e6), // epoch milliseconds
    () => rand() * 1e6 - 5e5, // Double, may be negative
    () => pick(words),
    () => new Date(1700000000000 + Math.floor(rand() * 1e10)),
  ];
  if (depth <= 0 || rand() < 0.3) return pick(leaf)();
  const kind = Math.floor(rand() * 4);
  const n = Math.floor(rand() * 5);
  if (kind === 0) {
    const o = {};
    for (let i = 0; i < n; i++) o[pick(words.filter((w) => w.length < 50)) + i] = randomValue(depth - 1);
    return o;
  }
  if (kind === 1) return Array.from({ length: n }, () => randomValue(depth - 1));
  if (kind === 2) {
    const m = new Map();
    for (let i = 0; i < n; i++) m.set(pick([i, `k${i}`]), randomValue(depth - 1));
    return m;
  }
  return { messageMap: Object.fromEntries(Array.from({ length: n }, (_, i) => [`17685570${i}`, randomValue(depth - 1)])) };
}

const shared = { id: "shared" };
const sparse = [];
sparse[7] = "seven";
sparse.length = 12;
const holes = [1, , 3]; // eslint-disable-line no-sparse-arrays
const withProps = [1, 2];
withProps.extra = "prop";

const named = {
  "replychain": {
    conversationId: "19:abc@thread.v2",
    replyChainId: "1768557031836",
    messageMap: {
      "1768557031836": {
        id: "1768557031836",
        content: "<div>Hello &amp; welcome</div>",
        imDisplayName: "Ann Lee (Org)",
        originalArrivalTime: 1768557031836,
        messageType: "RichText/Html",
      },
    },
  },
  "integer-keys": { 1: "a", 2: "b", x: "c" },
  "shared-reference": { a: shared, b: shared, list: [shared, shared] },
  "sparse-array": sparse,
  "array-with-holes": holes,
  "array-with-props": withProps,
  "boxed": [new Number(3.5), new String("boxed"), new Boolean(false), Object(10n)],
  "bigint": [2n ** 70n, -(2n ** 70n), 0n, -5n],
  "regexp": /a+b/gi,
  "set": new Set(["only"]),
  "nested-map": new Map([["inner", new Map([[1, "one"]])]]),
  "array-buffer": new Uint8Array([1, 2, 3, 250]).buffer,
  "long-string": "y".repeat(70000),
  "two-byte-long": "✓".repeat(200),
  "deep": JSON.parse("[".repeat(200) + "]".repeat(200)),
  "special-doubles": [0.1, -0.0, 1e21, 1e-7, Number.MAX_VALUE, Infinity, -Infinity, NaN],
  "top-level-string": "just a string",
  "top-level-int": 42,
};

for (const [name, value] of Object.entries(named)) {
  writeFileSync(join(out, `${name}.bin`), serialize(value));
}
for (let i = 0; i < 200; i++) {
  writeFileSync(join(out, `random-${String(i).padStart(3, "0")}.bin`), serialize(randomValue(5)));
}
