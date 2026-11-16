"use strict";
const assert = require("node:assert/strict");
const {test} = require("node:test");
const {describe} = require("../app/static/outcome.js");

const succeeded = {terminal: true, result: "SUCCESS"};
const failed = {terminal: true, result: "FAILED"};
const pending = {terminal: false, result: null};
const evidence = (verdict = "PASS", usable = true, bills = [{verdict: "PASS"}]) => ({
  execution_id: "200:456:0", summary: {verdict, usable}, bills,
});

test("successful pipeline with trusted evidence reports a billing pass", () => {
  const result = describe(succeeded, evidence(), "ready");
  assert.equal(result.title, "All billing checks passed");
  assert.equal(result.tone, "positive");
  assert.equal(result.verdict, "PASS");
  assert.equal(result.canReview, true);
  assert.equal(result.filter, "all");
});

test("billing failure is separate from a successful pipeline and opens failed bills", () => {
  const result = describe(succeeded, evidence("FAIL", true, [{verdict: "PASS"}, {verdict: "FAIL"}]), "ready");
  assert.equal(result.title, "Billing differences found");
  assert.equal(result.verdict, "FAIL");
  assert.equal(result.reviewLabel, "Review failed bills");
  assert.equal(result.filter, "FAIL");
  assert.equal(result.canReview, true);
});

test("a failed run remains visibly failed even when its saved bills passed", () => {
  const result = describe(failed, evidence(), "ready");
  assert.equal(result.title, "Simulation did not finish");
  assert.equal(result.tone, "negative");
  assert.match(result.description, /saved bills passed.*pipeline did not finish successfully/);
  assert.equal(result.verdict, "PASS");
  assert.equal(result.canReview, true);
});

test("canceled, skipped and internal-error runs never get a success headline", () => {
  for (const resultCode of ["CANCELED", "SKIPPED", "INTERNAL_ERROR", "TIMEDOUT", "UPSTREAM_FAILED"]) {
    const result = describe({terminal: true, result: resultCode}, evidence(), "ready");
    assert.equal(result.title, "Simulation did not finish", resultCode);
    assert.equal(result.tone, "negative", resultCode);
  }
});

test("terminal run with no result code warns even if saved evidence says pass", () => {
  for (const resultCode of [null, undefined, ""]) {
    const result = describe({terminal: true, result: resultCode}, evidence(), "ready");
    assert.equal(result.title, "Run status needs attention");
    assert.equal(result.tone, "warning");
    assert.match(result.description, /did not report a clear completion status/);
  }
});

test("success without complete usable evidence is blocked", () => {
  for (const value of [null, {}, {summary: null}, evidence("PASS", false), evidence("PASS", "true"), evidence("PASS", 1), evidence("BLOCKED"), evidence("unexpected")]) {
    const result = describe(succeeded, value, "ready");
    assert.equal(result.title, "Results need attention");
    assert.equal(result.tone, "warning");
    assert.equal(result.verdict, "BLOCKED");
    assert.equal(result.canReview, false);
    assert.equal(result.filter, "all");
  }
});

test("failed run with missing results reports the operational failure", () => {
  const result = describe(failed, null, "ready");
  assert.equal(result.title, "Simulation did not finish");
  assert.equal(result.verdict, "BLOCKED");
  assert.equal(result.canReview, false);
  assert.match(result.description, /No trusted billing result/);
});

test("pending, loading and retry states do not retain a previous financial result", () => {
  for (const phase of ["loading_run", "running", "loading_results", "error", "unexpected"]) {
    const result = describe(succeeded, evidence(), phase);
    assert.equal(result.verdict, null, phase);
    assert.equal(result.canReview, false, phase);
    assert.equal(result.filter, "all", phase);
    assert.notEqual(result.tone, "positive", phase);
  }
});

test("a running job cannot show completed billing evidence even in ready phase", () => {
  const result = describe(pending, evidence(), "ready");
  assert.equal(result.title, "Simulation in progress");
  assert.equal(result.verdict, null);
  assert.equal(result.canReview, false);
});

test("reading or failing to read results does not hide an already failed run", () => {
  for (const phase of ["loading_results", "error"]) {
    const result = describe(failed, evidence(), phase);
    assert.equal(result.title, "Simulation did not finish");
    assert.equal(result.tone, "negative");
    assert.equal(result.verdict, null);
    assert.equal(result.canReview, false);
  }
});

test("review failed bills requires actual failed bill rows", () => {
  const result = describe(succeeded, evidence("FAIL", true, [{verdict: "BLOCKED"}]), "ready");
  assert.equal(result.reviewLabel, "View bills");
  assert.equal(result.filter, "all");
  assert.equal(result.canReview, true);
});

test("missing or empty bills do not expose a useless review action", () => {
  for (const bills of [undefined, null, [], "not an array"]) {
    const result = describe(succeeded, {summary: {verdict: "PASS", usable: true}, bills}, "ready");
    assert.equal(result.canReview, false);
    assert.equal(result.reviewLabel, "View bills");
    assert.equal(result.filter, "all");
  }
});

test("untrusted failed rows do not become reviewable", () => {
  const result = describe(succeeded, evidence("FAIL", false, [{verdict: "FAIL"}]), "ready");
  assert.equal(result.canReview, false);
  assert.equal(result.reviewLabel, "View bills");
  assert.equal(result.filter, "all");
});

test("presentation is pure and does not mutate API data", () => {
  const run = Object.freeze({...succeeded});
  const result = Object.freeze({summary: Object.freeze({verdict: "FAIL", usable: true}), bills: Object.freeze([Object.freeze({verdict: "FAIL"})])});
  assert.deepEqual(describe(run, result, "ready"), describe(run, result, "ready"));
});


test("browser build exposes the same helper without a module loader", () => {
  const {readFileSync} = require("node:fs");
  const {runInNewContext} = require("node:vm");
  const context = {};
  runInNewContext(readFileSync(require.resolve("../app/static/outcome.js"), "utf8"), context);
  assert.equal(typeof context.ChargeAssertOutcome.describe, "function");
  assert.equal(context.ChargeAssertOutcome.describe(succeeded, evidence(), "ready").verdict, "PASS");
});
