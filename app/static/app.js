"use strict";
(() => {
  const $ = (id) => document.getElementById(id);
  const state = {config: null, selected: null, version: 0, timer: null, run: null, bills: [], shown: 50, launch: null, starting: false, fetchingResults: false};
  const number = new Intl.NumberFormat();
  const setText = (id, value) => { $(id).textContent = value == null ? "—" : String(value); };
  const showMessage = (id, message, kind = "") => { const node = $(id); node.textContent = message || ""; node.className = `notice ${kind}`; node.hidden = !message; };
  const safeUrl = (value) => { try { const url = new URL(value); return url.protocol === "https:" ? url.href : ""; } catch (_) { return ""; } };
  function setLink(id, value) { const url = safeUrl(value); $(id).hidden = !url; if (url) $(id).href = url; else $(id).removeAttribute("href"); }
  function badge(node, value) {
    const text = value || "—";
    const good = ["PASS", "SUCCEEDED", "SUCCESS"].includes(text);
    const bad = ["FAIL", "FAILED", "TIMEDOUT", "CANCELED", "INTERNAL_ERROR"].includes(text);
    node.className = `badge ${good ? "positive" : bad ? "negative" : text === "BLOCKED" ? "warning" : "neutral"}`;
    node.textContent = text;
  }
  async function api(path, options = {}) {
    let response;
    try { response = await fetch(path, {credentials: "same-origin", ...options}); }
    catch (_) { throw Object.assign(new Error("The app could not reach the server. Check your connection and try again."), {uncertain: true}); }
    let data;
    try { data = await response.json(); }
    catch (_) { throw Object.assign(new Error("The server returned an unreadable response. Please try again."), {status: response.status, uncertain: options.method === "POST" || response.status >= 500}); }
    if (!data || typeof data !== "object" || Array.isArray(data)) throw Object.assign(new Error("The server returned an unexpected response. Please try again."), {uncertain: options.method === "POST"});
    if (!response.ok) throw Object.assign(new Error(data.error || "The request could not be completed. Please try again."), {status: response.status, uncertain: response.status >= 500});
    return data;
  }
  const modeName = (mode) => mode === "healthy" ? "healthy bills" : mode === "amount_error" ? "overcharge test" : (mode || "simulation");
  function runLabel(run) {
    const date = run.started_at ? new Date(run.started_at) : null;
    const stamp = date && !Number.isNaN(date.valueOf()) ? date.toLocaleString(undefined, {month: "short", day: "numeric", hour: "2-digit", minute: "2-digit"}) : `Run ${run.run_id}`;
    const params = run.parameters || {};
    return `${stamp} · ${params.session_count || "?"} sessions · ${modeName(params.candidate_mode)} · ${run.result || run.state || "starting"}`;
  }
  async function loadHistory() {
    const {runs} = await api("/api/runs");
    const select = $("run-history");
    select.replaceChildren(new Option("Choose a run", ""));
    (runs || []).slice(0, 20).forEach((run) => select.add(new Option(runLabel(run), String(run.run_id))));
    if (state.selected && !Array.from(select.options).some((option) => option.value === String(state.selected))) select.add(new Option(`Selected run ${state.selected}`, String(state.selected)));
    select.value = state.selected || "";
    select.disabled = false;
  }
  function renderRun(run) {
    state.run = run;
    setText("run-id", `Pipeline run ${run.run_id}`);
    const p = run.parameters || {};
    setText("run-description", `${number.format(Number(p.session_count || 0))} sessions · ${modeName(p.candidate_mode)} · Seed ${p.seed ?? "—"}`);
    badge($("pipeline-status"), run.result === "SUCCESS" ? "SUCCEEDED" : (run.result || run.state || "STARTING"));
    setLink("job-link", run.run_page_url);
    $("steps").replaceChildren();
    const fallback = [{key: "generate_sessions", label: "Generate sessions"}, {key: "ingest_ocpp_files", label: "Load events"}, {key: "evaluate_generated_batch", label: "Check bills"}];
    (run.steps && run.steps.length ? run.steps : fallback).forEach((step) => {
      const node = document.createElement("div");
      const current = step.result || step.state || "PENDING";
      const success = ["SUCCESS", "SUCCEEDED"].includes(current);
      const failed = ["FAILED", "UPSTREAM_FAILED", "TIMEDOUT", "CANCELED", "INTERNAL_ERROR"].includes(current);
      node.className = `step ${success ? "success" : failed ? "failed" : current === "RUNNING" ? "active" : ""}`;
      const label = document.createElement("strong"); label.textContent = step.label;
      const detail = document.createElement("span"); detail.className = "step-state"; detail.textContent = current.toLowerCase().replaceAll("_", " ");
      node.append(label, detail); $("steps").append(node);
    });
    if (!run.terminal) showMessage("result-message", run.message || "Your simulation is running. Billing results appear after this run finishes.");
  }
  function clearResults() {
    state.bills = []; state.shown = 50; state.fetchingResults = false;
    ["sessions", "passed", "failed", "blocked"].forEach((key) => setText(`metric-${key}`, null));
    ["baseline", "candidate", "overall"].forEach((key) => badge($(`${key}-verdict`), null));
    setText("decision-explanation", ""); setText("evidence-id", "");
    $("bills-body").replaceChildren(); $("billing-results").hidden = true;
    $("metric-failed").classList.remove("has-failures");
    $("role-filter").value = "all"; $("verdict-filter").value = "all";
  }
  async function selectRun(id) {
    clearTimeout(state.timer);
    state.version += 1; const version = state.version;
    state.selected = id ? String(id) : null; state.run = null;
    clearResults(); showMessage("result-message", "");
    $("empty-state").hidden = !!id; $("selected-run").hidden = !id;
    $("run-history").value = state.selected || "";
    if (!id) return;
    setText("run-description", "Loading the selected run…"); setText("run-id", `Pipeline run ${id}`);
    badge($("pipeline-status"), "LOADING"); $("steps").replaceChildren(); setLink("job-link", "");
    await pollRun(version);
  }
  async function pollRun(version) {
    if (version !== state.version || !state.selected) return;
    try {
      const run = await api(`/api/runs/${state.selected}`);
      if (version !== state.version) return;
      renderRun(run);
      if (run.terminal) {
        await loadResults(version);
        if (version === state.version) loadHistory().catch(() => {});
      } else state.timer = setTimeout(() => pollRun(version), 5000);
    } catch (error) {
      if (version !== state.version) return;
      const retry = !error.status || error.status >= 500 || error.status === 429;
      showMessage("result-message", error.message + (retry ? " Checking again shortly." : " Choose another run or reload this page."), "error");
      if (retry) state.timer = setTimeout(() => pollRun(version), 5000);
      else badge($("pipeline-status"), "UNAVAILABLE");
    }
  }
  async function loadResults(version = state.version) {
    if (version !== state.version || !state.run?.terminal || state.fetchingResults) return;
    state.fetchingResults = true;
    state.bills = [];
    ["sessions", "passed", "failed", "blocked"].forEach((key) => setText(`metric-${key}`, null));
    ["baseline", "candidate", "overall"].forEach((key) => badge($(`${key}-verdict`), null));
    $("metric-failed").classList.remove("has-failures");
    setText("decision-explanation", ""); setText("evidence-id", "");
    renderBills();
    $("billing-results").hidden = false;
    $("refresh-results").disabled = true;
    showMessage("result-message", "Reading saved results for this run…");
    try {
      const result = await api(`/api/runs/${state.selected}/results`);
      if (version !== state.version) return;
      const summary = result.summary;
      state.bills = result.bills || [];
      setText("evidence-id", result.execution_id ? `Evaluation ${result.execution_id} · Results are tied to this exact run.` : "No saved evaluation is available for this run.");
      if (summary) {
        const fields = {sessions: "session_count", passed: "passed_checks", failed: "failed_checks", blocked: "blocked_checks"};
        Object.entries(fields).forEach(([key, field]) => setText(`metric-${key}`, summary[field] == null ? null : number.format(Number(summary[field]))));
        badge($("baseline-verdict"), summary.baseline_verdict);
        badge($("candidate-verdict"), summary.candidate_verdict);
        badge($("overall-verdict"), summary.verdict);
        $("metric-failed").classList.toggle("has-failures", Number(summary.failed_checks) > 0);
        setText("decision-explanation", summary.usable ? (summary.verdict === "PASS" ? "Both billing versions passed every required check." : "Billing differences found. Review the failed bills before release.") : "Billing results are blocked because the saved evidence is incomplete or inconsistent.");
      } else {
        ["baseline", "candidate", "overall"].forEach((key) => badge($(`${key}-verdict`), "BLOCKED"));
        setText("decision-explanation", "There is no trusted billing result for this run yet.");
      }
      const parentFailed = state.run.result && !["SUCCESS", "SUCCEEDED"].includes(state.run.result);
      const message = parentFailed ? `The pipeline did not succeed (${state.run.result}). ${result.message || "Open the job run to see what happened."}` : result.message;
      showMessage("result-message", message || (summary?.usable ? "" : "No trusted billing result is available. Check the job run or refresh the saved results."), parentFailed ? "error" : summary?.usable ? "" : "warning");
      renderBills();
    } catch (error) {
      if (version === state.version) showMessage("result-message", error.message + " Use Refresh results to try again.", "error");
    } finally {
      if (version === state.version) { state.fetchingResults = false; $("refresh-results").disabled = false; }
    }
  }
  function amount(value, currency, difference = false) {
    if (value == null || value === "") return "—";
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return String(value);
    const formatted = new Intl.NumberFormat(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}).format(numeric);
    return `${difference && numeric > 0 ? "+" : ""}${formatted}${difference ? "" : ` ${currency || ""}`}`.trim();
  }
  function renderBills() {
    const role = $("role-filter").value; const verdict = $("verdict-filter").value;
    const rows = state.bills.filter((row) => (role === "all" || row.release_role === role) && (verdict === "all" || row.verdict === verdict));
    const body = $("bills-body"); body.replaceChildren();
    rows.slice(0, state.shown).forEach((row) => {
      const tr = document.createElement("tr");
      const values = [row.session_id, row.release_role === "candidate" ? "Candidate" : "Baseline", amount(row.expected_amount, row.currency), amount(row.actual_amount, row.currency), amount(row.difference, row.currency, true)];
      values.forEach((value, index) => { const td = document.createElement("td"); td.textContent = value || "—"; if (index === 0) td.title = String(value || ""); if (index >= 2) td.className = "number"; if (index === 4 && Number(row.difference) !== 0 && row.difference != null) td.classList.add("difference-positive"); tr.append(td); });
      const td = document.createElement("td"); const status = document.createElement("span"); badge(status, row.verdict); td.append(status); tr.append(td); body.append(tr);
    });
    setText("bill-count", `${number.format(rows.length)} bills${rows.length > state.shown ? ` · Showing ${state.shown}` : ""}`);
    $("bills-empty").hidden = rows.length > 0; $("show-more").hidden = rows.length <= state.shown;
  }
  async function startRun(event) {
    event.preventDefault(); if (state.starting || !state.config) return;
    if (!state.launch) {
      if (!$("run-form").reportValidity()) return;
      const payload = {candidate_mode: $("candidate-mode").value, session_count: Number($("session-count").value), seed: $("seed").value === "" ? 42 : Number($("seed").value), idempotency_token: crypto.randomUUID()};
      state.launch = payload;
    }
    state.starting = true; $("run-button").disabled = true; $("settings").disabled = true;
    $("run-button").textContent = "Starting simulation…"; showMessage("page-message", "");
    try {
      const run = await api("/api/runs", {method: "POST", headers: {"Content-Type": "application/json", "X-CSRF-Token": state.config.csrf_token}, body: JSON.stringify(state.launch)});
      const validRunId = typeof run.run_id === "string" ? /^[1-9][0-9]*$/.test(run.run_id) : Number.isSafeInteger(run.run_id) && run.run_id > 0;
      if (!validRunId) throw Object.assign(new Error("The server did not confirm a valid run ID."), {uncertain: true});
      state.launch = null;
      await selectRun(run.run_id);
      loadHistory().catch(() => {});
    } catch (error) {
      if (!error.uncertain) state.launch = null;
      showMessage("page-message", error.message + (state.launch ? " The start outcome is unknown. Retry with the same settings and request ID to avoid creating a second run." : ""), "error");
    } finally {
      state.starting = false; $("settings").disabled = !!state.launch; $("run-button").disabled = false;
      $("run-button").textContent = state.launch ? "Retry start request" : "Run simulation";
    }
  }
  $("run-form").addEventListener("submit", startRun);
  $("run-history").addEventListener("change", (event) => selectRun(event.target.value));
  $("refresh-results").addEventListener("click", () => loadResults());
  ["role-filter", "verdict-filter"].forEach((id) => $(id).addEventListener("change", () => { state.shown = 50; renderBills(); }));
  $("show-more").addEventListener("click", () => { state.shown += 50; renderBills(); });
  function updateModeHelp() {
    setText("mode-help", $("candidate-mode").value === "healthy" ? "Both versions should match the expected charges." : "Adds €0.87 to each candidate bill.");
  }
  $("candidate-mode").addEventListener("change", updateModeHelp);
  $("seed").addEventListener("invalid", () => { $("seed").closest("details").open = true; });
  async function initialize() {
    try {
      state.config = await api("/api/config");
      $("preview-banner").hidden = !state.config.preview;
      const defaults = state.config.defaults;
      $("session-count").value = defaults.session_count; $("seed").value = defaults.seed; $("candidate-mode").value = defaults.candidate_mode;
      updateModeHelp();
      $("session-count").max = state.config.limits.session_count; $("seed").max = state.config.limits.seed;
      $("settings").disabled = false; $("run-button").disabled = false; setLink("dashboard-link", state.config.dashboard_url);
      await loadHistory();
    } catch (error) { showMessage("page-message", error.message, "error"); }
  }
  initialize();
})();
