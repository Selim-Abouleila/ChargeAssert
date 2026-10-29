"use strict";

// One presentation decision shared by the browser and the local outcome tests.
(function (root, factory) {
  const outcome = factory();
  if (typeof module === "object" && module.exports) module.exports = outcome;
  else root.ChargeAssertOutcome = outcome;
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  const successful = new Set(["SUCCESS", "SUCCEEDED"]);
  const financial = new Set(["PASS", "FAIL"]);

  function describe(run, result, phase) {
    const presentation = {
      tone: "neutral", title: "Loading simulation", description: "Checking the status of this run.",
      verdict: null, reviewLabel: "View bills", filter: "all", canReview: false,
    };
    const terminal = run?.terminal === true;
    const failed = terminal && Boolean(run.result) && !successful.has(run.result);
    const unknown = terminal && !run.result;

    function operationalProblem(description) {
      return {...presentation,
        tone: failed ? "negative" : "warning",
        title: failed ? "Simulation did not finish" : "Run status needs attention",
        description,
      };
    }

    if (phase === "loading_run") return presentation;
    if (phase === "error") {
      if (failed || unknown) return operationalProblem(
        "The run did not report a successful finish, and its saved results could not be read. Try again or open run details.");
      return {...presentation, tone: "warning",
        title: terminal ? "Could not load results" : "Could not load simulation",
        description: "Try again to check this run. No billing result is shown until it can be verified.",
      };
    }
    if (phase === "running" || !terminal) {
      if (failed || unknown) return operationalProblem("Open run details to see what happened.");
      return {...presentation, title: "Simulation in progress",
        description: "Creating sessions, loading their data and checking the bills. This page updates automatically.",
      };
    }
    if (phase === "loading_results") {
      if (failed || unknown) return operationalProblem("Checking whether any billing results were saved. Open run details to see what happened.");
      return {...presentation, title: "Reading billing results", description: "Checking the saved bills for this run."};
    }
    if (phase !== "ready") return presentation;

    const summary = result?.summary;
    const usable = summary?.usable === true && financial.has(summary.verdict);
    const bills = usable && Array.isArray(result.bills) ? result.bills : [];
    const failedBills = bills.some((bill) => bill?.verdict === "FAIL");
    const details = {
      verdict: usable ? summary.verdict : "BLOCKED",
      canReview: usable && bills.length > 0,
      reviewLabel: failedBills ? "Review failed bills" : "View bills",
      filter: failedBills ? "FAIL" : "all",
    };

    if (failed || unknown) {
      const saved = usable
        ? summary.verdict === "PASS" ? "The saved bills passed their checks." : "The saved bills contain billing differences."
        : "No trusted billing result is available.";
      const caution = failed
        ? "The pipeline did not finish successfully. Open run details to see what happened."
        : "The job did not report a clear completion status. Open run details before relying on these results.";
      return {...operationalProblem(`${saved} ${caution}`), ...details};
    }
    if (!usable) return {...presentation, ...details, tone: "warning", title: "Results need attention",
      description: "There is no complete, trusted billing result for this run. Open run details or try loading it again.",
    };
    if (summary.verdict === "FAIL") return {...presentation, ...details, tone: "negative", title: "Billing differences found",
      description: "One or more bills failed the checks. Review them before releasing this version.",
    };
    return {...presentation, ...details, tone: "positive", title: "All billing checks passed",
      description: "Both billing versions passed the required checks for this run.",
    };
  }

  return {describe};
});
