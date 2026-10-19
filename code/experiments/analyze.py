"""Aggregate runner output into results/runs.csv, results/summary.md and figures/.

usage: python analyze.py RESULTS_DIR FIGURES_DIR HISTORIES_DIR"""
from __future__ import annotations

import itertools
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "harness"))
from oracle import all_logged_files, classify  # noqa: E402

IMPL_ORDER = ["deltars-1.6.6", "deltars-main", "deltars-main+4858", "spark-4.4.1"]
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
plt.rcParams.update({"axes.spines.top": False, "axes.spines.right": False, "axes.edgecolor": "#888888",
                     "axes.grid": True, "grid.color": "#e5e5e5", "grid.linewidth": 0.6, "axes.axisbelow": True,
                     "font.size": 9, "lines.linewidth": 2})
TAXONOMY = {
    "F1": "DV file referenced by the latest snapshot deleted (full mode)",
    "F2": "Data file referenced by the latest snapshot deleted",
    "F3": "Data file of an unexpired tombstone deleted because an older tombstone for the same path had expired",
    "F4": "DV file referenced only by unexpired tombstones deleted (treated as orphan)",
    "F5": "Data file referenced only by unexpired tombstones deleted",
    "F6": "Other / unclassified",
}


MECHANISMS = [
    "Classification uses the log references of each wrongly deleted file at vacuum time (see `wrong_detail` in the JSONL rows). "
    "Mechanisms, each reproduced by a hand-written minimal history (section 'Minimal reproducing histories'):",
    "",
    "- **F1** (minimal case 1, full mode, 0 h): the keep set of full vacuum held the data paths of active files but not their DV files, "
    "so an active file's DV was an orphan once its mtime passed the cutoff; the latest version becomes unreadable. Fixed by delta-rs #4839 (in main).",
    "- **F3** (minimal case 3, lite and full, 168 h): every DV rewrite commits remove(X, oldDV) + add(X, newDV), so one data file collects several tombstones. "
    "Vacuum queued X as soon as any tombstone for X had expired, although a newer tombstone for X was still inside the retention window; "
    "versions inside the window that read X fail. Fixed by #4858.",
    "- **F4** (minimal cases 2 and 3, full mode, 168 h): DV files referenced only by unexpired tombstones were not in the keep set; "
    "a DV is written when its version is committed, long before the tombstone that retires it, so its mtime is often past the cutoff and full vacuum deleted it as an orphan. "
    "Present in main after #4839, fixed by #4858.",
    "- **E1** (operational, not a deletion of a needed file): the same path was queued more than once (one entry per tombstone). "
    "Concurrent deletes of the same file on Windows fail with 'Access is denied' for the delete-pending file, vacuum returns an error after VACUUM START without VACUUM END, "
    "and in-flight deletes continue after the error is returned. Outcome varies between identical trials. Disappears with the de-duplication in #4858.",
    "- **R1** (reclamation, not integrity): delta-rs lite vacuum only deletes data paths of expired tombstones, never their DV files, so DV files of removed files are never reclaimed in lite mode (all three builds). Spark lite deletes them.",
]


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (math.nan, math.nan)
    p = k / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def fmt_rate(k: int, n: int) -> str:
    lo, hi = wilson(k, n)
    return f"{k}/{n} ({100 * k / n:.1f}%, CI {100 * lo:.1f}-{100 * hi:.1f})" if n else "0/0"


def classify_wrong(detail: dict, kind: str) -> str:
    cutoff = detail["cutoff"]
    if detail["active_in_latest"]:
        return "F1" if kind == "dv" else "F2"
    removes = [r for r in detail["refs"] if r["kind"] == "remove"]
    unexpired = [r for r in removes if (r["ts"] or 0) >= cutoff]
    expired = [r for r in removes if (r["ts"] or 0) < cutoff]
    if kind == "dv" and unexpired:
        return "F4"
    if kind == "data" and unexpired and expired:
        return "F3"
    if kind == "data" and unexpired:
        return "F5"
    return "F6"


def load(results: Path) -> list[dict]:
    rows = []
    for f in sorted(results.glob("runs_*.jsonl")):
        rows.extend(json.loads(line) for line in f.read_text(encoding="utf-8").splitlines() if line.strip())
    return rows


def error_kind(err: str) -> str:
    if not err:
        return ""
    if "Access is denied" in err:
        return "delete-failed-access-denied"
    if "harness" in err:
        return "harness"
    return err.split(":")[0] + ":" + err.split(":")[1][:40] if ":" in err else err[:40]


def flatten(r: dict) -> dict:
    wrong = r.get("wrongly_deleted", {})
    detail = r.get("wrong_detail", {})
    classes = Counter(classify_wrong(detail[p], k) for p, k in wrong.items() if p in detail)
    deleted = r.get("deleted", {})
    retained = r.get("wrongly_retained", {})
    oracle = set(r.get("plan_unsafe_oracle", []))
    flagged = set(r.get("checker_flagged", []))
    reads = r.get("reads", [])
    return {
        "impl": r["impl"],
        "family": r["family"],
        "seed": r["seed"],
        "mode": r["mode"],
        "retention_h": r["retention_h"],
        "n_versions": r.get("n_versions"),
        "n_protected": r.get("n_protected"),
        "has_checkpoint": r.get("has_checkpoint"),
        "files_before": r.get("files_before"),
        "plan_size": r.get("plan_size"),
        "plan_raw_len": r.get("plan_raw_len"),
        "plan_dups": len(r.get("plan_duplicates", [])),
        "deleted_total": len(deleted),
        "deleted_data": sum(v == "data" for v in deleted.values()),
        "deleted_dv": sum(v == "dv" for v in deleted.values()),
        "deleted_orphan": sum(v == "orphan" for v in deleted.values()),
        "deleted_log": sum(v == "log" for v in deleted.values()),
        "wrong_total": len(wrong),
        "wrong_data": sum(v == "data" for v in wrong.values()),
        "wrong_dv": sum(v == "dv" for v in wrong.values()),
        **{f"wrong_{c}": classes.get(c, 0) for c in TAXONOMY},
        "unreadable_protected": len(r.get("unreadable_protected", [])),
        "changed_protected": len(r.get("changed_protected", [])),
        "latest_ok": r.get("latest_readable"),
        "rows_lost_protected": sum(x["rows_lost"] for x in reads if x["protected"] and not x["same"]),
        "unreadable_unprotected": sum(1 for x in reads if not x["protected"] and not x["ok"]),
        "reclaimable": r.get("reclaimable_files"),
        "retained_total": len(retained),
        "retained_data": sum(v == "data" for v in retained.values()),
        "retained_dv": sum(v == "dv" for v in retained.values()),
        "retained_orphan": sum(v == "orphan" for v in retained.values()),
        "plan_unsafe_oracle": len(oracle),
        "checker_flagged": len(flagged),
        "checker_tp": len(oracle & flagged),
        "checker_fp": len(flagged - oracle),
        "checker_fn": len(oracle - flagged),
        "plan_vs_actual_diff": len(r.get("plan_vs_actual_diff", [])),
        "dry_run_s": r.get("dry_run_s"),
        "vacuum_s": r.get("vacuum_s"),
        "checker_s": r.get("checker_s"),
        "checker_actions": r.get("checker_actions"),
        "error_kind": error_kind(r.get("error", "")),
        "error": r.get("error", "")[:300],
    }


def md_table(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    lines = ["| " + " | ".join(map(str, cols)) + " |", "|" + "---|" * len(cols)]
    lines += ["| " + " | ".join(str(v) for v in row) + " |" for row in df.itertuples(index=False)]
    return "\n".join(lines)


def rq1_table(df: pd.DataFrame) -> pd.DataFrame:
    out = []
    for (impl, mode, ret), g in df.groupby(["impl", "mode", "retention_h"], sort=False):
        n = len(g)
        out.append(
            {
                "implementation": impl,
                "mode": mode,
                "retention_h": ret,
                "N runs": n,
                "runs with unsafe deletion": fmt_rate(int((g.wrong_total > 0).sum()), n),
                "runs with unreadable protected version": fmt_rate(int((g.unreadable_protected > 0).sum()), n),
                "latest version unreadable": int((g.latest_ok == False).sum()),  # noqa: E712
                "wrong data files": int(g.wrong_data.sum()),
                "wrong DV files": int(g.wrong_dv.sum()),
                "unreadable protected versions": int(g.unreadable_protected.sum()),
                "vacuum errors": int((g.error_kind != "").sum()),
                "plans with duplicate paths": int((g.plan_dups > 0).sum()),
            }
        )
    return pd.DataFrame(out)


def ordered(df: pd.DataFrame) -> pd.DataFrame:
    order = {k: i for i, k in enumerate(IMPL_ORDER)}
    return df.assign(_o=df.impl.map(order)).sort_values(["_o", "mode", "retention_h"]).drop(columns="_o")


def main() -> None:
    results, figures, histories = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    logged: dict = {}

    def kind_of(family: str, seed: int, path: str) -> str:
        if (family, seed) not in logged:
            logged[(family, seed)] = all_logged_files(histories / family / f"seed_{seed}" / "table")
        return classify(path, *logged[(family, seed)])

    figures.mkdir(parents=True, exist_ok=True)
    raw = load(results)
    df = ordered(pd.DataFrame([flatten(r) for r in raw]))
    df.to_csv(results / "runs.csv", index=False)
    md = ["# Results summary", "", "Generated by experiments/analyze.py from results/runs_*.jsonl. Rates are runs with the event / runs, with Wilson 95% confidence intervals.", ""]

    md += ["## RQ1: unsafe deletions and unreadable versions", "", md_table(rq1_table(df)), ""]
    by_family = []
    for (impl, mode, ret, fam), g in df.groupby(["impl", "mode", "retention_h", "family"], sort=False):
        by_family.append({"implementation": impl, "mode": mode, "retention_h": ret, "family": fam, "N": len(g),
                          "unsafe runs": int((g.wrong_total > 0).sum()), "unreadable runs": int((g.unreadable_protected > 0).sum())})
    md += ["### By checkpoint family", "", md_table(pd.DataFrame(by_family)), ""]

    tax = df.groupby(["impl", "mode", "retention_h"], sort=False)[[f"wrong_{c}" for c in TAXONOMY]].sum().reset_index()
    tax_runs = df.assign(**{f"r_{c}": df[f"wrong_{c}"] > 0 for c in TAXONOMY}).groupby(["impl", "mode", "retention_h"], sort=False)[[f"r_{c}" for c in TAXONOMY]].sum().reset_index()
    md += ["## Failure taxonomy", "", *[f"- **{k}**: {v}" for k, v in TAXONOMY.items()], "", *MECHANISMS, "",
           "Wrongly deleted files per class:", "", md_table(tax), "", "Runs with at least one file of the class:", "", md_table(tax_runs), ""]

    dup = df.assign(has_dup=df.plan_dups > 0, has_err=df.error_kind != "").groupby(["impl", "has_dup", "has_err"]).size().reset_index(name="runs")
    md += ["### Duplicate paths in the vacuum plan vs vacuum errors", "",
           "`has_dup`: the dry-run list named at least one path more than once. `has_err`: the real vacuum raised an error.", "", md_table(dup), ""]
    vc = results / "vacuum_commits.csv"
    if vc.exists():
        v = pd.read_csv(vc)
        md += ["### Commits left by vacuum in kept work directories (experiments/vacuum_commits.py)", "",
               md_table(v.groupby(["impl", "vacuum_error", "commits"]).size().reset_index(name="runs")), ""]
    errs = df[df.error_kind != ""].groupby(["impl", "mode", "retention_h", "error_kind"]).size().reset_index(name="runs")
    md += ["### Vacuum errors", "", md_table(errs) if len(errs) else "None.", ""]

    repeats = sorted(results.glob("repeat_*.jsonl"))
    if repeats:
        key = lambda r: (r["impl"], r["family"], r["seed"], r["mode"], r["retention_h"])  # noqa: E731
        base = {key(r): r for r in raw}
        trials = defaultdict(list)
        for f in repeats:
            for line in f.read_text(encoding="utf-8").splitlines():
                r = json.loads(line)
                if key(r) in base:
                    trials[key(r)].append(r)
        rep_rows = []
        for (mode, ret), keys in itertools.groupby(sorted(trials, key=lambda k: (k[3], k[4], k[2])), key=lambda k: (k[3], k[4])):
            keys = list(keys)
            outcomes = [[base[k]] + trials[k] for k in keys]
            err_flip = sum(len({bool(t["error"]) for t in o}) > 1 for o in outcomes)
            wrong_flip = sum(len({tuple(sorted(t.get("wrongly_deleted", {}))) for t in o}) > 1 for o in outcomes)
            unread_flip = sum(len({tuple(t.get("unreadable_protected", [])) for t in o}) > 1 for o in outcomes)
            any_err = sum(any(t["error"] for t in o) for o in outcomes)
            rep_rows.append({"mode": mode, "retention_h": ret, "histories": len(keys), "trials per history": 1 + len(outcomes[0]) - 1,
                             "error in any trial": any_err, "error status differs between trials": err_flip,
                             "unsafe-deletion set differs": wrong_flip, "unreadable versions differ": unread_flip})
        md += ["### Repeatability of deltars-1.6.6 outcomes (nockpt seeds 0-24, 3 trials)", "", md_table(pd.DataFrame(rep_rows)), ""]

    ret = df.groupby(["impl", "mode", "retention_h"], sort=False)[["reclaimable", "retained_total", "retained_data", "retained_dv", "retained_orphan"]].sum().reset_index()
    md += ["### Files not reclaimed", "", "`reclaimable` counts files that no protected version references and whose mtime is older than the cutoff. `retained_*` are those still present after vacuum. Lite mode never lists the directory, so retained orphans are expected there by design.", "", md_table(ret), ""]

    # RQ2
    keys = ["family", "seed", "mode", "retention_h"]
    plans = defaultdict(dict)
    for r in raw:
        if "plan" in r:
            plans[tuple(r[k] for k in keys)][r["impl"]] = (set(r["plan"]), r.get("deleted", {}), set(r.get("plan_unsafe_oracle", [])))
    impls = [i for i in IMPL_ORDER if any(i in v for v in plans.values())]
    pair_rows = []
    for a, b in itertools.combinations(impls, 2):
        for mode in ("lite", "full"):
            for retention in sorted(df.retention_h.unique()):
                same = n = 0
                diff_kinds: Counter = Counter()
                for k, v in plans.items():
                    if k[2] != mode or k[3] != retention or a not in v or b not in v:
                        continue
                    n += 1
                    pa_, pb_ = v[a][0], v[b][0]
                    same += pa_ == pb_
                    for p in pa_ - pb_:
                        diff_kinds[f"only {a}: {kind_of(k[0], k[1], p)}{' UNSAFE' if p in v[a][2] else ''}"] += 1
                    for p in pb_ - pa_:
                        diff_kinds[f"only {b}: {kind_of(k[0], k[1], p)}{' UNSAFE' if p in v[b][2] else ''}"] += 1
                if n:
                    pair_rows.append({"A": a, "B": b, "mode": mode, "retention_h": retention, "N histories": n,
                                      "identical plans": fmt_rate(same, n),
                                      "differing files (by side, kind)": "; ".join(f"{k}: {c}" for k, c in sorted(diff_kinds.items())) or "-"})
    md += ["## RQ2: agreement between implementations (dry-run plans on identical histories)", "",
           "Each file named in a plan is classified as data / dv / orphan from the log; UNSAFE marks files the oracle says a protected version needs.", "",
           md_table(pd.DataFrame(pair_rows)) if pair_rows else "No overlapping runs.", ""]

    # RQ3
    ck = df[df.checker_s.notna()]
    rows3 = []
    for impl, g in ck.groupby("impl", sort=False):
        unsafe_plans = int((g.plan_unsafe_oracle > 0).sum())
        flagged_plans = int((g.checker_flagged > 0).sum())
        tp_plans = int(((g.plan_unsafe_oracle > 0) & (g.checker_flagged > 0)).sum())
        tp, fp, fn = int(g.checker_tp.sum()), int(g.checker_fp.sum()), int(g.checker_fn.sum())
        rows3.append({"plans from": impl, "N plans": len(g), "unsafe plans (oracle)": unsafe_plans, "flagged plans": flagged_plans,
                      "plan recall": f"{tp_plans}/{unsafe_plans}" if unsafe_plans else "n/a",
                      "plan precision": f"{tp_plans}/{flagged_plans}" if flagged_plans else "n/a",
                      "file TP": tp, "file FP": fp, "file FN": fn})
    g = ck
    tp, fp, fn = int(g.checker_tp.sum()), int(g.checker_fp.sum()), int(g.checker_fn.sum())
    unsafe_plans = int((g.plan_unsafe_oracle > 0).sum())
    flagged_plans = int((g.checker_flagged > 0).sum())
    tp_plans = int(((g.plan_unsafe_oracle > 0) & (g.checker_flagged > 0)).sum())
    rows3.append({"plans from": "ALL", "N plans": len(g), "unsafe plans (oracle)": unsafe_plans, "flagged plans": flagged_plans,
                  "plan recall": f"{tp_plans}/{unsafe_plans}", "plan precision": f"{tp_plans}/{flagged_plans}",
                  "file TP": tp, "file FP": fp, "file FN": fn})
    lo_r, hi_r = wilson(tp_plans, unsafe_plans)
    md += ["## RQ3: safety checker vs oracle", "", md_table(pd.DataFrame(rows3)), "",
           f"Plan-level recall over all implementations: {tp_plans}/{unsafe_plans} (Wilson 95% CI {100*lo_r:.1f}-{100*hi_r:.1f}%).", "",
           "Dry-run plan vs actual deletions: runs where the set of files actually deleted differs from the dry-run plan:", "",
           md_table(df.assign(d=df.plan_vs_actual_diff > 0).groupby(["impl", "mode", "retention_h"], sort=False).agg(N=("d", "size"), differing=("d", "sum")).reset_index()), ""]

    cc = results / "crosscheck_spark.csv"
    if cc.exists():
        c = pd.read_csv(cc)
        t = c.groupby("impl", sort=False).agg(
            runs=("run", "nunique"), version_reads=("agree", "size"), verdicts_agree=("agree", "sum"),
            oracle_unreadable=("oracle_ok", lambda s: int((~s.astype(bool)).sum())),
            spark_unreadable=("spark_ok", lambda s: int((~s.astype(bool)).sum())),
        ).reset_index()
        g = c.groupby(["protected", "spark_guard_refused"]).size().reset_index(name="version reads")
        md += ["### Oracle validated against Spark (experiments/crosscheck_spark.py)", "",
               "Kept work directories (up to 25 failing and 10 passing runs per implementation, chosen in CRC32 order of the run id) re-read version by version with Delta Lake for Spark. "
               "`verdicts_agree` counts reads where Spark and the oracle agree on readable and on identical rows.", "", md_table(t), "",
               "Spark 4.4 by default refuses time travel to versions older than delta.deletedFileRetentionDuration (error DELTA_UNSUPPORTED_TIME_TRAVEL_BEYOND_DELETED_FILE_RETENTION_DURATION). "
               "Refused reads were retried with spark.databricks.delta.vacuum.enforceTimeTravelWithinDeletedFileRetentionDuration=false. Refusals by whether the oracle counts the version as protected:", "",
               md_table(g), ""]

    abl = results / "ablation.csv"
    if abl.exists():
        a = pd.read_csv(abl)
        t = a.groupby(["variant", "retention_h"]).agg(
            histories=("seed", "size"),
            histories_with_unprotected_needed_file=("missed", lambda s: int((s > 0).sum())),
            needed_files_unprotected=("missed", "sum"),
            of_which_dv=("missed_dv", "sum"),
            histories_over_protected=("over_protected", lambda s: int((s > 0).sum())),
            files_over_protected=("over_protected", "sum"),
        ).reset_index()
        md += ["### Ablation of the invariant (experiments/ablation.py)", "",
               "Protected set of each rule vs the oracle's needed set on every generated history, with the log aged relative to the evaluation time. "
               "A rule is unsafe for a history if a file needed by a protected version is not protected.", "", md_table(t), ""]

    timing = df.groupby(["impl", "mode"], sort=False).agg(
        N=("dry_run_s", "size"), dry_run_median_s=("dry_run_s", "median"), vacuum_median_s=("vacuum_s", "median"),
        checker_median_s=("checker_s", "median"), checker_p95_s=("checker_s", lambda s: s.quantile(0.95))).reset_index().round(4)
    md += ["### Timings on the matrix histories (small tables)", "",
           "Indicative only: these runs shared the machine with other experiment processes, and all of them used the checker before its DV-path caching change "
           "(commit 'checker: resolve relative DV paths without filesystem calls'). The controlled measurement is RQ4.", "", md_table(timing), ""]
    perf = results / "perf.csv"
    if perf.exists():
        p = pd.read_csv(perf)
        acts = p[p.what == "checker"].groupby("table").actions.max()
        p["actions"] = p.table.map(acts).fillna(p.actions)
        md += ["## RQ4: overhead on larger synthetic tables", "", "See results/perf.csv and figures/rq4_*.png. Each delta-rs and checker value is measured in a fresh process; peak_mb is the peak working set of that process (import-only baselines are in perf.csv). Spark runs in one warm JVM, memory not measured. Medians over repetitions:", "",
               md_table(p[p.what != "import-baseline"].groupby(["table", "files", "actions", "retention_h", "mode", "what", "impl"]).agg(reps=("seconds", "size"), median_s=("seconds", "median"), peak_mb=("peak_mb", "median"), n_plan_or_flagged=("n", "median")).reset_index().round(3)), ""]

    minimal = [json.loads(line) for f in sorted(results.glob("minimal_*.jsonl")) for line in f.read_text(encoding="utf-8").splitlines() if line.strip()]
    if minimal:
        mrows = []
        for r in sorted(minimal, key=lambda r: (r["seed"], IMPL_ORDER.index(r["impl"]) if r["impl"] in IMPL_ORDER else 9, r["mode"], r["retention_h"])):
            detail = r.get("wrong_detail", {})
            classes = sorted({classify_wrong(detail[p], k) for p, k in r.get("wrongly_deleted", {}).items() if p in detail})
            mrows.append({"case": r["seed"], "implementation": r["impl"], "mode": r["mode"], "retention_h": r["retention_h"],
                          "plan": len(r.get("plan", [])), "unsafe deletions": len(r.get("wrongly_deleted", {})),
                          "classes": ",".join(classes) or "-", "unreadable protected versions": r.get("unreadable_protected"),
                          "checker flagged": len(r.get("checker_flagged", [])), "error": error_kind(r.get("error", "")) or "-"})
        md += ["## Minimal reproducing histories", "",
               "Case 1: write 1 file (v0), DELETE (v1); ages 12.5 h and 2.5 h. "
               "Case 2: write (v0), DELETE (v1), DELETE (v2); ages 400.5, 202.5, 2.5 h. "
               "Case 3: write (v0), DELETE (v1), DELETE (v2), REORG PURGE (v3); ages 300.5, 200.5, 100.5, 2.5 h. "
               "See experiments/minimal.py.", "", md_table(pd.DataFrame(mrows)), ""]

    (results / "summary.md").write_text("\n".join(md), encoding="utf-8")
    figures_rq(df, figures)
    if perf.exists():
        figures_perf(pd.read_csv(perf), figures)


def save(fig, figures: Path, name: str) -> None:
    fig.tight_layout()
    fig.savefig(figures / f"{name}.png", dpi=160)
    fig.savefig(figures / f"{name}.pdf")
    plt.close(fig)


def bar_rates(df: pd.DataFrame, figures: Path, column: str, title: str, name: str) -> None:
    impls = [i for i in IMPL_ORDER if i in set(df.impl)]
    combos = [(m, r) for m in ("lite", "full") for r in sorted(df.retention_h.unique())]
    fig, ax = plt.subplots(figsize=(8, 3.6))
    width = 0.8 / len(impls)
    colors = PALETTE
    for i, impl in enumerate(impls):
        xs, ys, lo, hi = [], [], [], []
        for j, (m, r) in enumerate(combos):
            g = df[(df.impl == impl) & (df["mode"] == m) & (df.retention_h == r)]
            k, n = int((g[column] > 0).sum()), len(g)
            if not n:
                continue
            a, b = wilson(k, n)
            xs.append(j + i * width)
            ys.append(100 * k / n)
            lo.append(100 * (k / n - a))
            hi.append(100 * (b - k / n))
        for x, y in zip(xs, ys):
            if y == 0:
                ax.text(x, 1, "0", ha="center", va="bottom", fontsize=6, color="#555555")
        ax.bar(xs, ys, width * 0.92, yerr=[lo, hi], capsize=2, label=impl, color=colors[i], error_kw={"elinewidth": 0.8, "ecolor": "#555555"})
    ax.set_xticks([j + 0.4 - width / 2 for j in range(len(combos))])
    ax.set_xticklabels([f"{m}, {r}h" for m, r in combos])
    ax.set_ylabel("% of runs")
    ax.set_ylim(0, 100)
    ax.set_title(title)
    ax.legend(fontsize=8, frameon=False)
    save(fig, figures, name)


def figure_taxonomy(df: pd.DataFrame, figures: Path) -> None:
    classes = [c for c in TAXONOMY if df[f"wrong_{c}"].sum() > 0]
    cells = [(i, m, r) for i in IMPL_ORDER if i in set(df.impl) for m in ("lite", "full") for r in sorted(df.retention_h.unique())]
    fig, ax = plt.subplots(figsize=(9, 3.6))
    bottom = [0] * len(cells)
    for j, c in enumerate(classes):
        vals = [int(df[(df.impl == i) & (df["mode"] == m) & (df.retention_h == r)][f"wrong_{c}"].sum()) for i, m, r in cells]
        ax.bar(range(len(cells)), vals, 0.7, bottom=bottom, label=f"{c}: {TAXONOMY[c]}", color=PALETTE[j], edgecolor="white", linewidth=1)
        bottom = [b + v for b, v in zip(bottom, vals)]
    ax.set_xticks(range(len(cells)))
    ax.set_xticklabels([f"{i}\n{m} {r}h" for i, m, r in cells], fontsize=6, rotation=90)
    ax.set_ylabel("wrongly deleted files (all runs)")
    ax.set_title("Unsafe deletions by failure class")
    ax.legend(fontsize=6, frameon=False, loc="upper left")
    save(fig, figures, "rq1_taxonomy")


def figures_rq(df: pd.DataFrame, figures: Path) -> None:
    figure_taxonomy(df, figures)
    bar_rates(df, figures, "wrong_total", "Runs that deleted a file needed by a protected version", "rq1_unsafe_rate")
    bar_rates(df, figures, "unreadable_protected", "Runs leaving a protected version unreadable", "rq1_unreadable_rate")
    ck = df[df.checker_s.notna() & df.checker_actions.notna()]
    if len(ck):
        fig, ax = plt.subplots(figsize=(5, 3.4))
        ax.scatter(ck.checker_actions, 1000 * ck.checker_s, s=8, alpha=0.4, color=PALETTE[0], edgecolors="none")
        ax.set_xlabel("log actions read by checker")
        ax.set_ylabel("checker time (ms)")
        ax.set_title("Checker time on matrix histories")
        save(fig, figures, "rq3_checker_time_small")


def figures_perf(p_all: pd.DataFrame, figures: Path) -> None:
    for retention in sorted(p_all.retention_h.dropna().unique()):
        figures_perf_one(p_all[p_all.retention_h == retention], figures, int(retention))


def figures_perf_one(p: pd.DataFrame, figures: Path, retention: int) -> None:
    p = p[p.table.str.startswith("wide")]
    med = p.groupby(["files", "impl", "what"]).seconds.median().reset_index()
    fig, ax = plt.subplots(figsize=(8.5, 3.8))
    styles = {"dry_run": "-", "vacuum": "--", "checker": ":"}
    impl_color = {k: PALETTE[i] for i, k in enumerate(IMPL_ORDER)}
    impl_color["checker"] = "#222222"
    for (impl, what), g in med.groupby(["impl", "what"]):
        if what == "import-baseline":
            continue
        g = g.sort_values("files")
        ax.plot(g.files, g.seconds, marker="o", markersize=4, linestyle=styles.get(what, "-"), color=impl_color.get(impl, "#777777"), label=f"{impl} {what}")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("data files in table")
    ax.set_ylabel("seconds (median)")
    ax.set_title(f"Vacuum vs checker runtime (wide tables, retention {retention} h)")
    ax.legend(fontsize=7, frameon=False, loc="center left", bbox_to_anchor=(1.01, 0.5))
    save(fig, figures, f"rq4_runtime_{retention}h")
    mem = p[p.peak_mb.notna()].groupby(["files", "impl", "what"]).peak_mb.median().reset_index()
    if len(mem):
        fig, ax = plt.subplots(figsize=(8.5, 3.8))
        for (impl, what), g in mem.groupby(["impl", "what"]):
            if what == "import-baseline":
                continue
            g = g.sort_values("files")
            ax.plot(g.files, g.peak_mb, marker="o", markersize=4, linestyle=styles.get(what, "-"), color=impl_color.get(impl, "#777777"), label=f"{impl} {what}")
        ax.set_ylabel("peak working set (MB, median over modes and repetitions)")
        ax.set_xscale("log")
        ax.set_xlabel("data files in table")
        ax.set_title(f"Peak memory of a fresh process (retention {retention} h)")
        ax.legend(fontsize=7, frameon=False, loc="center left", bbox_to_anchor=(1.01, 0.5))
        save(fig, figures, f"rq4_memory_{retention}h")


if __name__ == "__main__":
    main()
