/**
 * Quant Factor Lab CLI (`npm run research:factors`)
 * -------------------------------------------------
 * Read-only diagnostic research desk modeled after RD-Agent's factor leaderboard
 * & OOS validation loop, pulling live Walk-Forward Shadow + Counterfactual data
 * directly from the Multi-Instance Orchestrator.
 *
 * Usage:
 *   npm run research:factors
 *   npm run research:factors -- --instance binance_dry
 */

import "dotenv/config";

function parseArgs(argv: string[]) {
  let instanceFilter = "";
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--instance" && argv[i + 1]) {
      instanceFilter = String(argv[i + 1]).trim();
      i++;
    }
  }
  return { instanceFilter };
}

function fmtSigned(v: number | null | undefined, suffix = "", digits = 2): string {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return "-";
  const num = Number(v);
  return `${num >= 0 ? "+" : ""}${num.toFixed(digits)}${suffix}`;
}

function pad(s: string, len: number): string {
  return s.length >= len ? s.slice(0, len) : s + " ".repeat(len - s.length);
}

async function main() {
  const { instanceFilter } = parseArgs(process.argv.slice(2));
  const baseUrl = (process.env.ORCH_URL || "").replace(/\/+$/, "");
  const username = process.env.ORCH_USERNAME || "";
  const password = process.env.ORCH_PASSWORD || "";

  if (!baseUrl || !username || !password) {
    console.error("Missing ORCH_URL, ORCH_USERNAME, or ORCH_PASSWORD in .env");
    process.exit(1);
  }

  const loginRes = await fetch(`${baseUrl}/api/orch/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  if (!loginRes.ok) {
    console.error(`Orchestrator login failed: HTTP ${loginRes.status}`);
    process.exit(1);
  }
  const cookies = loginRes.headers.getSetCookie
    ? loginRes.headers.getSetCookie()
    : [loginRes.headers.get("set-cookie")].filter((x): x is string => Boolean(x));
  const cookieHeader = cookies.map((c) => c.split(";")[0]).join("; ");

  const fetchJson = async (url: string) => {
    const r = await fetch(`${baseUrl}${url}`, { headers: { Cookie: cookieHeader } });
    if (!r.ok) throw new Error(`HTTP ${r.status} for ${url}`);
    return r.json();
  };

  const instRes = await fetchJson("/api/orch/instances");
  const allInstances: Array<{ instanceKey: string; status: string }> = Array.isArray(instRes)
    ? instRes
    : instRes.instances || [];
  const targets = instanceFilter
    ? allInstances.filter((i) => i.instanceKey === instanceFilter)
    : allInstances;

  if (targets.length === 0) {
    console.error(`No matching instances found${instanceFilter ? ` for "${instanceFilter}"` : ""}.`);
    process.exit(1);
  }

  console.log(`\n=== Aegis Quant Factor Lab Report (${new Date().toISOString()}) ===`);

  for (const inst of targets) {
    const key = inst.instanceKey;
    const lr: any = await fetchJson(`/i/${key}/api/learning/report`);
    const sl: any = lr?.shadowLearning || {};
    const cfgParams = lr?.summary?.activeTechnicalParams || {};

    console.log(`\n--------------------------------------------------------------------------------`);
    console.log(
      `INSTANCE: ${key} (${inst.status}) | Pullback: ${cfgParams.portfolioPullbackMode || "-"} | TP1: ${cfgParams.portfolioTp1TargetMode || "-"} | Q6 Mode: ${cfgParams.portfolioQuantContextGateMode || "auto_half_size"}`,
    );

    const train = sl?.tuning?.baselineTrain;
    const test = sl?.tuning?.baselineTest;
    const eqOos = sl?.confidenceSizingAudit?.equalWeight;
    console.log(
      `BASELINE TRAIN: ${train ? `${train.trades}t, WR ${train.winRatePct}%, Avg ${fmtSigned(train.avgPnlPct, "%", 3)}, PF ${train.profitFactor}` : "n/a"}`,
    );
    console.log(
      `BASELINE OOS  : ${test ? `${test.trades}t, WR ${test.winRatePct}%, Avg ${fmtSigned(test.avgPnlPct, "%", 3)}, PF ${test.profitFactor}` : "n/a"}${eqOos ? ` | Portfolio OOS: PF ${eqOos.profitFactor}, MaxDD ${eqOos.maxDrawdownPct}%, Sharpe ${eqOos.sharpeProxy}` : ""}`,
    );

    const icRows: any[] = Array.isArray(sl?.factorRankIc) ? sl.factorRankIc : [];
    console.log(`\n[1] Factor Rank IC Leaderboard (Train 70% vs OOS Test 30%)`);
    if (icRows.length === 0) {
      console.log("  (no Rank IC data available yet)");
    } else {
      console.log(`  ${pad("Factor", 28)} ${pad("Verdict", 12)} ${pad("TrainIC", 9)} ${pad("TestIC", 9)} ${pad("OOS N", 6)} Hint`);
      for (const r of icRows) {
        console.log(
          `  ${pad(String(r.label || r.factor), 28)} ${pad(String(r.verdict), 12)} ${pad(fmtSigned(r.trainIc, "", 3), 9)} ${pad(fmtSigned(r.testIc, "", 3), 9)} ${pad(String(r.testN ?? "-"), 6)} ${r.hint || ""}`,
        );
      }
    }

    const gateRules: any[] = Array.isArray(sl?.contextGateOos?.rules) ? sl.contextGateOos.rules : [];
    console.log(`\n[2] Context Gate OOS Tournament (Closed-Loop Q6 Candidates)`);
    if (gateRules.length === 0) {
      console.log("  (no Context Gate evaluations yet)");
    } else {
      console.log(`  Verdict: ${sl.contextGateOos.verdict}`);
      console.log(
        `  ${pad("Rule ID", 22)} ${pad("Status", 14)} ${pad("Action", 10)} ${pad("Aff(Tr/Te)", 11)} ${pad("Hard(Tr/Te)", 17)} ${pad("Half(Tr/Te)", 17)} OOS PF`,
      );
      for (const g of gateRules) {
        const aff = `${g.affectedTrain}/${g.affectedTest}`;
        const hardStr = `${fmtSigned(g.hardGate?.trainDeltaPct, "", 3)}/${fmtSigned(g.hardGate?.testDeltaPct, "", 3)}`;
        const halfStr = `${fmtSigned(g.halfSize?.trainDeltaPct, "", 3)}/${fmtSigned(g.halfSize?.testDeltaPct, "", 3)}`;
        console.log(
          `  ${pad(String(g.id), 22)} ${pad(String(g.status), 14)} ${pad(String(g.recommendedAction), 10)} ${pad(aff, 11)} ${pad(hardStr, 17)} ${pad(halfStr, 17)} ${g.hardGate?.test?.profitFactor ?? "-"}`,
        );
      }
    }

    const blocked: any[] = Array.isArray(lr?.blockedSignals?.breakdown) ? lr.blockedSignals.breakdown : [];
    console.log(`\n[3] Live Blocked-Signal Counterfactual Efficiency (Top 8)`);
    if (blocked.length === 0) {
      console.log("  (no evaluated blocked signals yet)");
    } else {
      const topBlocked = blocked.slice().sort((a, b) => (b.evaluated || 0) - (a.evaluated || 0)).slice(0, 8);
      console.log(`  ${pad("Block Reason", 28)} ${pad("Eval N", 7)} ${pad("Saved%", 8)} ${pad("MissWin%", 9)} AvgCF%`);
      for (const b of topBlocked) {
        console.log(
          `  ${pad(String(b.reason), 28)} ${pad(String(b.evaluated), 7)} ${pad(`${b.savedPct}%`, 8)} ${pad(`${b.missedWinPct}%`, 9)} ${fmtSigned(b.avgCounterfactualPnlPct, "%", 2)}`,
        );
      }
    }
  }
  console.log("");
}

main().catch((err) => {
  console.error(`Factor Lab error: ${(err as Error).message}`);
  process.exit(1);
});
