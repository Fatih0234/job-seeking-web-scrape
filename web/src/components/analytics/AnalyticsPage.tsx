"use client";

import Link from "next/link";
import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import ThemeToggle from "@/components/chrome/ThemeToggle";

const TrendChart = dynamic(() => import("./TrendChart"), {
  ssr: false,
  loading: () => <div className="h-full w-full animate-pulse rounded-lg bg-gray-100 dark:bg-gray-800" />,
});

type KPIs = {
  total_active_jobs: number;
  remote_roles: number;
  new_companies_7d: number;
  as_of: string;
} | null;

type TrendRow = { bucket_start: string; jobs_unique: number };
type SkillRow = { days_window: number; skill: string; jobs_unique: number };
type HotspotRow = {
  map_city_label: string;
  new_24h_unique: number;
  new_7d_unique: number;
  lat: number;
  lon: number;
};

function cx(...parts: Array<string | false | null | undefined>) {
  return parts.filter(Boolean).join(" ");
}

function fmtK(n: number | null | undefined) {
  if (n == null) return "—";
  if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  return String(n);
}

export default function AnalyticsPage() {
  const [kpis, setKpis] = useState<KPIs>(null);
  const [trend, setTrend] = useState<TrendRow[]>([]);
  const [skills, setSkills] = useState<SkillRow[]>([]);
  const [hotspots, setHotspots] = useState<HotspotRow[]>([]);

  useEffect(() => {
    void (async () => {
      const [k, t, s, h] = await Promise.all([
        fetch("/api/analytics/kpis").then((r) => r.json()).catch(() => null),
        fetch("/api/analytics/trend?months=8").then((r) => r.json()).catch(() => null),
        fetch("/api/analytics/top-skills?days=90&limit=10").then((r) => r.json()).catch(() => null),
        fetch("/api/analytics/hotspots?days=7&limit=6").then((r) => r.json()).catch(() => null),
      ]);

      setKpis(k?.kpis ?? null);
      setTrend(t?.rows ?? []);
      setSkills(s?.rows ?? []);
      setHotspots(h?.rows ?? []);
    })();
  }, []);

  const trendChart = useMemo(() => {
    return trend.map((r) => ({
      month: new Date(r.bucket_start).toLocaleString(undefined, { month: "short" }),
      jobs: r.jobs_unique,
    }));
  }, [trend]);

  const maxSkill = useMemo(() => Math.max(1, ...skills.map((s) => s.jobs_unique ?? 0)), [skills]);

  return (
    <main className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-background-light dark:bg-background-dark">
      <header className="sticky top-0 z-20 flex h-16 items-center justify-between border-b border-border-light bg-surface-light px-6 dark:border-border-dark dark:bg-surface-dark">
        <div className="flex items-center gap-4">
          <div className="bg-background-light p-1 rounded-lg flex border border-border-light dark:bg-background-dark dark:border-border-dark">
            <Link
              href="/map"
              className="px-4 py-1.5 rounded-md text-sm font-medium text-subtext-light hover:text-text-light dark:text-subtext-dark dark:hover:text-text-dark"
            >
              Map
            </Link>
            <Link
              href="/analytics"
              className="px-4 py-1.5 rounded-md text-sm font-semibold bg-surface-light shadow-sm text-primary dark:bg-surface-dark"
            >
              Analytics
            </Link>
          </div>
          <span className="h-6 w-px bg-border-light dark:bg-border-dark" />
          <div className="flex items-center gap-2 text-sm text-subtext-light dark:text-subtext-dark">
            <span className="material-symbols-outlined text-lg">location_on</span>
            <span>Germany + DACH</span>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <div className="relative hidden sm:block">
            <span className="material-symbols-outlined absolute left-3 top-2 text-lg text-subtext-light dark:text-subtext-dark">
              search
            </span>
            <input
              className="w-64 rounded-lg border border-border-light bg-background-light py-1.5 pl-9 pr-3 text-sm text-text-light placeholder:text-subtext-light focus:border-primary focus:ring-primary dark:border-border-dark dark:bg-background-dark dark:text-text-dark dark:placeholder:text-subtext-dark"
              placeholder="Search data..."
              type="text"
              disabled
            />
          </div>
          <button
            type="button"
            className="grid h-9 w-9 place-items-center rounded-full text-subtext-light transition-colors hover:bg-gray-100 hover:text-text-light dark:text-subtext-dark dark:hover:bg-gray-800 dark:hover:text-text-dark"
            aria-label="Notifications"
          >
            <span className="material-symbols-outlined">notifications_none</span>
          </button>
          <ThemeToggle />
        </div>
      </header>

      <div className="flex-1 space-y-6 p-6">
        <section className="grid grid-cols-1 gap-6 md:grid-cols-2 lg:grid-cols-4">
          <div className="rounded-xl border border-border-light bg-surface-light p-5 shadow-sm dark:border-border-dark dark:bg-surface-dark">
            <div className="mb-4 flex items-start justify-between">
              <div>
                <p className="text-sm font-medium text-subtext-light dark:text-subtext-dark">Total Active Jobs</p>
                <h3 className="mt-1 text-2xl font-bold text-text-light dark:text-text-dark">
                  {fmtK(kpis?.total_active_jobs)}
                </h3>
              </div>
              <div className="rounded-lg bg-blue-50 p-2 text-primary dark:bg-blue-900/20">
                <span className="material-symbols-outlined">work</span>
              </div>
            </div>
            <div className="text-xs text-subtext-light dark:text-subtext-dark">Active = seen in last 60 days</div>
          </div>

          <div className="rounded-xl border border-border-light bg-surface-light p-5 shadow-sm dark:border-border-dark dark:bg-surface-dark">
            <div className="mb-4 flex items-start justify-between">
              <div>
                <p className="text-sm font-medium text-subtext-light dark:text-subtext-dark">Remote Roles</p>
                <h3 className="mt-1 text-2xl font-bold text-text-light dark:text-text-dark">{fmtK(kpis?.remote_roles)}</h3>
              </div>
              <div className="rounded-lg bg-purple-50 p-2 text-purple-600 dark:bg-purple-900/20 dark:text-purple-400">
                <span className="material-symbols-outlined">laptop_chromebook</span>
              </div>
            </div>
            <div className="text-xs text-subtext-light dark:text-subtext-dark">Remote-like = text + platform flags</div>
          </div>

          <div className="rounded-xl border border-border-light bg-surface-light p-5 shadow-sm dark:border-border-dark dark:bg-surface-dark">
            <div className="mb-4 flex items-start justify-between">
              <div>
                <p className="text-sm font-medium text-subtext-light dark:text-subtext-dark">New Companies</p>
                <h3 className="mt-1 text-2xl font-bold text-text-light dark:text-text-dark">
                  {fmtK(kpis?.new_companies_7d)}
                </h3>
              </div>
              <div className="rounded-lg bg-orange-50 p-2 text-orange-600 dark:bg-orange-900/20 dark:text-orange-400">
                <span className="material-symbols-outlined">domain</span>
              </div>
            </div>
            <div className="text-xs text-subtext-light dark:text-subtext-dark">First seen in last 7 days</div>
          </div>

          <div className="rounded-xl border border-border-light bg-surface-light p-5 shadow-sm dark:border-border-dark dark:bg-surface-dark">
            <div className="mb-4 flex items-start justify-between">
              <div>
                <p className="text-sm font-medium text-subtext-light dark:text-subtext-dark">Avg. Salary (MVP)</p>
                <h3 className="mt-1 text-2xl font-bold text-text-light dark:text-text-dark">—</h3>
              </div>
              <div className="rounded-lg bg-green-50 p-2 text-green-600 dark:bg-green-900/20 dark:text-green-400">
                <span className="material-symbols-outlined">payments</span>
              </div>
            </div>
            <div className="text-xs text-subtext-light dark:text-subtext-dark">Salary parsing skipped (for now)</div>
          </div>
        </section>

        <section className="grid grid-cols-1 gap-6 lg:grid-cols-3">
          <div className="rounded-xl border border-border-light bg-surface-light p-6 shadow-sm dark:border-border-dark dark:bg-surface-dark lg:col-span-2">
            <div className="mb-6 flex items-center justify-between">
              <h3 className="text-lg font-semibold text-text-light dark:text-text-dark">Job Market Trend</h3>
              <span className="rounded-lg border border-border-light bg-background-light px-2 py-1 text-xs text-subtext-light dark:border-border-dark dark:bg-background-dark dark:text-subtext-dark">
                Last 8 months
              </span>
            </div>
            <div className="h-64">
              <TrendChart data={trendChart} />
            </div>
          </div>

          <div className="rounded-xl border border-border-light bg-surface-light p-6 shadow-sm dark:border-border-dark dark:bg-surface-dark">
            <h3 className="mb-4 text-lg font-semibold text-text-light dark:text-text-dark">Top Skills Demand</h3>
            <div className="space-y-4">
              {skills.length === 0 ? (
                <div className="text-sm text-subtext-light dark:text-subtext-dark">No skill data yet.</div>
              ) : (
                skills.map((s) => (
                  <div key={s.skill}>
                    <div className="mb-1 flex justify-between text-sm">
                      <span className="font-medium text-text-light dark:text-text-dark">{s.skill}</span>
                      <span className="text-subtext-light dark:text-subtext-dark">
                        {Math.round((100 * s.jobs_unique) / maxSkill)}%
                      </span>
                    </div>
                    <div className="h-2 w-full rounded-full bg-gray-100 dark:bg-gray-700">
                      <div
                        className="h-2 rounded-full bg-primary"
                        style={{ width: `${Math.round((100 * s.jobs_unique) / maxSkill)}%` }}
                      />
                    </div>
                  </div>
                ))
              )}
            </div>
            <div className="mt-6 border-t border-border-light pt-4 text-center dark:border-border-dark">
              <span className="text-sm font-medium text-primary">90d window</span>
            </div>
          </div>
        </section>

        <section className="grid grid-cols-1 gap-6 md:grid-cols-2">
          <div className="rounded-xl border border-border-light bg-surface-light p-6 shadow-sm dark:border-border-dark dark:bg-surface-dark">
            <div className="mb-4 flex items-center justify-between">
              <h3 className="text-lg font-semibold text-text-light dark:text-text-dark">Recent Hotspots</h3>
              <Link href="/map" className="text-xs font-bold text-primary hover:text-primary-hover">
                View Map
              </Link>
            </div>
            <div className="space-y-3">
              {hotspots.map((h) => (
                <Link
                  key={`${h.lat}:${h.lon}:${h.map_city_label}`}
                  href="/map"
                  className="flex items-center gap-3 rounded-lg p-3 transition-colors hover:bg-gray-50 dark:hover:bg-gray-800"
                >
                  <div className="grid h-10 w-10 place-items-center rounded-lg bg-teal-50 text-teal-600 dark:bg-teal-900/20">
                    <span className="material-symbols-outlined">location_city</span>
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-sm font-semibold">{h.map_city_label}</div>
                    <div className="text-xs text-subtext-light dark:text-subtext-dark">
                      {h.new_7d_unique} new listings this week
                    </div>
                  </div>
                  <span
                    className={cx(
                      "rounded px-2 py-1 text-xs font-medium",
                      h.new_7d_unique >= 25
                        ? "bg-green-50 text-green-600 dark:bg-green-900/20 dark:text-green-400"
                        : "bg-gray-100 text-subtext-light dark:bg-gray-800 dark:text-subtext-dark"
                    )}
                  >
                    +{Math.min(99, Math.round((h.new_7d_unique / Math.max(1, kpis?.total_active_jobs ?? 1)) * 100))}%
                  </span>
                </Link>
              ))}
            </div>
          </div>

          <div className="rounded-xl border border-border-light bg-surface-light p-6 shadow-sm dark:border-border-dark dark:bg-surface-dark">
            <div className="mb-4 flex items-center justify-between">
              <h3 className="text-lg font-semibold text-text-light dark:text-text-dark">Saved Searches (UI-only)</h3>
              <Link href="/alerts" className="text-xs font-bold text-primary hover:text-primary-hover">
                Manage
              </Link>
            </div>
            <div className="space-y-4">
              <div className="rounded-lg border border-border-light p-4 dark:border-border-dark">
                <div className="flex items-start justify-between">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-semibold">&quot;Data Engineer&quot; in Berlin</div>
                    <div className="mt-1 text-xs text-subtext-light dark:text-subtext-dark">12 new matches today</div>
                  </div>
                  <span className="mt-1.5 h-2 w-2 rounded-full bg-green-500" />
                </div>
                <div className="mt-3 flex gap-2">
                  <span className="rounded bg-gray-100 px-2 py-1 text-[10px] text-subtext-light dark:bg-gray-700 dark:text-subtext-dark">
                    Hybrid
                  </span>
                  <span className="rounded bg-gray-100 px-2 py-1 text-[10px] text-subtext-light dark:bg-gray-700 dark:text-subtext-dark">
                    Python
                  </span>
                </div>
              </div>

              <div className="rounded-lg border border-border-light p-4 dark:border-border-dark">
                <div className="flex items-start justify-between">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-semibold">&quot;Remote&quot; (Germany)</div>
                    <div className="mt-1 text-xs text-subtext-light dark:text-subtext-dark">3 new matches today</div>
                  </div>
                  <span className="mt-1.5 h-2 w-2 rounded-full bg-green-500" />
                </div>
                <div className="mt-3 flex gap-2">
                  <span className="rounded bg-gray-100 px-2 py-1 text-[10px] text-subtext-light dark:bg-gray-700 dark:text-subtext-dark">
                    Remote
                  </span>
                  <span className="rounded bg-gray-100 px-2 py-1 text-[10px] text-subtext-light dark:bg-gray-700 dark:text-subtext-dark">
                    ETL
                  </span>
                </div>
              </div>
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}

