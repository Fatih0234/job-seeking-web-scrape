"use client";

import dynamic from "next/dynamic";
import Link from "next/link";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ThemeToggle from "@/components/chrome/ThemeToggle";
import type { CityBubble, JobDetail, MapPoint } from "@/lib/apiTypes";
import { formatDistanceToNowStrict } from "date-fns";

const MapCanvas = dynamic(() => import("./MapCanvas"), { ssr: false });
type CityJobsState = "idle" | "loading" | "loaded" | "empty-mismatch" | "error";
type Timeframe = "24h" | "7d" | "30d" | "all";

const TIMEFRAME_OPTIONS: Array<{ value: Timeframe; label: string }> = [
  { value: "24h", label: "Last 24 Hours" },
  { value: "7d", label: "Last 7 Days" },
  { value: "30d", label: "Last Month" },
  { value: "all", label: "All Jobs" },
];

function cx(...parts: Array<string | false | null | undefined>) {
  return parts.filter(Boolean).join(" ");
}

function platformAccent(platform: string) {
  if (platform === "linkedin") return "border-linkedin hover:border-linkedin";
  if (platform === "xing") return "border-xing hover:border-xing";
  if (platform === "stepstone") return "border-stepstone hover:border-stepstone";
  return "border-border-light dark:border-border-dark";
}

function platformEmoji(platform: string) {
  if (platform === "linkedin") return "💼";
  if (platform === "xing") return "✖️";
  if (platform === "stepstone") return "👟";
  return "🔎";
}

function safeRelTime(iso: string | null | undefined) {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return formatDistanceToNowStrict(d, { addSuffix: true });
}

function flattenSkills(extracted: unknown): string[] {
  if (!extracted || typeof extracted !== "object") return [];
  const obj = extracted as Record<string, unknown>;
  const out: string[] = [];
  for (const v of Object.values(obj)) {
    if (Array.isArray(v)) {
      for (const s of v) if (typeof s === "string" && s.trim()) out.push(s.trim());
    }
  }
  return Array.from(new Set(out)).slice(0, 10);
}

function timeframeLabel(timeframe: Timeframe) {
  if (timeframe === "24h") return "Last 24 Hours";
  if (timeframe === "7d") return "Last 7 Days";
  if (timeframe === "30d") return "Last Month";
  return "All Jobs";
}

export default function MapPage() {
  const [timeframe, setTimeframe] = useState<Timeframe>("7d");
  const [timeframeMenuOpen, setTimeframeMenuOpen] = useState(false);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const lastBboxRef = useRef<{ nelat: number; nelon: number; swlat: number; swlon: number } | null>(null);
  const cityJobsRequestRef = useRef(0);
  const timeframeMenuRef = useRef<HTMLDivElement | null>(null);

  const [bubbles, setBubbles] = useState<CityBubble[]>([]);
  const [activeBubble, setActiveBubble] = useState<CityBubble | null>(null);
  const [cityJobs, setCityJobs] = useState<MapPoint[]>([]);
  const [cityJobsState, setCityJobsState] = useState<CityJobsState>("idle");

  const [remoteOn, setRemoteOn] = useState(false);
  const [remotePoints, setRemotePoints] = useState<MapPoint[]>([]);

  const [selected, setSelected] = useState<{ platform: string; job_id: string } | null>(null);
  const [selectedDetail, setSelectedDetail] = useState<JobDetail | null>(null);
  const [modalOpen, setModalOpen] = useState(false);

  const bubbleMetricField =
    timeframe === "24h" ? "new_24h_unique" : timeframe === "7d" ? "new_7d_unique" : timeframe === "30d" ? "new_30d_unique" : "jobs_unique";

  useEffect(() => {
    function onPointerDown(event: PointerEvent) {
      if (!timeframeMenuRef.current?.contains(event.target as Node)) {
        setTimeframeMenuOpen(false);
      }
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") setTimeframeMenuOpen(false);
    }

    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, []);

  const dedupedCityJobs = useMemo(() => {
    const seen = new Set<string>();
    const out: MapPoint[] = [];
    for (const row of cityJobs) {
      const k = `${row.platform}:${row.job_id}`;
      if (seen.has(k)) continue;
      seen.add(k);
      out.push(row);
    }
    return out;
  }, [cityJobs]);

  const fetchBubbles = useCallback(
    async (bbox: { nelat: number; nelon: number; swlat: number; swlon: number }) => {
      const qs = new URLSearchParams({
        nelat: String(bbox.nelat),
        nelon: String(bbox.nelon),
        swlat: String(bbox.swlat),
        swlon: String(bbox.swlon),
        days: timeframe,
      });
      const res = await fetch(`/api/map/bubbles?${qs.toString()}`);
      if (!res.ok) return;
      const json = (await res.json()) as { rows: CityBubble[] };
      const rows = json.rows ?? [];
      setBubbles(rows);
      setActiveBubble((current) => {
        if (!current) return current;
        return (
          rows.find(
            (row) =>
              row.lat === current.lat && row.lon === current.lon && row.map_city_label === current.map_city_label
          ) ?? current
        );
      });
    },
    [timeframe]
  );

  const fetchCityJobs = useCallback(
    async (bubble: CityBubble, timeframeWindow: Timeframe = timeframe) => {
      const requestId = ++cityJobsRequestRef.current;
      const expectedCount =
        timeframeWindow === "24h"
          ? bubble.new_24h_unique
          : timeframeWindow === "7d"
            ? bubble.new_7d_unique
            : timeframeWindow === "30d"
              ? (bubble.new_30d_unique ?? bubble.jobs_unique)
              : bubble.jobs_unique;

      setCityJobs([]);
      setCityJobsState("loading");

      try {
        const qs = new URLSearchParams({
          lat: String(bubble.lat),
          lon: String(bubble.lon),
          days: timeframeWindow,
          limit: "80",
        });
        const res = await fetch(`/api/map/city-jobs?${qs.toString()}`);
        if (requestId !== cityJobsRequestRef.current) return;
        if (!res.ok) {
          setCityJobsState("error");
          return;
        }
        const json = (await res.json()) as { rows: MapPoint[] };
        const rows = json.rows ?? [];
        setCityJobs(rows);
        setCityJobsState(rows.length === 0 && expectedCount > 0 ? "empty-mismatch" : "loaded");
      } catch {
        if (requestId !== cityJobsRequestRef.current) return;
        setCityJobsState("error");
      }
    },
    [timeframe]
  );

  const fetchRemote = useCallback(
    async (bbox: { nelat: number; nelon: number; swlat: number; swlon: number }) => {
      const qs = new URLSearchParams({
        nelat: String(bbox.nelat),
        nelon: String(bbox.nelon),
        swlat: String(bbox.swlat),
        swlon: String(bbox.swlon),
        days: timeframe,
        limit: "500",
      });
      const res = await fetch(`/api/map/remote?${qs.toString()}`);
      if (!res.ok) return;
      const json = (await res.json()) as { rows: MapPoint[] };
      setRemotePoints(json.rows ?? []);
    },
    [timeframe]
  );

  useEffect(() => {
    if (!lastBboxRef.current) return;
    void fetchBubbles(lastBboxRef.current);
    if (remoteOn) void fetchRemote(lastBboxRef.current);
  }, [fetchBubbles, fetchRemote, remoteOn, timeframe]);

  const openJob = useCallback(async (platform: string, job_id: string) => {
    setSelected({ platform, job_id });
    setModalOpen(true);
    setSelectedDetail(null);
    const res = await fetch(`/api/jobs/${encodeURIComponent(platform)}/${encodeURIComponent(job_id)}`);
    if (!res.ok) return;
    const json = (await res.json()) as { job: JobDetail };
    setSelectedDetail(json.job);
  }, []);

  const closeModal = useCallback(() => {
    setModalOpen(false);
    setSelectedDetail(null);
    setSelected(null);
  }, []);

  const applyTimeframe = useCallback(
    (nextTimeframe: Timeframe) => {
      setTimeframeMenuOpen(false);
      setTimeframe(nextTimeframe);
      if (activeBubble) void fetchCityJobs(activeBubble, nextTimeframe);
    },
    [activeBubble, fetchCityJobs]
  );

  const activeBubbleCount = useMemo(() => {
    if (!activeBubble) return 0;
    if (bubbleMetricField === "new_24h_unique") return activeBubble.new_24h_unique;
    if (bubbleMetricField === "new_7d_unique") return activeBubble.new_7d_unique;
    if (bubbleMetricField === "new_30d_unique") return activeBubble.new_30d_unique ?? activeBubble.jobs_unique;
    return activeBubble.jobs_unique;
  }, [activeBubble, bubbleMetricField]);

  const resultsSubtitle = useMemo(() => {
    if (!activeBubble) return "Pick a city bubble";
    if (cityJobsState === "loading") return `Loading jobs in ${activeBubble.map_city_label}`;
    if (cityJobsState === "error") return `Couldn't load jobs in ${activeBubble.map_city_label}`;
    return `Showing ${dedupedCityJobs.length} jobs in ${activeBubble.map_city_label} for ${timeframeLabel(timeframe).toLowerCase()}`;
  }, [activeBubble, cityJobsState, dedupedCityJobs.length, timeframe]);

  const emptyState = useMemo(() => {
    if (!activeBubble || cityJobsState === "idle") {
      return {
        title: "Click a city bubble to load jobs.",
        body: "Remote jobs are a separate toggle on the map.",
      };
    }
    if (cityJobsState === "loading") {
      return {
        title: `Loading jobs for ${activeBubble.map_city_label}...`,
        body: `Fetching ${timeframeLabel(timeframe).toLowerCase()} results for the selected city.`,
      };
    }
    if (cityJobsState === "error") {
      return {
        title: `Couldn't load jobs for ${activeBubble.map_city_label}.`,
        body: "Try clicking the bubble again. If this keeps happening, check the map API response.",
      };
    }
    if (cityJobsState === "empty-mismatch") {
      return {
        title: `No jobs came back for ${activeBubble.map_city_label}.`,
        body: `This bubble still shows ${activeBubbleCount} jobs, so the dashboard read models may be stale. Refresh them and try again.`,
      };
    }
    return {
      title: `No jobs matched the current timeframe in ${activeBubble.map_city_label}.`,
      body: "Try a different timeframe or pick another city bubble.",
    };
  }, [activeBubble, activeBubbleCount, cityJobsState, timeframe]);

  return (
    <div className="flex h-full min-w-0 flex-1 flex-col">
      <header className="z-20 flex h-16 shrink-0 items-center justify-between border-b border-border-light bg-surface-light px-6 shadow-sm dark:border-border-dark dark:bg-surface-dark">
        <div className="flex min-w-0 items-center gap-4">
          <div className="bg-background-light p-1 rounded-lg flex border border-border-light dark:bg-background-dark dark:border-border-dark">
            <Link
              href="/map"
              className="px-4 py-1.5 rounded-md text-sm font-semibold bg-surface-light shadow-sm text-primary dark:bg-surface-dark"
            >
              Map
            </Link>
            <Link
              href="/analytics"
              className="px-4 py-1.5 rounded-md text-sm font-medium text-subtext-light hover:text-text-light dark:text-subtext-dark dark:hover:text-text-dark"
            >
              Analytics
            </Link>
          </div>

          <span className="hidden h-6 w-px bg-border-light dark:bg-border-dark md:block" />

          <div className="hidden items-center gap-3 md:flex">
            <div ref={timeframeMenuRef} className="relative">
              <button
                type="button"
                aria-expanded={timeframeMenuOpen}
                aria-haspopup="menu"
                className="flex items-center gap-2 rounded-md border border-border-light px-3 py-1.5 text-sm text-subtext-light transition-colors hover:bg-gray-50 hover:text-text-light dark:border-border-dark dark:text-subtext-dark dark:hover:bg-gray-800 dark:hover:text-text-dark"
                onClick={() => setTimeframeMenuOpen((open) => !open)}
              >
                <span className="material-symbols-outlined text-lg">calendar_today</span>
                <span>{timeframeLabel(timeframe)}</span>
                <span className="material-symbols-outlined text-lg">
                  {timeframeMenuOpen ? "arrow_drop_up" : "arrow_drop_down"}
                </span>
              </button>
              {timeframeMenuOpen ? (
                <div
                  className="absolute left-0 top-full z-30 mt-2 w-64 rounded-xl border border-border-light bg-surface-light p-2 shadow-xl dark:border-border-dark dark:bg-surface-dark"
                  role="menu"
                >
                  {TIMEFRAME_OPTIONS.map((option) => {
                    const selectedOption = option.value === timeframe;
                    return (
                      <button
                        key={option.value}
                        type="button"
                        role="menuitemradio"
                        aria-checked={selectedOption}
                        className={cx(
                          "flex w-full items-start justify-between rounded-lg px-3 py-2 text-left transition-colors",
                          selectedOption
                            ? "bg-blue-50 text-primary dark:bg-blue-900/20"
                            : "text-subtext-light hover:bg-gray-50 hover:text-text-light dark:text-subtext-dark dark:hover:bg-gray-800 dark:hover:text-text-dark"
                        )}
                        onClick={() => applyTimeframe(option.value)}
                      >
                        <span className="text-sm font-medium">{option.label}</span>
                        {selectedOption ? (
                          <span className="material-symbols-outlined text-lg text-primary">check</span>
                        ) : null}
                      </button>
                    );
                  })}
                </div>
              ) : null}
            </div>

            <div className="relative hidden md:block">
              <span className="material-symbols-outlined absolute left-2.5 top-2 text-lg text-subtext-light dark:text-subtext-dark">
                search
              </span>
              <input
                className="w-64 rounded-md border border-border-light bg-background-light py-1.5 pl-9 pr-3 text-sm text-text-light placeholder:text-subtext-light focus:border-primary focus:ring-primary dark:border-border-dark dark:bg-background-dark dark:text-text-dark dark:placeholder:text-subtext-dark"
                placeholder="Search location or keyword..."
                type="text"
                disabled
              />
            </div>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            className="relative grid h-9 w-9 place-items-center rounded-full text-subtext-light transition-colors hover:bg-gray-100 hover:text-text-light dark:text-subtext-dark dark:hover:bg-gray-800 dark:hover:text-text-dark"
            aria-label="Notifications"
          >
            <span className="material-symbols-outlined text-[20px]">notifications_none</span>
            <span className="absolute right-2 top-2 h-2 w-2 rounded-full bg-red-500 ring-2 ring-surface-light dark:ring-surface-dark" />
          </button>
          <ThemeToggle />
        </div>
      </header>

      <div className="relative flex min-h-0 flex-1 overflow-hidden">
        <div
          className={cx(
            "z-10 h-full w-full bg-surface-light shadow-lg border-r border-border-light dark:bg-surface-dark dark:border-border-dark sm:relative sm:w-80 lg:w-96",
            sidebarOpen ? "absolute sm:static" : "absolute -translate-x-full sm:static sm:w-0 sm:border-r-0",
            "transform transition-transform duration-300 ease-in-out"
          )}
        >
          <div className="flex items-center justify-between border-b border-border-light bg-surface-light p-4 dark:border-border-dark dark:bg-surface-dark">
            <div className="min-w-0">
              <h2 className="truncate font-semibold text-text-light dark:text-text-dark">Search Results</h2>
              <p className="text-xs text-subtext-light dark:text-subtext-dark">{resultsSubtitle}</p>
            </div>
            <span className="rounded-full border border-border-light bg-background-light px-3 py-1 text-[11px] font-medium text-subtext-light dark:border-border-dark dark:bg-background-dark dark:text-subtext-dark">
              {timeframeLabel(timeframe)}
            </span>
          </div>

          <div className="gw-scrollbar h-full overflow-y-auto bg-background-light p-3 dark:bg-background-dark/50">
            {dedupedCityJobs.length === 0 ? (
              <div className="rounded-xl border border-border-light bg-surface-light p-4 text-sm text-subtext-light dark:border-border-dark dark:bg-surface-dark dark:text-subtext-dark">
                <p className="font-medium text-text-light dark:text-text-dark">{emptyState.title}</p>
                <p className="mt-1">{emptyState.body}</p>
              </div>
            ) : (
              <div className="space-y-3">
                {dedupedCityJobs.map((job) => {
                  const rel = safeRelTime(job.posted_at_utc ?? job.first_seen_at);
                  const skills = flattenSkills(job.extracted_skills);
                  return (
                    <button
                      key={`${job.platform}:${job.job_id}`}
                      type="button"
                      onClick={() => openJob(job.platform, job.job_id)}
                      className={cx(
                        "group relative w-full overflow-hidden rounded-xl border bg-white p-4 text-left shadow-sm transition-all hover:shadow-md dark:bg-surface-dark",
                        platformAccent(job.platform)
                      )}
                    >
                      <div
                        className={cx(
                          "absolute left-0 top-0 bottom-0 w-1 opacity-100",
                          job.platform === "linkedin" && "bg-linkedin",
                          job.platform === "xing" && "bg-xing",
                          job.platform === "stepstone" && "bg-stepstone"
                        )}
                      />
                      <div className="mb-2 flex items-start justify-between gap-3 pl-2">
                        <h3 className="line-clamp-2 text-sm font-semibold text-text-light transition-colors group-hover:text-primary dark:text-text-dark">
                          {job.job_title ?? "Untitled role"}
                        </h3>
                        <span className="shrink-0 rounded-full border border-border-light bg-gray-100 px-2 py-0.5 text-[10px] text-subtext-light dark:border-border-dark dark:bg-gray-800 dark:text-subtext-dark">
                          {rel ?? "recent"}
                        </span>
                      </div>
                      <p className="mb-1 flex items-center gap-1 pl-2 text-xs font-medium text-subtext-light dark:text-subtext-dark">
                        <span className="text-sm">{platformEmoji(job.platform)}</span>
                        <span className="truncate">{job.company_name ?? "Unknown company"}</span>
                      </p>
                      <div className="mb-3 flex items-center gap-1 pl-2 text-xs text-subtext-light dark:text-subtext-dark">
                        <span className="material-symbols-outlined text-[14px]">location_on</span>
                        <span className="truncate">{job.job_location ?? job.map_city_label}</span>
                      </div>
                      {skills.length > 0 ? (
                        <div className="flex flex-wrap gap-2 pl-2">
                          {skills.slice(0, 5).map((s) => (
                            <span
                              key={s}
                              className="rounded border border-blue-100 bg-blue-50 px-2 py-0.5 text-[10px] text-blue-600 dark:border-blue-900/40 dark:bg-blue-900/20 dark:text-blue-300"
                            >
                              {s}
                            </span>
                          ))}
                        </div>
                      ) : null}
                    </button>
                  );
                })}
              </div>
            )}
            <div className="h-24" />
          </div>

          <div className="border-t border-border-light bg-surface-light p-3 dark:border-border-dark dark:bg-surface-dark">
            <button
              type="button"
              className="w-full rounded-lg py-2 text-sm font-medium text-primary transition-colors hover:bg-blue-50 dark:hover:bg-blue-900/20"
              onClick={() => setSidebarOpen(false)}
            >
              Hide list
            </button>
          </div>
        </div>

        <div className="relative min-w-0 flex-1">
          <MapCanvas
            bubbles={bubbles}
            bubbleMetricField={bubbleMetricField}
            remoteOn={remoteOn}
            remotePoints={remotePoints}
            onBoundsChanged={(bbox) => {
              lastBboxRef.current = bbox;
              void fetchBubbles(bbox);
              if (remoteOn) void fetchRemote(bbox);
            }}
            onBubbleClicked={(b) => {
              setActiveBubble(b);
              void fetchCityJobs(b);
              setSidebarOpen(true);
            }}
          />

          <div className="absolute top-4 left-4 z-[500]">
            <button
              type="button"
              className="flex items-center gap-2 rounded-lg border border-border-light bg-surface-light px-3 py-2 text-xs font-semibold text-subtext-light shadow-lg transition-colors hover:bg-gray-50 hover:text-primary dark:border-border-dark dark:bg-surface-dark dark:text-subtext-dark dark:hover:bg-gray-800"
              onClick={() => setSidebarOpen((v) => !v)}
            >
              <span className="material-symbols-outlined">first_page</span>
              <span className="hidden sm:inline">{sidebarOpen ? "Hide list" : "Show list"}</span>
            </button>
          </div>

          <div className="absolute top-4 right-4 z-[500] w-48 space-y-3 rounded-xl border border-border-light bg-surface-light/95 p-4 text-xs shadow-xl backdrop-blur-md dark:border-border-dark dark:bg-surface-dark/95">
            <div className="border-b border-border-light pb-2 font-semibold text-text-light dark:border-border-dark dark:text-text-dark">
              Map Legend
            </div>
            <div className="flex items-center justify-between">
              <span className="text-subtext-light dark:text-subtext-dark">Remote layer</span>
              <button
                type="button"
                className={cx(
                  "relative inline-flex h-6 w-11 items-center rounded-full transition-colors",
                  remoteOn ? "bg-primary" : "bg-gray-200 dark:bg-gray-700"
                )}
                onClick={() => {
                  setRemoteOn((v) => {
                    const next = !v;
                    if (next && lastBboxRef.current) void fetchRemote(lastBboxRef.current);
                    return next;
                  });
                }}
                aria-label="Toggle remote layer"
              >
                <span
                  className={cx(
                    "inline-block h-5 w-5 transform rounded-full bg-white transition-transform",
                    remoteOn ? "translate-x-5" : "translate-x-1"
                  )}
                />
              </button>
            </div>
            <div className="h-px w-full bg-border-light dark:bg-border-dark" />
            <div className="flex items-center gap-2">
              <span className="text-base">💼</span>
              <span>LinkedIn</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-base">✖️</span>
              <span>XING</span>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-base">👟</span>
              <span>StepStone</span>
            </div>
          </div>

          <div className="absolute bottom-6 right-6 z-[500] flex flex-col gap-2">
            <button
              type="button"
              className="grid h-10 w-10 place-items-center rounded-lg border border-border-light bg-surface-light text-subtext-light shadow-xl transition-colors hover:bg-gray-100 hover:text-primary dark:border-border-dark dark:bg-surface-dark dark:text-subtext-dark dark:hover:bg-gray-800"
              onClick={() => {
                /* map handles */
              }}
              aria-label="My location"
              disabled
            >
              <span className="material-symbols-outlined text-xl">my_location</span>
            </button>
          </div>
        </div>
      </div>

      {modalOpen ? (
        <div className="fixed inset-0 z-[999] flex items-center justify-center bg-primary/20 p-4 backdrop-blur-sm sm:p-6 lg:p-8">
          <div className="flex h-[90vh] w-full max-w-5xl flex-col overflow-hidden rounded-2xl border border-white/50 bg-white shadow-[0_20px_40px_-12px_rgba(0,0,0,0.25)] ring-1 ring-black/5 dark:bg-surface-dark">
            <div className="flex items-center justify-between border-b border-border-light bg-white px-6 py-4 dark:border-border-dark dark:bg-surface-dark">
              <div className="flex items-center gap-3">
                <button
                  type="button"
                  className="flex items-center text-subtext-light hover:text-text-light dark:text-subtext-dark dark:hover:text-text-dark"
                  onClick={closeModal}
                >
                  <span className="material-symbols-outlined mr-1 text-[20px]">arrow_back</span>
                  <span className="text-xs font-semibold uppercase tracking-wide">Back to Map</span>
                </button>
                <div className="mx-2 h-4 w-px bg-border-light dark:bg-border-dark" />
                <span className="rounded border border-border-light bg-gray-50 px-2 py-0.5 font-mono text-xs text-subtext-light dark:border-border-dark dark:bg-gray-800 dark:text-subtext-dark">
                  ID: {selected ? `${selected.platform}:${selected.job_id}` : "—"}
                </span>
              </div>
              <div className="flex items-center gap-3">
                <button
                  type="button"
                  className="hidden h-10 items-center justify-center gap-2 rounded-lg border border-border-light bg-white px-3 text-sm font-medium text-subtext-light transition-colors hover:bg-gray-50 hover:text-text-light dark:border-border-dark dark:bg-surface-dark dark:text-subtext-dark dark:hover:bg-gray-800 sm:flex"
                  disabled
                >
                  <span className="material-symbols-outlined text-[20px]">folder_open</span>
                  <span>Save</span>
                  <span className="material-symbols-outlined text-[16px]">expand_more</span>
                </button>
                <a
                  href={selectedDetail?.job_url ?? "#"}
                  target="_blank"
                  rel="noreferrer"
                  className={cx(
                    "flex h-10 items-center gap-2 rounded-lg bg-linkedin px-6 text-sm font-semibold text-white shadow-lg shadow-blue-900/10 transition-all hover:-translate-y-0.5",
                    !selectedDetail?.job_url && "pointer-events-none opacity-50"
                  )}
                >
                  <span>Apply Now</span>
                  <span className="material-symbols-outlined text-[16px]">open_in_new</span>
                </a>
                <div className="mx-1 h-6 w-px bg-border-light dark:bg-border-dark" />
                <button
                  type="button"
                  aria-label="Close modal"
                  className="grid h-8 w-8 place-items-center rounded-full text-subtext-light hover:bg-gray-100 dark:text-subtext-dark dark:hover:bg-gray-800"
                  onClick={closeModal}
                >
                  <span className="material-symbols-outlined text-[20px]">close</span>
                </button>
              </div>
            </div>

            <div className="gw-scrollbar flex-1 overflow-y-auto bg-gray-50/50 dark:bg-background-dark/40">
              <div className="mx-auto max-w-5xl">
                <div className="border-b border-border-light bg-white p-8 pb-10 dark:border-border-dark dark:bg-surface-dark">
                  <div className="flex flex-col justify-between gap-6 lg:flex-row lg:items-start">
                    <div className="flex gap-6">
                      <div className="grid h-20 w-20 place-items-center rounded-xl border border-border-light bg-white p-4 shadow-sm dark:border-border-dark dark:bg-background-dark">
                        <span className="text-2xl font-black text-primary">G</span>
                      </div>
                      <div className="flex flex-col gap-2 pt-1">
                        <h1 className="text-2xl font-bold tracking-tight text-text-light dark:text-text-dark">
                          {selectedDetail?.job_title ?? "Loading…"}
                        </h1>
                        <div className="flex flex-wrap items-center gap-3 text-sm text-text-light dark:text-text-dark">
                          <div className="flex items-center gap-1.5 font-semibold">
                            <span className="material-symbols-outlined text-[18px] text-subtext-light dark:text-subtext-dark">
                              business
                            </span>
                            <span>{selectedDetail?.company_name ?? "—"}</span>
                          </div>
                          <span className="text-gray-300">|</span>
                          <div className="flex items-center gap-1.5">
                            <span className="material-symbols-outlined text-[18px] text-subtext-light dark:text-subtext-dark">
                              location_on
                            </span>
                            <span>{selectedDetail?.job_location ?? "—"}</span>
                          </div>
                        </div>
                        <div className="mt-3 flex items-center gap-4">
                          <div className="flex items-center gap-1.5 rounded border border-gray-200 bg-gray-50 px-2 py-1 text-xs font-medium text-subtext-light dark:border-border-dark dark:bg-gray-800 dark:text-subtext-dark">
                            <span className="material-symbols-outlined text-[16px]">schedule</span>
                            <span>
                              Posted{" "}
                              {safeRelTime(selectedDetail?.posted_at_utc ?? selectedDetail?.first_seen_at) ?? "recently"}
                            </span>
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="shrink-0 rounded-xl border border-border-light bg-white p-3 shadow-sm dark:border-border-dark dark:bg-surface-dark">
                      <div className="grid h-16 w-16 place-items-center">
                        <div className="grid h-14 w-14 place-items-center rounded-full border-[3px] border-green-500/30">
                          <div className="grid h-11 w-11 place-items-center rounded-full border-[3px] border-green-600">
                            <span className="text-sm font-bold text-green-600">—</span>
                          </div>
                        </div>
                      </div>
                      <div className="text-center text-[10px] font-bold uppercase tracking-wider text-subtext-light dark:text-subtext-dark">
                        Match Score
                      </div>
                    </div>
                  </div>
                </div>

                <div className="grid min-h-[500px] grid-cols-1 gap-0 lg:grid-cols-12">
                  <div className="space-y-8 bg-white p-8 dark:bg-surface-dark lg:col-span-7">
                    <div className="space-y-3">
                      <h3 className="flex items-center gap-2 text-xs font-bold uppercase tracking-wider text-subtext-light dark:text-subtext-dark">
                        <span className="material-symbols-outlined text-[16px]">code</span>
                        Tech Stack & Skills
                      </h3>
                      <div className="flex flex-wrap gap-2">
                        {flattenSkills(selectedDetail?.extracted_skills).map((s) => (
                          <span
                            key={s}
                            className="cursor-default rounded-md border border-border-light bg-gray-50 px-3 py-1.5 text-sm font-medium text-text-light dark:border-border-dark dark:bg-gray-800 dark:text-text-dark"
                          >
                            {s}
                          </span>
                        ))}
                        {flattenSkills(selectedDetail?.extracted_skills).length === 0 ? (
                          <span className="text-sm text-subtext-light dark:text-subtext-dark">No extracted skills yet.</span>
                        ) : null}
                      </div>
                    </div>

                    <div className="prose prose-sm max-w-none prose-slate dark:prose-invert">
                      <h3 className="mb-3 flex items-center gap-2 text-sm font-bold uppercase tracking-wide">
                        <span className="material-symbols-outlined text-[16px]">description</span>
                        About the Role
                      </h3>
                      <p className="whitespace-pre-line leading-relaxed">
                        {selectedDetail?.job_description ?? "Loading description…"}
                      </p>
                    </div>
                  </div>

                  <div className="space-y-6 border-l border-border-light bg-gray-50/50 p-8 dark:border-border-dark dark:bg-background-dark/30 lg:col-span-5">
                    <div className="rounded-xl border border-border-light bg-white p-4 text-sm text-subtext-light shadow-sm dark:border-border-dark dark:bg-surface-dark dark:text-subtext-dark">
                      Commute analysis is UI-only for now (needs your home location + routing API).
                    </div>
                    <div className="rounded-xl border border-border-light bg-white p-4 text-sm text-subtext-light shadow-sm dark:border-border-dark dark:bg-surface-dark dark:text-subtext-dark">
                      Similar roles will be powered later (skill/title similarity).
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
