"use client";

import ThemeToggle from "@/components/chrome/ThemeToggle";

export default function AlertsPage() {
  return (
    <main className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-background-light dark:bg-background-dark">
      <header className="sticky top-0 z-20 flex h-16 items-center justify-between border-b border-border-light bg-surface-light px-6 dark:border-border-dark dark:bg-surface-dark">
        <div className="flex items-center gap-4">
          <h1 className="text-xl font-semibold text-text-light dark:text-text-dark">Job Alerts</h1>
          <span className="hidden h-6 w-px bg-border-light dark:bg-border-dark sm:block" />
          <div className="hidden items-center gap-2 text-sm text-subtext-light dark:text-subtext-dark sm:flex">
            <span className="material-symbols-outlined text-lg">location_on</span>
            <span>Europe & Remote</span>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <div className="relative hidden sm:block">
            <span className="material-symbols-outlined absolute left-3 top-2 text-lg text-subtext-light dark:text-subtext-dark">
              search
            </span>
            <input
              className="w-64 rounded-lg border border-border-light bg-background-light py-1.5 pl-9 pr-3 text-sm text-text-light placeholder:text-subtext-light focus:border-primary focus:ring-primary dark:border-border-dark dark:bg-background-dark dark:text-text-dark dark:placeholder:text-subtext-dark"
              placeholder="Search alerts..."
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

      <div className="mx-auto w-full max-w-7xl flex-1 p-6">
        <div className="mb-6 flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <h2 className="text-2xl font-bold text-text-light dark:text-text-dark">Manage Alerts</h2>
            <p className="mt-1 text-sm text-subtext-light dark:text-subtext-dark">
              Configure job notifications and preferences. (UI-only in MVP)
            </p>
          </div>
          <button
            type="button"
            className="inline-flex items-center gap-2 rounded-lg bg-primary px-4 py-2 font-medium text-white shadow-sm transition-colors hover:bg-primary-hover"
            disabled
          >
            <span className="material-symbols-outlined text-sm">add</span>
            Create New Alert
          </button>
        </div>

        <div className="space-y-4">
          {[
            {
              badge: "Daily",
              badgeCls: "bg-blue-100 text-primary border-blue-200 dark:bg-blue-900/30 dark:border-blue-800",
              title: "Data Engineer in Oldenburg",
              chips: ["City: Oldenburg, DE (+20km)", "Title: Data Engineer", "Min: €60k"],
              last: "Last sent: Today, 8:00 AM",
              status: "Active",
              statusCls: "text-green-600",
              enabled: true,
            },
            {
              badge: "Weekly",
              badgeCls:
                "bg-purple-100 text-purple-600 border-purple-200 dark:bg-purple-900/30 dark:border-purple-800 dark:text-purple-400",
              title: "Remote Data Engineering",
              chips: ["Location: Remote (DE)", "Keywords: Python, SQL, Airflow"],
              last: "Last sent: Mon, Oct 24",
              status: "Active",
              statusCls: "text-green-600",
              enabled: true,
            },
            {
              badge: "Instant",
              badgeCls: "bg-gray-100 text-subtext-light border-gray-200 dark:bg-gray-800 dark:border-gray-700 dark:text-subtext-dark",
              title: "Geospatial Analyst (Part-time)",
              chips: ["Location: Berlin, DE", "Type: Part-time"],
              last: "Last sent: 2 days ago",
              status: "Paused",
              statusCls: "text-subtext-light dark:text-subtext-dark",
              enabled: false,
            },
          ].map((a) => (
            <div
              key={a.title}
              className={`rounded-xl border border-border-light bg-surface-light p-5 shadow-sm transition-shadow hover:shadow-md dark:border-border-dark dark:bg-surface-dark ${
                a.enabled ? "" : "opacity-80"
              }`}
            >
              <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                <div className="min-w-0 flex-1">
                  <div className="mb-2 flex items-center gap-3">
                    <span className={`rounded-full border px-2.5 py-0.5 text-xs font-semibold ${a.badgeCls}`}>
                      {a.badge}
                    </span>
                    <h3 className="truncate text-lg font-semibold text-text-light dark:text-text-dark">{a.title}</h3>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {a.chips.map((c) => (
                      <div
                        key={c}
                        className="flex items-center gap-1.5 rounded-md border border-border-light bg-background-light px-3 py-1 text-sm text-subtext-light dark:border-border-dark dark:bg-background-dark dark:text-subtext-dark"
                      >
                        <span className="material-symbols-outlined text-base">tune</span>
                        {c}
                      </div>
                    ))}
                  </div>
                </div>

                <div className="flex items-center gap-6 md:pl-6">
                  <div className="flex items-center gap-3">
                    <span className="text-sm font-medium text-text-light dark:text-text-dark">Email</span>
                    <div className={`relative h-6 w-11 rounded-full ${a.enabled ? "bg-primary" : "bg-gray-200 dark:bg-gray-700"}`}>
                      <div
                        className={`absolute top-[2px] h-5 w-5 rounded-full bg-white transition-transform ${
                          a.enabled ? "translate-x-5" : "translate-x-1"
                        }`}
                      />
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      className="grid h-9 w-9 place-items-center rounded-lg text-subtext-light transition-colors hover:bg-gray-100 dark:text-subtext-dark dark:hover:bg-gray-800"
                      disabled
                      title="Edit"
                    >
                      <span className="material-symbols-outlined">edit</span>
                    </button>
                    <button
                      type="button"
                      className="grid h-9 w-9 place-items-center rounded-lg text-subtext-light transition-colors hover:bg-red-50 hover:text-red-600 dark:text-subtext-dark dark:hover:bg-red-900/20 dark:hover:text-red-400"
                      disabled
                      title="Delete"
                    >
                      <span className="material-symbols-outlined">delete</span>
                    </button>
                  </div>
                </div>
              </div>

              <div className="mt-4 flex items-center justify-between border-t border-border-light pt-4 text-xs text-subtext-light dark:border-border-dark dark:text-subtext-dark">
                <span>{a.last}</span>
                <span className="flex items-center gap-2">
                  <span className={`h-2 w-2 rounded-full ${a.enabled ? "bg-green-500" : "bg-gray-400"}`} />
                  <span className={a.statusCls}>{a.status}</span>
                </span>
              </div>
            </div>
          ))}
        </div>

        <div className="mt-8 flex flex-col items-center justify-between gap-4 rounded-xl border border-blue-100 bg-blue-50 p-6 dark:border-blue-900/20 dark:bg-blue-900/10 md:flex-row">
          <div className="flex items-start gap-4">
            <div className="grid h-12 w-12 place-items-center rounded-lg bg-white text-primary shadow-sm dark:bg-blue-900/30">
              <span className="material-symbols-outlined">lightbulb</span>
            </div>
            <div>
              <h4 className="font-semibold text-text-light dark:text-text-dark">Did you know?</h4>
              <p className="mt-1 text-sm text-subtext-light dark:text-subtext-dark">
                Narrow filters like &quot;Python&quot; + &quot;Berlin&quot; can reduce noise dramatically.
              </p>
            </div>
          </div>
          <button type="button" className="text-sm font-medium text-primary hover:text-primary-hover" disabled>
            View Tips & Tricks
          </button>
        </div>
      </div>
    </main>
  );
}
