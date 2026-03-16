"use client";

import Link from "next/link";
import ThemeToggle from "@/components/chrome/ThemeToggle";

const MOCK = [
  {
    id: "linkedin:abc",
    title: "Senior GIS Data Engineer",
    company: "Hexagon Geospatial",
    location: "Berlin, Remote",
    posted: "2 days ago",
    postedDate: "Oct 24, 2023",
    status: "To Apply",
    skills: ["Python", "PostGIS", "ETL"],
    color: "bg-orange-100 text-orange-600 dark:bg-orange-900/30 dark:text-orange-400",
  },
  {
    id: "xing:def",
    title: "Geospatial Analyst",
    company: "Planet",
    location: "San Francisco, CA",
    posted: "5 days ago",
    postedDate: "Oct 21, 2023",
    status: "Applied",
    skills: ["ArcGIS Pro", "Python", "Remote Sensing"],
    color: "bg-indigo-100 text-indigo-600 dark:bg-indigo-900/30 dark:text-indigo-400",
  },
  {
    id: "stepstone:ghi",
    title: "Cartographer / GIS Specialist",
    company: "ClimateEngine",
    location: "Ottawa, CA",
    posted: "1 week ago",
    postedDate: "Oct 18, 2023",
    status: "To Apply",
    skills: ["Mapbox", "QGIS", "Design"],
    color: "bg-teal-100 text-teal-600 dark:bg-teal-900/30 dark:text-teal-400",
  },
  {
    id: "linkedin:jkl",
    title: "Earth Observation Scientist",
    company: "ESA",
    location: "Frascati, IT",
    posted: "2 weeks ago",
    postedDate: "Oct 12, 2023",
    status: "Interviewing",
    skills: ["SAR", "Python", "SNAP"],
    color: "bg-red-100 text-red-600 dark:bg-red-900/30 dark:text-red-400",
  },
];

export default function SavedJobsPage() {
  return (
    <main className="flex min-h-0 flex-1 flex-col overflow-y-auto bg-background-light dark:bg-background-dark">
      <header className="sticky top-0 z-20 flex h-16 items-center justify-between border-b border-border-light bg-surface-light px-6 dark:border-border-dark dark:bg-surface-dark">
        <div className="flex items-center gap-4">
          <h1 className="text-lg font-semibold text-text-light dark:text-text-dark">My Saved Jobs</h1>
          <span className="h-6 w-px bg-border-light dark:bg-border-dark" />
          <div className="flex items-center gap-2 text-sm text-subtext-light dark:text-subtext-dark">
            <span className="material-symbols-outlined text-lg">folder_open</span>
            <span>12 items</span>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <div className="relative hidden sm:block">
            <span className="material-symbols-outlined absolute left-3 top-2 text-lg text-subtext-light dark:text-subtext-dark">
              search
            </span>
            <input
              className="w-64 rounded-lg border border-border-light bg-background-light py-1.5 pl-9 pr-3 text-sm text-text-light placeholder:text-subtext-light focus:border-primary focus:ring-primary dark:border-border-dark dark:bg-background-dark dark:text-text-dark dark:placeholder:text-subtext-dark"
              placeholder="Search saved jobs..."
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
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex w-full items-center gap-2 overflow-x-auto pb-2 sm:w-auto sm:pb-0">
            {[
              { label: "All Saved", active: true },
              { label: "Data Engineering" },
              { label: "Analytics" },
              { label: "Remote Sensing" },
            ].map((b) => (
              <button
                key={b.label}
                type="button"
                className={
                  b.active
                    ? "whitespace-nowrap rounded-lg bg-primary px-4 py-2 text-sm font-medium text-white shadow-sm"
                    : "whitespace-nowrap rounded-lg border border-border-light bg-surface-light px-4 py-2 text-sm font-medium text-subtext-light transition-colors hover:bg-gray-50 dark:border-border-dark dark:bg-surface-dark dark:text-subtext-dark dark:hover:bg-gray-800"
                }
              >
                {b.label}
              </button>
            ))}
            <button
              type="button"
              className="grid h-10 w-10 place-items-center rounded-lg text-subtext-light transition-colors hover:bg-gray-100 dark:text-subtext-dark dark:hover:bg-gray-800"
              title="Create folder"
              disabled
            >
              <span className="material-symbols-outlined">create_new_folder</span>
            </button>
          </div>

          <div className="ml-auto flex items-center gap-3">
            <div className="flex items-center gap-2 rounded-lg border border-border-light bg-surface-light px-3 py-1.5 text-sm text-subtext-light dark:border-border-dark dark:bg-surface-dark dark:text-subtext-dark">
              <span>Sort by:</span>
              <select className="cursor-pointer bg-transparent p-0 text-sm font-medium text-text-light focus:ring-0 dark:text-text-dark">
                <option>Date Added</option>
                <option>Posted Date</option>
              </select>
            </div>
          </div>
        </div>

        <div className="overflow-hidden rounded-xl border border-border-light bg-surface-light shadow-sm dark:border-border-dark dark:bg-surface-dark">
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-border-light bg-gray-50 text-xs font-semibold uppercase tracking-wider text-subtext-light dark:border-border-dark dark:bg-gray-800/50 dark:text-subtext-dark">
                  <th className="w-12 px-6 py-4">
                    <input type="checkbox" className="rounded border-border-light text-primary" />
                  </th>
                  <th className="w-1/3 px-6 py-4">Job Details</th>
                  <th className="px-6 py-4">Skills</th>
                  <th className="px-6 py-4">Posted</th>
                  <th className="px-6 py-4">Status / Notes</th>
                  <th className="px-6 py-4 text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border-light dark:divide-border-dark">
                {MOCK.map((row) => (
                  <tr key={row.id} className="group transition-colors hover:bg-gray-50 dark:hover:bg-gray-800/50">
                    <td className="px-6 py-4">
                      <input type="checkbox" className="rounded border-border-light text-primary" />
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex items-start gap-4">
                        <div className={`grid h-10 w-10 place-items-center rounded-lg font-bold ${row.color}`}>
                          {row.company.slice(0, 1).toUpperCase()}
                        </div>
                        <div className="min-w-0">
                          <div className="truncate text-sm font-semibold text-text-light hover:text-primary dark:text-text-dark">
                            {row.title}
                          </div>
                          <div className="mt-0.5 truncate text-sm text-subtext-light dark:text-subtext-dark">
                            {row.company}
                          </div>
                          <div className="mt-1.5 flex items-center gap-2 text-xs text-subtext-light dark:text-subtext-dark">
                            <span className="flex items-center gap-1">
                              <span className="material-symbols-outlined text-[14px]">location_on</span>
                              {row.location}
                            </span>
                            <span className="h-1 w-1 rounded-full bg-gray-300" />
                            <span className="text-subtext-light dark:text-subtext-dark">UI-only</span>
                          </div>
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex flex-wrap gap-1.5">
                        {row.skills.map((s) => (
                          <span
                            key={s}
                            className="rounded bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-600 dark:bg-blue-900/20 dark:text-blue-300"
                          >
                            {s}
                          </span>
                        ))}
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <div className="text-sm text-text-light dark:text-text-dark">{row.posted}</div>
                      <div className="mt-0.5 text-xs text-subtext-light dark:text-subtext-dark">{row.postedDate}</div>
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex flex-col gap-2">
                        <select className="w-32 rounded border border-border-light bg-transparent px-2 py-1 text-xs text-text-light focus:border-primary focus:ring-primary dark:border-border-dark dark:text-text-dark">
                          <option>{row.status}</option>
                          <option>To Apply</option>
                          <option>Applied</option>
                          <option>Interviewing</option>
                          <option>Offer</option>
                        </select>
                        <span className="max-w-[180px] truncate text-xs italic text-subtext-light dark:text-subtext-dark">
                          Add note (UI-only)
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-4 text-right">
                      <div className="flex items-center justify-end gap-2 opacity-0 transition-opacity group-hover:opacity-100">
                        <button
                          type="button"
                          className="grid h-8 w-8 place-items-center rounded text-subtext-light hover:bg-gray-100 dark:text-subtext-dark dark:hover:bg-gray-700"
                          title="Edit note"
                          disabled
                        >
                          <span className="material-symbols-outlined text-lg">edit_note</span>
                        </button>
                        <button
                          type="button"
                          className="grid h-8 w-8 place-items-center rounded text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20"
                          title="Remove"
                          disabled
                        >
                          <span className="material-symbols-outlined text-lg">delete</span>
                        </button>
                        <Link
                          href="/map"
                          className="grid h-8 w-8 place-items-center rounded text-primary hover:bg-gray-100 dark:hover:bg-gray-700"
                          title="Open"
                        >
                          <span className="material-symbols-outlined text-lg">open_in_new</span>
                        </Link>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="flex items-center justify-between border-t border-border-light px-6 py-4 text-sm text-subtext-light dark:border-border-dark dark:text-subtext-dark">
            <span>Showing 4 of 12 saved jobs</span>
            <div className="flex gap-2">
              <button
                type="button"
                className="rounded border border-border-light px-3 py-1 text-sm disabled:opacity-50 dark:border-border-dark"
                disabled
              >
                Previous
              </button>
              <button type="button" className="rounded border border-border-light px-3 py-1 text-sm dark:border-border-dark">
                Next
              </button>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-6 md:grid-cols-3">
          {[
            { label: "Applications Sent", value: 8, icon: "send", color: "bg-blue-50 dark:bg-blue-900/10 border-blue-100 dark:border-blue-900/30 text-primary" },
            { label: "Interviews", value: 2, icon: "forum", color: "bg-purple-50 dark:bg-purple-900/10 border-purple-100 dark:border-purple-900/30 text-purple-600 dark:text-purple-400" },
            { label: "Expiring Soon", value: 1, icon: "hourglass_top", color: "bg-orange-50 dark:bg-orange-900/10 border-orange-100 dark:border-orange-900/30 text-orange-600 dark:text-orange-400" },
          ].map((c) => (
            <div key={c.label} className={`flex items-center gap-4 rounded-xl border p-4 ${c.color}`}>
              <div className="grid h-10 w-10 place-items-center rounded-lg bg-white/70 dark:bg-white/10">
                <span className="material-symbols-outlined">{c.icon}</span>
              </div>
              <div>
                <p className="text-xs font-semibold uppercase tracking-wider text-subtext-light dark:text-subtext-dark">
                  {c.label}
                </p>
                <p className="text-lg font-bold text-text-light dark:text-text-dark">{c.value}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </main>
  );
}

