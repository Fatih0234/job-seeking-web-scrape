"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV = [
  { href: "/map", icon: "map", label: "Map" },
  { href: "/analytics", icon: "dashboard", label: "Dashboard" },
  { href: "/saved", icon: "bookmark", label: "Saved Jobs" },
  { href: "/alerts", icon: "notifications_active", label: "Alerts" },
  { href: "/settings", icon: "settings", label: "Settings" },
] as const;

function cx(...parts: Array<string | false | null | undefined>) {
  return parts.filter(Boolean).join(" ");
}

export default function AppRail() {
  const pathname = usePathname();
  const active = NAV.find((n) => (pathname || "/").startsWith(n.href))?.href ?? "/map";

  return (
    <aside className="hidden shrink-0 flex-col border-r border-border-light bg-surface-light dark:border-border-dark dark:bg-surface-dark md:flex w-20 z-30">
      <div className="flex h-16 items-center justify-center border-b border-border-light dark:border-border-dark">
        <Link
          href="/map"
          className="group grid h-10 w-10 place-items-center rounded-xl bg-primary text-white shadow-lg shadow-blue-500/20"
          aria-label="GeoWorks"
        >
          <span className="text-lg font-extrabold tracking-tight">G</span>
        </Link>
      </div>

      <nav className="flex flex-1 flex-col items-center gap-4 p-2 pt-6">
        {NAV.map((item) => {
          const isActive = item.href === active;
          return (
            <Link
              key={item.href}
              href={item.href}
              title={item.label}
              className={cx(
                "grid h-12 w-12 place-items-center rounded-xl transition-all",
                isActive
                  ? "bg-blue-50 text-primary shadow-sm dark:bg-blue-900/20"
                  : "text-subtext-light hover:bg-gray-50 hover:text-text-light dark:text-subtext-dark dark:hover:bg-gray-800 dark:hover:text-text-dark"
              )}
            >
              <span className="material-symbols-outlined text-2xl">{item.icon}</span>
            </Link>
          );
        })}
      </nav>

      <div className="border-t border-border-light p-4 dark:border-border-dark">
        <div className="grid h-10 w-10 place-items-center rounded-full border border-border-light bg-surface-light text-subtext-light dark:border-border-dark dark:bg-surface-dark dark:text-subtext-dark">
          <span className="material-symbols-outlined text-xl">person</span>
        </div>
      </div>
    </aside>
  );
}

