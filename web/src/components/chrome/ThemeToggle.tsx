"use client";

import { useEffect, useState } from "react";

function getInitialTheme(): "light" | "dark" {
  if (typeof window === "undefined") return "light";
  const stored = window.localStorage.getItem("gw-theme");
  if (stored === "dark" || stored === "light") return stored;
  return window.matchMedia?.("(prefers-color-scheme: dark)")?.matches ? "dark" : "light";
}

export default function ThemeToggle() {
  const [theme, setTheme] = useState<"light" | "dark">(() => getInitialTheme());

  useEffect(() => {
    const root = document.documentElement;
    if (theme === "dark") root.classList.add("dark");
    else root.classList.remove("dark");
    window.localStorage.setItem("gw-theme", theme);
  }, [theme]);

  return (
    <button
      type="button"
      className="relative grid h-9 w-9 place-items-center rounded-full text-subtext-light transition-colors hover:bg-gray-100 hover:text-text-light dark:text-subtext-dark dark:hover:bg-gray-800 dark:hover:text-text-dark"
      aria-label="Toggle dark mode"
      onClick={() => setTheme((t) => (t === "dark" ? "light" : "dark"))}
    >
      <span className="material-symbols-outlined text-[20px] dark:hidden">dark_mode</span>
      <span className="material-symbols-outlined hidden text-[20px] dark:inline">light_mode</span>
    </button>
  );
}
