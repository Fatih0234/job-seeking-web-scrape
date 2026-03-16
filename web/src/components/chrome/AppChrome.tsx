"use client";

import type { ReactNode } from "react";
import AppRail from "@/components/chrome/AppRail";

export default function AppChrome({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-dvh w-dvw overflow-hidden bg-background-light text-text-light dark:bg-background-dark dark:text-text-dark">
      <AppRail />
      <div className="flex min-w-0 flex-1 flex-col">{children}</div>
    </div>
  );
}

