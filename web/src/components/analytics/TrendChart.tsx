"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export default function TrendChart(props: { data: Array<{ month: string; jobs: number }> }) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={props.data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" opacity={0.15} />
        <XAxis dataKey="month" tick={{ fill: "currentColor", fontSize: 12 }} />
        <YAxis tick={{ fill: "currentColor", fontSize: 12 }} width={40} />
        <Tooltip
          contentStyle={{
            borderRadius: 12,
            border: "1px solid rgba(229,231,235,0.9)",
            background: "rgba(255,255,255,0.92)",
            backdropFilter: "blur(10px)",
          }}
        />
        <Bar dataKey="jobs" fill="#2563EB" radius={[6, 6, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

