import type { LucideIcon } from "lucide-react";

interface MetricCardProps {
  label: string;
  value: number;
  icon: LucideIcon;
  color?: string;
}

export default function MetricCard({ label, value, icon: Icon, color }: MetricCardProps) {
  return (
    <div className="card">
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <h3>{label}</h3>
        <Icon size={20} style={{ color: color ?? "var(--accent)" }} />
      </div>
      <div className="metric-value">{value}</div>
    </div>
  );
}