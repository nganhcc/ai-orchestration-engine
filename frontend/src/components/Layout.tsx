import { NavLink } from "react-router-dom";
import { ListOrdered, Boxes, Activity, Bomb } from "lucide-react";
import type { ReactNode } from "react";

const navItems = [
  { to: "/", label: "Dashboard", icon: Activity },
  { to: "/batches", label: "Batches", icon: ListOrdered },
  { to: "/workers", label: "Workers", icon: Boxes },
  { to: "/chaos", label: "Chaos Lab", icon: Bomb },
];

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <div className="app-layout">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <Activity size={22} className="logo" />
          <div>
            <h1>AI Orchestration</h1>
            <p>Distributed Engine</p>
          </div>
        </div>
        <nav className="sidebar-nav">
          {navItems.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              end={to === "/"}
              className={({ isActive }) => (isActive ? "active" : "")}
            >
              <Icon size={17} />
              {label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="content">{children}</main>
    </div>
  );
}