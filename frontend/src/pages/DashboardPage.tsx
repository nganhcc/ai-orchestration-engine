import { usePolling } from "../hooks/usePolling";
import { orchestratorApi } from "../services/orchestrator";
import MetricCard from "../components/MetricCard";
import StatusBadge from "../components/StatusBadge";
import { Layers, CheckCircle2, RotateCcw, RefreshCw, Server, Radio } from "lucide-react";

export default function DashboardPage() {
  const { data: cluster, loading: clusterLoading } = usePolling(
    orchestratorApi.getClusterStatus,
    2000
  );
  const { data: metrics } = usePolling(orchestratorApi.getMetrics, 2000);

  return (
    <div>
      <div className="page-header">
        <h2>Dashboard</h2>
        <p>Tổng quan hệ thống AI Orchestration Engine — tự động làm mới mỗi 2 giây</p>
      </div>

      <div className="grid grid-4 mb-4">
        <MetricCard label="Batches Created" value={metrics?.batchesCreated ?? 0} icon={Layers} color="var(--accent)" />
        <MetricCard label="Steps Completed" value={metrics?.stepsCompleted ?? 0} icon={CheckCircle2} color="var(--green)" />
        <MetricCard label="Steps Reassigned" value={metrics?.stepsReassigned ?? 0} icon={RotateCcw} color="var(--yellow)" />
        <MetricCard label="Steps Rerun" value={metrics?.stepsRerun ?? 0} icon={RefreshCw} color="var(--orange)" />
      </div>

      <div className="grid grid-2">
        <div className="card">
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <Server size={16} color="var(--accent)" />
            <h3 style={{ margin: 0 }}>Raft Cluster Status</h3>
            <span style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 6 }}>
              <Radio size={14} color="var(--green)" />
              <small>Live</small>
            </span>
          </div>

          {clusterLoading ? (
            <div className="loading">Đang tải trạng thái cluster...</div>
          ) : (
            <div style={{ marginTop: 16 }}>
              {cluster?.enabled === false ? (
                <div className="error-banner" style={{ marginBottom: 0 }}>
                  Raft cluster chưa được kích hoạt hoặc không thể kết nối.
                </div>
              ) : (
                <table>
                  <tbody>
                    <tr>
                      <td style={{ color: "var(--text-dim)" }}>Node ID</td>
                      <td style={{ fontWeight: 600 }}>{cluster?.nodeId}</td>
                    </tr>
                    <tr>
                      <td style={{ color: "var(--text-dim)" }}>Role</td>
                      <td>
                        <StatusBadge status={cluster?.role ?? "UNKNOWN"} />
                      </td>
                    </tr>
                    <tr>
                      <td style={{ color: "var(--text-dim)" }}>Current Term</td>
                      <td style={{ fontWeight: 600 }}>{cluster?.term}</td>
                    </tr>
                    <tr>
                      <td style={{ color: "var(--text-dim)" }}>Epoch (Fencing)</td>
                      <td style={{ fontWeight: 600 }}>{cluster?.epoch}</td>
                    </tr>
                    <tr>
                      <td style={{ color: "var(--text-dim)" }}>Is Leader</td>
                      <td>
                        {cluster?.isLeader ? (
                          <span className="badge badge-green">YES</span>
                        ) : (
                          <span className="badge badge-gray">NO</span>
                        )}
                      </td>
                    </tr>
                  </tbody>
                </table>
              )}
            </div>
          )}
        </div>

        <div className="card">
          <h3>Kiến trúc hệ thống</h3>
          <div style={{ fontSize: 13, color: "var(--text-dim)", lineHeight: 1.8 }}>
            <p>• <strong style={{ color: "var(--text)" }}>Raft Cluster (3 nodes)</strong> — consensus qua Netty TCP</p>
            <p>• <strong style={{ color: "var(--text)" }}>Kafka</strong> — giao tiếp Orchestrator ↔ Worker</p>
            <p>• <strong style={{ color: "var(--text)" }}>PostgreSQL</strong> — state & outbox</p>
            <p>• <strong style={{ color: "var(--text)" }}>Redis</strong> — cache invalidation</p>
            <p>• <strong style={{ color: "var(--text)" }}>DAG + DRR</strong> — scheduling & priority</p>
            <p>• <strong style={{ color: "var(--text)" }}>Circuit Breaker</strong> — bảo vệ LLM client</p>
          </div>
        </div>
      </div>
    </div>
  );
}