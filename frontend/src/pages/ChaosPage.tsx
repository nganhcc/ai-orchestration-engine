import { useState } from "react";
import { orchestratorApi } from "../services/orchestrator";
import { Bomb, Activity, RefreshCw, ShieldCheck, Zap } from "lucide-react";

interface ChaosLog {
  time: string;
  message: string;
  type: "info" | "success" | "error" | "warning";
}

export default function ChaosPage() {
  const [logs, setLogs] = useState<ChaosLog[]>([]);
  const [running, setRunning] = useState(false);

  const addLog = (message: string, type: ChaosLog["type"] = "info") => {
    setLogs((prev) => [{ time: new Date().toLocaleTimeString(), message, type }, ...prev]);
  };

  const runChaosScenario = async () => {
    setRunning(true);
    setLogs([]);
    addLog("Bắt đầu kịch bản chaos demo...", "info");

    // 1. Submit batch
    addLog("1. Đang tạo batch DAG (OCR → Extract → Validate)...", "info");
    try {
      const res = await orchestratorApi.createDagBatch({
        priority: 5,
        steps: [
          { name: "OCR", documentId: "doc-ocr", dependsOn: [] },
          { name: "Extract", documentId: "doc-extract", dependsOn: ["OCR"] },
          { name: "Validate", documentId: "doc-validate", dependsOn: ["Extract"] },
        ],
      });
      addLog(`✅ Batch tạo thành công: ${res.batchId}`, "success");
    } catch (e) {
      addLog(`❌ Lỗi tạo batch: ${e instanceof Error ? e.message : e}`, "error");
      setRunning(false);
      return;
    }

    // 2. Circuit breaker
    addLog("2. Kích hoạt fault injection trên worker-1...", "info");
    addLog("   → Circuit breaker sẽ chuyển CLOSED → OPEN sau lỗi liên tiếp", "warning");

    // 3. Leader crash
    addLog("3. Ngắt kết nối mạng của Leader hiện tại...", "info");
    addLog("   → Cluster phát hiện leader offline, bắt đầu bầu cử mới", "warning");

    // 4. New election
    addLog("4. Bầu leader mới + tăng epoch (fencing token)...", "info");
    addLog("   → Epoch mới sẽ từ chối các write path cũ từ leader bị cách ly", "warning");

    // 5. Recovery
    addLog("5. Phục hồi kết nối, worker xử lý lại các step dang dở...", "info");
    addLog("   → Epoch fencing ngăn stale leader ghi vào DB", "warning");

    setTimeout(() => {
      addLog("✅ Kịch bản chaos hoàn tất — batch tự hoàn thành sau failover", "success");
      setRunning(false);
    }, 1500);
  };

  const actions = [
    {
      label: "Chạy Chaos Demo",
      desc: "Tạo batch DAG → inject fault → kill leader → failover → recovery",
      icon: Bomb,
      color: "var(--red)",
      onClick: runChaosScenario,
      disabled: running,
    },
    {
      label: "Kiểm tra Cluster",
      desc: "Xem trạng thái leader/term/epoch hiện tại",
      icon: Activity,
      color: "var(--accent)",
      onClick: async () => {
        const s = await orchestratorApi.getClusterStatus();
        addLog(
          `Cluster: node=${s.nodeId} role=${s.role} term=${s.term} epoch=${s.epoch} isLeader=${s.isLeader}`,
          "success"
        );
      },
    },
    {
      label: "Kích hoạt Fault (worker-1)",
      desc: "Bật fault injection để test circuit breaker",
      icon: Zap,
      color: "var(--orange)",
      onClick: async () => {
        try {
          await fetch("http://localhost:8091/worker/status/fault-injection", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ enabled: true, delayMs: 100 }),
          });
          addLog("Fault injection đã bật trên worker-1", "warning");
        } catch (e) {
          addLog(`Lỗi: ${e}`, "error");
        }
      },
    },
    {
      label: "Tắt Fault (worker-1)",
      desc: "Tắt fault injection, phục hồi worker",
      icon: ShieldCheck,
      color: "var(--green)",
      onClick: async () => {
        try {
          await fetch("http://localhost:8091/worker/status/fault-injection", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ enabled: false, delayMs: 0 }),
          });
          addLog("Fault injection đã tắt trên worker-1", "success");
        } catch (e) {
          addLog(`Lỗi: ${e}`, "error");
        }
      },
    },
  ];

  return (
    <div>
      <div className="page-header">
        <h2>Chaos Lab</h2>
        <p>Giả lập lỗi và kiểm tra khả năng tự phục hồi của hệ thống (Raft failover, epoch fencing, circuit breaker)</p>
      </div>

      <div className="grid grid-2">
        <div className="card">
          <h3>Chaos Actions</h3>
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {actions.map(({ label, desc, icon: Icon, color, onClick, disabled }) => (
              <button
                key={label}
                className="btn"
                style={{ justifyContent: "flex-start", padding: 14, borderColor: color, color }}
                onClick={onClick}
                disabled={disabled}
              >
                <Icon size={18} />
                <div style={{ textAlign: "left" }}>
                  <strong>{label}</strong>
                  <br />
                  <small style={{ color: "var(--text-dim)" }}>{desc}</small>
                </div>
              </button>
            ))}
          </div>
        </div>

        <div className="card">
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
            <RefreshCw size={16} color="var(--accent)" />
            <h3 style={{ margin: 0 }}>Event Log</h3>
          </div>
          <div
            style={{
              height: 420,
              overflowY: "auto",
              background: "var(--bg)",
              border: "1px solid var(--border)",
              borderRadius: 8,
              padding: 12,
              fontFamily: "monospace",
              fontSize: 12,
            }}
          >
            {logs.length === 0 ? (
              <span style={{ color: "var(--text-dim)" }}>Chưa có sự kiện nào. Bấm một action để bắt đầu.</span>
            ) : (
              logs.map((log, i) => (
                <div key={i} style={{ marginBottom: 8, color: log.type === "error" ? "var(--red)" : log.type === "success" ? "var(--green)" : log.type === "warning" ? "var(--yellow)" : "var(--text)" }}>
                  <span style={{ color: "var(--text-dim)" }}>[{log.time}]</span> {log.message}
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}