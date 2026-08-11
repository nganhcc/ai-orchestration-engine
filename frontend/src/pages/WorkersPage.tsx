import { useEffect, useState } from "react";
import { workerApi } from "../services/orchestrator";
import StatusBadge from "../components/StatusBadge";
import type { CircuitBreakerStatus } from "../types";

const WORKERS = [
  { name: "worker-1", port: 8091 },
  { name: "worker-2", port: 8092 },
];

export default function WorkersPage() {
  const [statuses, setStatuses] = useState<Record<number, CircuitBreakerStatus>>({});
  const [faultStates, setFaultStates] = useState<Record<number, { enabled: boolean; delayMs: number }>>({});
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    const poll = async () => {
      for (const w of WORKERS) {
        const s = await workerApi.getCircuitBreakerStatus(w.port);
        setStatuses((prev) => ({ ...prev, [w.port]: s }));
      }
    };
    poll();
    const id = setInterval(poll, 2000);
    return () => clearInterval(id);
  }, []);

  const toggleFault = async (port: number, enabled: boolean, delayMs: number) => {
    try {
      const res = await workerApi.configureFaultInjection(enabled, delayMs, port);
      setFaultStates((prev) => ({ ...prev, [port]: { enabled: res.failureMode, delayMs: res.failureDelayMs } }));
      setMessage(`Worker :${port} — fault injection ${res.failureMode ? "bật" : "tắt"} (delay ${res.failureDelayMs}ms)`);
      setTimeout(() => setMessage(null), 3000);
    } catch (e) {
      setMessage(`Lỗi: ${e instanceof Error ? e.message : e}`);
    }
  };

  return (
    <div>
      <div className="page-header">
        <h2>Workers</h2>
        <p>Giám sát circuit breaker và điều khiển fault injection của các worker</p>
      </div>

      {message && <div className="success-banner">{message}</div>}

      <div className="grid grid-2">
        {WORKERS.map((w) => {
          const status = statuses[w.port];
          const fault = faultStates[w.port] ?? { enabled: false, delayMs: 0 };
          return (
            <div className="card" key={w.port}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <h3 style={{ margin: 0 }}>{w.name} <span style={{ color: "var(--text-dim)", fontWeight: 400 }}>(:{w.port})</span></h3>
                {status && <StatusBadge status={status.state} />}
              </div>

              <div style={{ marginTop: 16 }}>
                <p style={{ fontSize: 13, color: "var(--text-dim)" }}>
                  Consecutive Failures:{" "}
                  <strong style={{ color: "var(--text)" }}>{status?.consecutiveFailures ?? 0}</strong>
                </p>
              </div>

              <div style={{ marginTop: 16, borderTop: "1px solid var(--border)", paddingTop: 16 }}>
                <h3 style={{ marginBottom: 12 }}>Fault Injection</h3>
                <div className="grid grid-2">
                  <div className="field" style={{ marginBottom: 0 }}>
                    <label>Trạng thái</label>
                    <button
                      className={`btn ${fault.enabled ? "btn-danger" : "btn-primary"}`}
                      onClick={() => toggleFault(w.port, !fault.enabled, fault.delayMs)}
                    >
                      {fault.enabled ? "Tắt Fault" : "Bật Fault"}
                    </button>
                  </div>
                  <div className="field" style={{ marginBottom: 0 }}>
                    <label>Delay (ms)</label>
                    <input
                      type="number"
                      value={fault.delayMs}
                      onChange={(e) =>
                        setFaultStates((prev) => ({
                          ...prev,
                          [w.port]: { ...fault, delayMs: Number(e.target.value) },
                        }))
                      }
                    />
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}