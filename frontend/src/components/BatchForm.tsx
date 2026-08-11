import { useState } from "react";
import { orchestratorApi } from "../services/orchestrator";
import type { DagStepInput } from "../types";

interface BatchFormProps {
  onCreated: (batchId: string) => void;
  onClose: () => void;
}

export default function BatchForm({ onCreated, onClose }: BatchFormProps) {
  const [mode, setMode] = useState<"simple" | "dag">("simple");
  const [priority, setPriority] = useState(5);
  const [idempotencyKey, setIdempotencyKey] = useState("");
  const [documents, setDocuments] = useState("doc-1\ndoc-2\ndoc-3");
  const [dagSteps, setDagSteps] = useState<DagStepInput[]>([
    { name: "OCR", documentId: "doc-ocr", dependsOn: [] },
    { name: "Extract", documentId: "doc-extract", dependsOn: ["OCR"] },
    { name: "Validate", documentId: "doc-validate", dependsOn: ["Extract"] },
  ]);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const handleSubmit = async () => {
    setError(null);
    setSubmitting(true);
    try {
      if (mode === "simple") {
        const docs = documents.split("\n").map((d) => d.trim()).filter(Boolean);
        const res = await orchestratorApi.createBatch({
          idempotencyKey: idempotencyKey || undefined,
          documents: docs,
          priority,
        });
        onCreated(res.batchId);
      } else {
        const res = await orchestratorApi.createDagBatch({
          idempotencyKey: idempotencyKey || undefined,
          priority,
          steps: dagSteps,
        });
        onCreated(res.batchId);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  };

  const updateDagStep = (index: number, field: keyof DagStepInput, value: any) => {
    setDagSteps((prev) =>
      prev.map((s, i) =>
        i === index ? { ...s, [field]: value } : s
      )
    );
  };

  const addDagStep = () => {
    setDagSteps((prev) => [...prev, { name: `Step ${prev.length + 1}`, documentId: `doc-${prev.length + 1}`, dependsOn: [] }]);
  };

  const removeDagStep = (index: number) => {
    setDagSteps((prev) => prev.filter((_, i) => i !== index));
  };

  const allNames = dagSteps.map((s) => s.name);

  return (
    <div className="card">
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <h3 style={{ margin: 0 }}>Tạo Batch Mới</h3>
        <button className="btn" onClick={onClose}>Hủy</button>
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 20 }}>
        <button
          className={`btn ${mode === "simple" ? "btn-primary" : ""}`}
          onClick={() => setMode("simple")}
        >
          Batch Đơn Giản
        </button>
        <button
          className={`btn ${mode === "dag" ? "btn-primary" : ""}`}
          onClick={() => setMode("dag")}
        >
          Batch DAG
        </button>
      </div>

      {error && <div className="error-banner">{error}</div>}

      <div className="grid grid-2">
        <div className="field">
          <label>Priority (1 = cao nhất, 10 = thấp nhất)</label>
          <input
            type="number"
            min={1}
            max={10}
            value={priority}
            onChange={(e) => setPriority(Number(e.target.value))}
          />
        </div>
        <div className="field">
          <label>Idempotency Key (tùy chọn)</label>
          <input
            value={idempotencyKey}
            onChange={(e) => setIdempotencyKey(e.target.value)}
            placeholder="vd: batch-001"
          />
        </div>
      </div>

      {mode === "simple" ? (
        <div className="field">
          <label>Danh sách documents (mỗi dòng 1 document)</label>
          <textarea
            rows={4}
            value={documents}
            onChange={(e) => setDocuments(e.target.value)}
          />
        </div>
      ) : (
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
            <label>DAG Steps (định nghĩa phụ thuộc)</label>
            <button className="btn" onClick={addDagStep}>+ Thêm step</button>
          </div>
          {dagSteps.map((step, i) => (
            <div key={i} className="grid grid-3" style={{ marginBottom: 10 }}>
              <div className="field" style={{ marginBottom: 0 }}>
                <label>Name</label>
                <input
                  value={step.name}
                  onChange={(e) => updateDagStep(i, "name", e.target.value)}
                />
              </div>
              <div className="field" style={{ marginBottom: 0 }}>
                <label>Document ID</label>
                <input
                  value={step.documentId}
                  onChange={(e) => updateDagStep(i, "documentId", e.target.value)}
                />
              </div>
              <div className="field" style={{ marginBottom: 0 }}>
                <label>Depends On (tên step trước)</label>
                <select
                  multiple
                  value={step.dependsOn ?? []}
                  onChange={(e) =>
                    updateDagStep(
                      i,
                      "dependsOn",
                      Array.from(e.target.selectedOptions).map((o) => o.value)
                    )
                  }
                >
                  {allNames
                    .filter((n) => n !== step.name)
                    .map((n) => (
                      <option key={n} value={n}>
                        {n}
                      </option>
                    ))}
                </select>
                <button
                  className="btn btn-danger"
                  style={{ marginTop: 6 }}
                  onClick={() => removeDagStep(i)}
                >
                  Xóa
                </button>
              </div>
            </div>
          ))}
        </div>
      )}

      <button
        className="btn btn-primary mt-4"
        onClick={handleSubmit}
        disabled={submitting}
      >
        {submitting ? "Đang tạo..." : "Tạo Batch"}
      </button>
    </div>
  );
}