import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import { orchestratorApi } from "../services/orchestrator";
import StatusBadge from "../components/StatusBadge";
import BatchForm from "../components/BatchForm";
import { Plus } from "lucide-react";
import type { Batch } from "../types";

export default function BatchesPage() {
  const [showForm, setShowForm] = useState(false);
  const navigate = useNavigate();
  const { data: batches, loading, error } = usePolling(
    orchestratorApi.getBatches,
    3000
  );

  const handleCreated = (batchId: string) => {
    setShowForm(false);
    navigate(`/batches/${batchId}`);
  };

  return (
    <div>
      <div className="page-header" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <h2>Batches</h2>
          <p>Quản lý các job batch — tạo batch đơn giản hoặc batch DAG</p>
        </div>
        <button className="btn btn-primary" onClick={() => setShowForm((v) => !v)}>
          <Plus size={16} />
          {showForm ? "Đóng" : "Tạo Batch"}
        </button>
      </div>

      {showForm && (
        <div className="mb-4">
          <BatchForm
            onCreated={handleCreated}
            onClose={() => setShowForm(false)}
          />
        </div>
      )}

      {error && <div className="error-banner">{error}</div>}

      <div className="card">
        <h3>Danh sách batches</h3>
        {loading && !batches ? (
          <div className="loading">Đang tải...</div>
        ) : !batches || batches.length === 0 ? (
          <div className="loading">Chưa có batch nào. Hãy tạo batch mới.</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Batch ID</th>
                <th>Status</th>
                <th>Priority</th>
                <th>Documents</th>
                <th>Created</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {batches.map((batch: Batch) => (
                <tr key={batch.batch_id}>
                  <td>
                    <Link className="link" to={`/batches/${batch.batch_id}`}>
                      {batch.batch_id.slice(0, 8)}...
                    </Link>
                  </td>
                  <td><StatusBadge status={batch.status} /></td>
                  <td>{batch.priority}</td>
                  <td>{batch.total_documents}</td>
                  <td>{new Date(batch.created_at).toLocaleString()}</td>
                  <td>
                    <Link className="btn" to={`/batches/${batch.batch_id}`}>
                      View
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}