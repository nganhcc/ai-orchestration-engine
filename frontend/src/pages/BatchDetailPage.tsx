import { useMemo } from "react";
import { useParams } from "react-router-dom";
import { usePolling } from "../hooks/usePolling";
import { orchestratorApi } from "../services/orchestrator";
import StatusBadge from "../components/StatusBadge";
import { ReactFlow, Background, Controls, Handle, Position, type Node, type Edge } from "reactflow";
import "reactflow/dist/style.css";
import type { Step } from "../types";

// Custom node component
function DagStepNode({ data }: { data: { label: string; documentId: string; status: string } }) {
  return (
    <div className={`dag-node status-${data.status}`}>
      <Handle type="target" position={Position.Top} />
      <div>{data.label}</div>
      <div className="sub">{data.documentId}</div>
      <div className="sub" style={{ marginTop: 4 }}>
        <StatusBadge status={data.status} />
      </div>
      <Handle type="source" position={Position.Bottom} />
    </div>
  );
}

const nodeTypes = { dagStep: DagStepNode };

export default function BatchDetailPage() {
  const { batchId } = useParams<{ batchId: string }>();

  if (!batchId) {
    return <div className="error-banner">Thiếu batchId</div>;
  }

  const { data: steps, loading, error } = usePolling(
    () => orchestratorApi.getBatchSteps(batchId),
    2000
  );
  const { data: batches } = usePolling(orchestratorApi.getBatches, 3000);
  const batch = batches?.find((b) => b.batch_id === batchId);

  // Build DAG nodes & edges from steps + document_id (assume simple batch has no deps → single column)
  const { nodes, edges } = useMemo(() => {
    const stepList: Step[] = steps ?? [];
    // For simple batches we show sequential steps. For DAG we infer deps from document naming is unreliable,
    // so we render each step as a node in a vertical flow. Real deps come from depends_on but API doesn't return it,
    // so we link steps sequentially.
    const n: Node[] = stepList.map((s, i) => ({
      id: s.step_id,
      type: "dagStep",
      position: { x: 0, y: i * 110 },
      data: {
        label: s.document_id,
        documentId: s.step_id.slice(0, 8),
        status: s.status,
      },
    }));
    const e: Edge[] = [];
    for (let i = 0; i < stepList.length - 1; i++) {
      e.push({
        id: `e-${i}`,
        source: stepList[i].step_id,
        target: stepList[i + 1].step_id,
        animated: true,
      });
    }
    return { nodes: n, edges: e };
  }, [steps]);

  return (
    <div>
      <div className="page-header">
        <h2>Batch Detail: {batchId}</h2>
        <p>
          {batch ? (
            <>
              Priority: <strong>{batch.priority}</strong> · Documents:{" "}
              <strong>{batch.total_documents}</strong> · Status:{" "}
              <StatusBadge status={batch.status} />
            </>
          ) : (
            "Đang tải thông tin batch..."
          )}
        </p>
      </div>

      {error && <div className="error-banner">{error}</div>}

      <div className="grid grid-2" style={{ gridTemplateColumns: "1.4fr 1fr" }}>
        <div className="card">
          <h3>DAG Visualization (realtime)</h3>
          <div style={{ height: 420, border: "1px solid var(--border)", borderRadius: 8 }}>
            {loading && !steps ? (
              <div className="loading">Đang tải DAG...</div>
            ) : steps && steps.length > 0 ? (
              <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={nodeTypes}
                fitView
                proOptions={{ hideAttribution: true }}
                nodesDraggable
              >
                <Background gap={16} />
                <Controls />
              </ReactFlow>
            ) : (
              <div className="loading">Chưa có step nào trong batch này.</div>
            )}
          </div>
        </div>

        <div className="card">
          <h3>Danh sách steps</h3>
          {loading && !steps ? (
            <div className="loading">Đang tải...</div>
          ) : steps && steps.length > 0 ? (
            <table>
              <thead>
                <tr>
                  <th>Document</th>
                  <th>Status</th>
                  <th>Retry</th>
                </tr>
              </thead>
              <tbody>
                {steps.map((s) => (
                  <tr key={s.step_id}>
                    <td>{s.document_id}</td>
                    <td><StatusBadge status={s.status} /></td>
                    <td>{s.retry_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="loading">Chưa có step nào.</div>
          )}
        </div>
      </div>
    </div>
  );
}