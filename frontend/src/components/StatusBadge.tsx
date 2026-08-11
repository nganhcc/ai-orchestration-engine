
const colors: Record<string, string> = {
  PENDING: "badge-gray",
  BLOCKED: "badge-yellow",
  ASSIGNED: "badge-blue",
  IN_PROGRESS: "badge-blue",
  DONE: "badge-green",
  FAILED: "badge-red",
  RUNNING: "badge-blue",
  LEADER: "badge-green",
  FOLLOWER: "badge-gray",
  CANDIDATE: "badge-yellow",
  CLOSED: "badge-green",
  OPEN: "badge-red",
  HALF_OPEN: "badge-yellow",
};

export default function StatusBadge({ status }: { status: string }) {
  return <span className={`badge ${colors[status] ?? "badge-gray"}`}>{status}</span>;
}