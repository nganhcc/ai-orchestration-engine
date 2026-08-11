import { Routes, Route } from "react-router-dom";
import Layout from "./components/Layout";
import DashboardPage from "./pages/DashboardPage";
import BatchesPage from "./pages/BatchesPage";
import BatchDetailPage from "./pages/BatchDetailPage";
import WorkersPage from "./pages/WorkersPage";
import ChaosPage from "./pages/ChaosPage";

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/batches" element={<BatchesPage />} />
        <Route path="/batches/:batchId" element={<BatchDetailPage />} />
        <Route path="/workers" element={<WorkersPage />} />
        <Route path="/chaos" element={<ChaosPage />} />
      </Routes>
    </Layout>
  );
}