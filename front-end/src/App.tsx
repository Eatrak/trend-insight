import { BrowserRouter, Route, Routes } from "react-router-dom";
import Layout from "@/components/layout";
import Dashboard from "@/pages/Dashboard";
import TopicDetail from "@/pages/TopicDetail";
import CreateTopic from "@/pages/CreateTopic";

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<Dashboard />} />
          <Route path="/topics/new" element={<CreateTopic />} />
          <Route path="/topics/:id" element={<TopicDetail />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;