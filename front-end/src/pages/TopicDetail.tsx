import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import {
  Bar,
  BarChart,
  Line,
  LineChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { format } from "date-fns";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { api, type Metric, type Topic } from "@/services/api";
import { useInterval } from "@/lib/useInterval";

const getSentimentEmoji = (score: number) => {
  if (score >= 0.05) return "😀"; // Positive
  if (score <= -0.05) return "😡"; // Negative
  return "😐"; // Neutral
};

const getSentimentLabel = (score: number) => {
  if (score >= 0.05) return "Positive";
  if (score <= -0.05) return "Negative";
  return "Neutral";
};

export default function TopicDetail() {
  const { id } = useParams<{ id: string }>();
  const [topic, setTopic] = useState<Topic | null>(null);
  const [metrics, setMetrics] = useState<Metric[]>([]);
  const [selectedWindow, setSelectedWindow] = useState<string>("1d");
  const [selectedRange, setSelectedRange] = useState<number>(7);
  const [isBackfilling, setIsBackfilling] = useState(false);

  const fetchTopic = async () => {
    if (!id) return;
    try {
      const t = await api.getTopic(id);
      setTopic(t);
    } catch (e) {
      console.error(e);
    }
  };

  const fetchMetrics = async () => {
    if (!id) return;
    try {
      const data = await api.getTopicReport(id);
      setMetrics(data.metrics);
      if (data.metrics.length > 0 && selectedWindow === "1d") {
        const has1d = data.metrics.some((m) => m.window_type === "1d");
        if (!has1d) {
          setSelectedWindow(data.metrics[0].window_type);
        }
      }
    } catch (e) {
      console.error(e);
    }
  };

  const refreshData = async () => {
    await Promise.all([fetchTopic(), fetchMetrics()]);
  };

  useEffect(() => {
    if (id) {
      refreshData();
    }
  }, [id]);

  // Auto-adjust range if it's too small for the selected window
  useEffect(() => {
    if (selectedWindow === "1w" && selectedRange < 14) {
      setSelectedRange(30); // Default to 30d for weekly view
    }
    if (selectedWindow === "1m" && selectedRange < 60) {
      setSelectedRange(90); // Default to 90d for monthly view
    }
  }, [selectedWindow, selectedRange]);

  // Always poll every 5s
  useInterval(refreshData, 5000);

  if (!topic) {
    return <div className="p-8">Loading topic...</div>;
  }

  // ... (rest of filtering) ...

  // Fixed window types (Resolution)
  const ALL_WINDOW_TYPES = ["1d", "1w", "1m"];
  const availableWindows = new Set(metrics.map((m) => m.window_type));

  // Time Range types (Zoom)
  const TIME_RANGES = [
    { label: "7D", value: 7 },
    { label: "30D", value: 30 },
    { label: "90D", value: 90 },
  ];

  // Filter metrics by selected window (Resolution)
  const filteredMetrics = metrics.filter(
    (m) => m.window_type === selectedWindow,
  );

  // 1. Determine Step Size based on Window
  let stepMs = 24 * 60 * 60 * 1000; // 1d
  if (selectedWindow === "1w") stepMs = 7 * 24 * 60 * 60 * 1000;
  if (selectedWindow === "1m") stepMs = 30 * 24 * 60 * 60 * 1000;

  // Filter and Time Range (Zoom)
  const now = new Date();

  // Align to Midnight for strict day boundaries
  const todayMidnight = new Date(now);
  todayMidnight.setHours(0, 0, 0, 0);

  // Range Cutoff = Midnight Today - (N-1) Days
  // E.g. 7d -> Today + 6 past days = 7 bars
  const rangeCutoff = new Date(todayMidnight);
  rangeCutoff.setDate(todayMidnight.getDate() - (selectedRange - 1));

  // 2. Generate Complete Timeline (Zero-Filling)
  // We align chart points to the known metrics for accuracy, filling gaps with 0.
  const chartData = [];
  let runner = rangeCutoff.getTime();
  const endTime = todayMidnight.getTime();

  while (runner <= endTime) {
    // Correct Matching Logic: Does the Metric belong to this Bar?
    // Bar Interval: [runner, runner + stepMs)
    // Metric Point: mStart
    // We match if the Metric started within this bar's timeframe.
    // This handles Timezone offsets (e.g. Metric 01:00 belongs to Bar 00:00).
    const match = filteredMetrics.find((m) => {
      const mStart = new Date(m.start).getTime();
      return mStart >= runner && mStart < runner + stepMs;
    });

    chartData.push({
      end: runner,
      mentions: match ? match.mentions : 0,
      engagement: match ? match.engagement : 0,
      sentiment: match ? match.sentiment || 0 : 0,
      growth: match ? match.growth || 0 : 0,
    });

    runner += stepMs;
  }

  // Calculate Weighted Average Sentiment for the current view
  const visibleMetrics = metrics.filter(
    (m) =>
      m.window_type === "1d" &&
      new Date(m.start).getTime() >= rangeCutoff.getTime() &&
      new Date(m.start).getTime() < now.getTime(),
  );

  const totalMentionsInView = visibleMetrics.reduce(
    (acc, curr) => acc + (curr.mentions || 0),
    0,
  );
  const totalEngagementInView = visibleMetrics.reduce(
    (acc, curr) => acc + (curr.engagement || 0),
    0,
  );
  const weightedSentimentSum = visibleMetrics.reduce(
    (acc, curr) => acc + (curr.sentiment || 0) * (curr.mentions || 0),
    0,
  );
  const avgSentimentInView =
    totalMentionsInView > 0 ? weightedSentimentSum / totalMentionsInView : 0;

  return (
    <div className="flex min-h-screen w-full flex-col bg-muted/40 p-4 md:p-8">
      <div className="mx-auto grid w-full max-w-6xl gap-4">
        <div className="flex items-center justify-between">
          <div className="flex-1">
            <h1 className="text-3xl font-bold tracking-tight">{topic.id}</h1>
            <p className="text-muted-foreground">{topic.description}</p>
            <div className="mt-2">
              <Button
                size="sm"
                variant="outline"
                disabled={
                  isBackfilling ||
                  topic.backfill_status === "PENDING" ||
                  topic.backfill_status === "COMPLETED"
                }
                onClick={async () => {
                  try {
                    setIsBackfilling(true);
                    setTopic((prev) =>
                      prev
                        ? {
                            ...prev,
                            backfill_status: "PENDING",
                            backfill_percentage: 0,
                          }
                        : null,
                    );
                    await api.triggerBackfill(topic.id);
                  } catch (e) {
                    console.error(e);
                    try {
                      const updated = await api.getTopic(topic.id);
                      if (updated.backfill_status === "PENDING") {
                        setTopic(updated);
                        return;
                      }
                    } catch (ignore) {
                      /* empty */
                    }
                    setTopic((prev) =>
                      prev ? { ...prev, backfill_status: "ERROR" } : null,
                    );
                  } finally {
                    setIsBackfilling(false);
                  }
                }}
              >
                {isBackfilling || topic.backfill_status === "PENDING"
                  ? `Backfilling... ${(topic.backfill_percentage || 0).toFixed(1)}%`
                  : topic.backfill_status === "COMPLETED"
                    ? "History Loaded"
                    : "Load 30-Day History"}
              </Button>
            </div>
          </div>

          {/* Window Selector */}
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground">Window:</span>
            <div className="flex items-center rounded-lg border bg-card p-1">
              {ALL_WINDOW_TYPES.map((w) => {
                const isAvailable = availableWindows.has(w);
                return (
                  <button
                    key={w}
                    disabled={!isAvailable}
                    onClick={() => setSelectedWindow(w)}
                    className={`px-3 py-1 text-sm font-medium rounded-md transition-colors ${
                      selectedWindow === w
                        ? "bg-primary text-primary-foreground"
                        : isAvailable
                          ? "text-muted-foreground hover:text-foreground"
                          : "text-muted-foreground/30 cursor-not-allowed"
                    }`}
                  >
                    {w}
                  </button>
                );
              })}
            </div>
          </div>

          {/* Range Selector */}
          <div className="flex items-center gap-2 border-l pl-4 ml-2">
            <span className="text-sm text-muted-foreground">Range:</span>
            <div className="flex items-center rounded-lg border bg-card p-1">
              {TIME_RANGES.map((r) => {
                let isDisabled = false;
                if (selectedWindow === "1w" && r.value < 14) isDisabled = true; // Need > 1 week
                if (selectedWindow === "1m" && r.value < 60) isDisabled = true; // Need > 1 month

                return (
                  <button
                    key={r.label}
                    disabled={isDisabled}
                    onClick={() => setSelectedRange(r.value)}
                    className={`px-3 py-1 text-sm font-medium rounded-md transition-colors ${
                      selectedRange === r.value
                        ? "bg-secondary text-secondary-foreground"
                        : isDisabled
                          ? "text-muted-foreground/30 cursor-not-allowed"
                          : "text-muted-foreground hover:text-foreground"
                    }`}
                  >
                    {r.label}
                  </button>
                );
              })}
            </div>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          {/* Keywords Display */}
          {Array.isArray(topic.keywords[0]) ? (
            // CNF Logic: Groups of ORs, connected by AND
            <div className="flex flex-wrap items-center gap-2">
              {(topic.keywords as string[][]).map((group, i) => (
                <div key={i} className="flex items-center gap-2">
                  <div className="flex flex-wrap items-center gap-y-2 p-2 rounded-lg border border-dashed border-primary/20 bg-primary/5">
                    {group.map((k, j) => (
                      <div key={k} className="flex items-center">
                        {j > 0 && (
                          <span className="text-[10px] uppercase font-bold text-muted-foreground mx-2">
                            OR
                          </span>
                        )}
                        <span className="inline-flex items-center rounded-md bg-orange-50 px-3 py-1 text-sm font-medium text-orange-700 ring-1 ring-inset ring-orange-700/10 shadow-sm">
                          {k}
                        </span>
                      </div>
                    ))}
                  </div>
                  {i < (topic.keywords as string[][]).length - 1 && (
                    <span className="text-[10px] uppercase font-bold text-muted-foreground mx-1">
                      AND
                    </span>
                  )}
                </div>
              ))}
            </div>
          ) : (
            // Legacy Flat List
            (topic.keywords as string[]).map((k) => (
              <span
                key={k}
                className="inline-flex items-center rounded-md bg-blue-50 px-2 py-1 text-xs font-medium text-blue-700 ring-1 ring-inset ring-blue-700/10"
              >
                {k}
              </span>
            ))
          )}
          {topic.subreddits.map((s) => (
            <span
              key={s}
              className="inline-flex items-center rounded-md bg-blue-100 px-3 py-1 text-sm font-medium text-blue-900 shadow-sm ring-1 ring-inset ring-blue-900/10"
            >
              r/{s}
            </span>
          ))}
        </div>

        {/* Summary Widgets */}
        <div className="grid gap-4 md:grid-cols-3">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Total Matches
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{totalMentionsInView}</div>
              <p className="text-xs text-muted-foreground">In selected range</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Total Engagement
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{totalEngagementInView}</div>
              <p className="text-xs text-muted-foreground">
                Posts + Upvotes + Comments
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Avg Sentiment
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {getSentimentEmoji(avgSentimentInView)}{" "}
                {avgSentimentInView.toFixed(2)}
              </div>
              <p className="text-xs text-muted-foreground">
                {getSentimentLabel(avgSentimentInView)}
              </p>
            </CardContent>
          </Card>
        </div>

        <div className="grid gap-4 md:grid-cols-1">
          <Card className="col-span-1">
            <CardHeader>
              <CardTitle>Matched Posts</CardTitle>
            </CardHeader>
            <CardContent className="pl-2">
              <div className="h-[300px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={chartData}>
                    <XAxis
                      dataKey="end"
                      tickFormatter={(val) => format(new Date(val), "MMM d")}
                      stroke="#888888"
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                    />
                    <YAxis
                      stroke="#888888"
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={(value) => `${value}`}
                    />
                    <CartesianGrid
                      strokeDasharray="3 3"
                      vertical={false}
                      stroke="hsl(var(--border))"
                    />
                    <Tooltip
                      cursor={false}
                      contentStyle={{
                        backgroundColor: "hsl(var(--card))",
                        borderColor: "hsl(var(--border))",
                      }}
                      itemStyle={{ color: "hsl(var(--foreground))" }}
                      labelFormatter={(label) =>
                        format(new Date(label), "PP p")
                      }
                    />
                    <Bar
                      dataKey="mentions"
                      fill="var(--chart-1)"
                      radius={[4, 4, 0, 0]}
                      activeBar={{
                        stroke: "var(--foreground)",
                        strokeWidth: 2,
                      }}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>

          <Card className="col-span-1">
            <CardHeader>
              <CardTitle>Engagement</CardTitle>
            </CardHeader>
            <CardContent className="pl-2">
              <div className="h-[300px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={chartData}>
                    <XAxis
                      dataKey="end"
                      tickFormatter={(val) => format(new Date(val), "MMM d")}
                      stroke="#888888"
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                    />
                    <YAxis
                      stroke="#888888"
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={(value) => `${value}`}
                    />
                    <CartesianGrid
                      strokeDasharray="3 3"
                      vertical={false}
                      stroke="hsl(var(--border))"
                    />
                    <Tooltip
                      cursor={false}
                      contentStyle={{
                        backgroundColor: "hsl(var(--card))",
                        borderColor: "hsl(var(--border))",
                      }}
                      itemStyle={{ color: "hsl(var(--foreground))" }}
                      labelFormatter={(label) =>
                        format(new Date(label), "PP p")
                      }
                    />
                    <Bar
                      dataKey="engagement"
                      fill="var(--chart-1)"
                      radius={[4, 4, 0, 0]}
                      name="Engagement"
                      activeBar={{
                        stroke: "var(--foreground)",
                        strokeWidth: 2,
                      }}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>

          <Card className="col-span-1">
            <CardHeader>
              <CardTitle>Sentiment</CardTitle>
            </CardHeader>

            <CardContent className="pl-2">
              <div className="h-[300px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData}>
                    <XAxis
                      dataKey="end"
                      tickFormatter={(val) => format(new Date(val), "MMM d")}
                      stroke="#888888"
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                    />
                    <YAxis
                      stroke="#888888"
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                      domain={[-1, 1]}
                      tickFormatter={(value) => getSentimentEmoji(value)}
                    />
                    <CartesianGrid
                      strokeDasharray="3 3"
                      vertical={false}
                      stroke="hsl(var(--border))"
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: "hsl(var(--card))",
                        borderColor: "hsl(var(--border))",
                      }}
                      itemStyle={{ color: "hsl(var(--foreground))" }}
                      labelFormatter={(label) =>
                        format(new Date(label), "PP p")
                      }
                      formatter={(value: any) => [
                        `${getSentimentEmoji(Number(value))} ${Number(value).toFixed(2)}`,
                        "Sentiment",
                      ]}
                    />
                    {/* Reference Line at 0 for neutral sentiment */}
                    <Line
                      type="monotone"
                      dataKey="sentiment"
                      name="Sentiment"
                      stroke="var(--chart-1)"
                      strokeWidth={2}
                      dot={{ strokeWidth: 2, r: 4 }}
                      activeDot={{ r: 6 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>

          <Card className="col-span-1">
            <CardHeader>
              <CardTitle>Growth</CardTitle>
            </CardHeader>
            <CardContent className="pl-2">
              <div className="h-[300px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData}>
                    <XAxis
                      dataKey="end"
                      type="number"
                      domain={[rangeCutoff.getTime(), now.getTime()]}
                      tickFormatter={(val) => format(new Date(val), "MMM d")}
                      scale="time"
                      stroke="#888888"
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                    />
                    <YAxis
                      stroke="#888888"
                      fontSize={12}
                      tickLine={false}
                      axisLine={false}
                      tickFormatter={(value) => `${value}x`}
                    />
                    <CartesianGrid
                      strokeDasharray="3 3"
                      vertical={false}
                      stroke="hsl(var(--border))"
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: "hsl(var(--card))",
                        borderColor: "hsl(var(--border))",
                      }}
                      itemStyle={{ color: "hsl(var(--foreground))" }}
                      labelFormatter={(label) =>
                        format(new Date(label), "PP p")
                      }
                      formatter={(value: any) => [
                        `${(Number(value) || 0).toFixed(2)}x`,
                        "Growth",
                      ]}
                    />
                    <Line
                      type="monotone"
                      dataKey="growth"
                      name="Growth"
                      stroke="var(--chart-1)"
                      strokeWidth={2}
                      dot={{ strokeWidth: 2, r: 4 }}
                      activeDot={{ r: 6 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
