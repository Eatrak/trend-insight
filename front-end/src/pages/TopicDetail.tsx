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
import dayjs from "dayjs";
import isoWeek from "dayjs/plugin/isoWeek";
import utc from "dayjs/plugin/utc";

dayjs.extend(isoWeek);
dayjs.extend(utc);

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
  const [selectedRange, setSelectedRange] = useState<string>("this_week");
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

  // Auto-select sensible range when window changes
  useEffect(() => {
    if (selectedWindow === "1m") {
      // For monthly view, default to "Last 12 Months"
      if (
        selectedRange !== "3m" &&
        selectedRange !== "6m" &&
        selectedRange !== "12m"
      ) {
        setSelectedRange("12m");
      }
    } else if (selectedWindow === "1w") {
      // For weekly view, default to "This Month"
      if (
        selectedRange !== "this_month" &&
        selectedRange !== "3m" &&
        selectedRange !== "6m"
      ) {
        setSelectedRange("this_month");
      }
    } else {
      // For daily, default to "This Week"
      if (["this_month", "3m", "6m", "12m"].includes(selectedRange as string)) {
        setSelectedRange("this_week");
      }
    }
  }, [selectedWindow]);

  // Always poll every 5s
  useInterval(refreshData, 5000);

  if (!topic) {
    return <div className="p-8">Loading topic...</div>;
  }

  // ... (rest of filtering) ...

  // Fixed window types (Resolution)
  const ALL_WINDOW_TYPES = ["1d", "1w", "1m"];
  const availableWindows = new Set(metrics.map((m) => m.window_type));

  // Dynamic Time Ranges based on Window
  let availableRanges = [
    { label: "This Week", value: "this_week" },
    { label: "This Month", value: "this_month" },
    { label: "Last 3 Months", value: "3m" },
  ];

  if (selectedWindow === "1w") {
    availableRanges = [
      { label: "This Month", value: "this_month" },
      { label: "Last 3 Months", value: "3m" },
      { label: "Last 6 Months", value: "6m" },
    ];
  } else if (selectedWindow === "1m") {
    availableRanges = [
      { label: "Last 3 Months", value: "3m" },
      { label: "Last 6 Months", value: "6m" },
      { label: "Last 12 Months", value: "12m" },
    ];
  }

  // Filter metrics by selected window (Resolution)
  const filteredMetrics = metrics.filter(
    (m) => m.window_type === selectedWindow,
  );

  // 1. Determine Step Size based on Window
  let stepUnit: "day" | "week" | "month" = "day";
  let stepValue = 1;

  if (selectedWindow === "1w") {
    stepUnit = "week";
  } else if (selectedWindow === "1m") {
    stepUnit = "month";
  }

  // Filter and Time Range (Zoom)
  // Loop range cutoff logic
  // Filter and Time Range (Zoom)
  // Loop range cutoff logic
  const now = dayjs.utc();
  let rangeCutoff = now.startOf("day");

  if (selectedRange === "this_week") {
    rangeCutoff = now.startOf("isoWeek");
  } else if (selectedRange === "this_month") {
    rangeCutoff = now.startOf("month");
  } else if (selectedRange === "3m") {
    rangeCutoff = now.startOf("month").subtract(2, "month");
  } else if (selectedRange === "6m") {
    rangeCutoff = now.startOf("month").subtract(5, "month");
  } else if (selectedRange === "12m") {
    rangeCutoff = now.startOf("month").subtract(11, "month");
  }

  // 2. Generate Complete Timeline (Zero-Filling)
  // We align chart points to the known metrics for accuracy, filling gaps with 0.
  const chartData = [];
  let runner = rangeCutoff;
  // End Time: Extend to the end of the current unit to show future empty buckets
  let rangeEnd = now.endOf("day");
  if (selectedRange === "this_week") {
    rangeEnd = now.endOf("isoWeek");
  } else if (
    ["this_month", "3m", "6m", "12m"].includes(selectedRange as string)
  ) {
    rangeEnd = now.endOf("month");
  }

  while (runner.isBefore(rangeEnd) || runner.isSame(rangeEnd)) {
    // Correct Matching Logic: Does the Metric belong to this Bar?
    // Bar Interval: [runner, runner + step)
    // Metric Point: mStart
    // We match if the Metric started within this bar's timeframe.
    const runnerEnd = runner.add(stepValue, stepUnit);

    const match = filteredMetrics.find((m) => {
      const mStart = dayjs.utc(m.start);
      return (
        (mStart.isSame(runner) || mStart.isAfter(runner)) &&
        mStart.isBefore(runnerEnd)
      );
    });

    chartData.push({
      end: runner.valueOf(), // recharts works well with timestamps
      mentions: match ? match.mentions : 0,
      engagement: match ? match.engagement : 0,
      sentiment: match ? match.sentiment || 0 : 0,
      growth: match ? match.growth || 0 : 0,
    });

    runner = runner.add(stepValue, stepUnit);
  }

  // Calculate Weighted Average Sentiment for the current view
  const visibleMetrics = metrics.filter(
    (m) =>
      m.window_type === "1d" &&
      dayjs(m.start).isAfter(rangeCutoff.subtract(1, "second")) &&
      dayjs(m.start).isBefore(now),
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
              {availableRanges.map((r) => {
                const isActive = selectedRange === r.value;
                return (
                  <button
                    key={r.value}
                    onClick={() => setSelectedRange(r.value)}
                    className={`px-3 py-1 text-sm font-medium rounded-md transition-colors ${
                      isActive
                        ? "bg-secondary text-secondary-foreground"
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
                      tickFormatter={(val) => dayjs(val).utc().format("MMM D")}
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
                        dayjs(label).utc().format("MMM D, YYYY h:mm A")
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
                      tickFormatter={(val) => dayjs(val).utc().format("MMM D")}
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
                        dayjs(label).utc().format("MMM D, YYYY h:mm A")
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
              <CardTitle>Average Sentiment</CardTitle>
            </CardHeader>

            <CardContent className="pl-2">
              <div className="h-[300px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData}>
                    <XAxis
                      dataKey="end"
                      tickFormatter={(val) => dayjs(val).utc().format("MMM D")}
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
                      domain={["auto", "auto"]}
                      tickFormatter={(value) => value.toFixed(2)}
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
                        dayjs(label).utc().format("MMM D, YYYY h:mm A")
                      }
                      formatter={(value: any) => [
                        `${getSentimentEmoji(Number(value))} ${Number(value).toFixed(2)}`,
                        "Average Sentiment",
                      ]}
                    />
                    {/* Reference Line at 0 for neutral sentiment */}
                    <Line
                      type="linear"
                      dataKey="sentiment"
                      name="Average Sentiment"
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
                      domain={[rangeCutoff.valueOf(), now.valueOf()]}
                      tickFormatter={(val) => dayjs(val).utc().format("MMM D")}
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
                        dayjs(label).utc().format("MMM D, YYYY h:mm A")
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
