import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { Plus, Activity, ArrowRight } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { api, type Topic } from "@/services/api";

export default function Dashboard() {
  const [topics, setTopics] = useState<Topic[]>([]);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      const topicsData = await api.getTopics();
      setTopics(topicsData);
    } catch (error) {
      console.error("Failed to fetch data:", error);
    }
  };

  return (
    <div className="flex min-h-screen w-full flex-col bg-muted/40">
      <div className="flex flex-col sm:gap-4 sm:py-4 sm:pl-14">
        <main className="grid flex-1 items-start gap-4 p-4 sm:px-6 sm:py-0 md:gap-8">
          

          {/* Topics Management Section */}
          <div className="grid gap-4 md:gap-8 lg:grid-cols-1 xl:grid-cols-1">
            <Card className="xl:col-span-2">
              <CardHeader className="flex flex-row items-center">
                <div className="grid gap-2">
                  <CardTitle>Tracked Topics</CardTitle>
                  <CardDescription>
                    Manage the topics you want to monitor on Reddit.
                  </CardDescription>
                </div>
                <div className="ml-auto gap-1">
                      <Button size="sm" className="h-8 gap-1" asChild>
                          <Link to="/topics/new">
                            <Plus className="h-3.5 w-3.5" />
                            <span className="sr-only sm:not-sr-only sm:whitespace-nowrap">
                            Add Topic
                            </span>
                          </Link>
                      </Button>
                </div>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Topic ID</TableHead>
                      <TableHead className="hidden xl:table-column">Type</TableHead>
                      <TableHead className="hidden xl:table-column">Status</TableHead>
                      <TableHead className="hidden xl:table-column">Date</TableHead>
                      <TableHead className="text-right">Action</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {topics.map((topic) => (
                      <TableRow key={topic.id}>
                        <TableCell>
                          <div className="font-medium">
                            <Link to={`/topics/${topic.id}`} className="hover:underline flex items-center gap-2">
                                <Activity className="h-4 w-4" />
                                {topic.id}
                            </Link>
                           </div>
                          <div className="hidden text-sm text-muted-foreground md:inline">
                            {topic.description}
                          </div>
                        </TableCell>
                        <TableCell className="hidden xl:table-column">
                          Sale
                        </TableCell>
                        <TableCell className="hidden xl:table-column">
                          <span className="inline-flex items-center rounded-md bg-green-50 px-2 py-1 text-xs font-medium ring-1 ring-inset ring-green-600/20">
                            Active
                          </span>
                        </TableCell>
                        <TableCell className="hidden md:table-cell lg:hidden xl:table-column">
                          2023-06-23
                        </TableCell>
                        <TableCell className="text-right">
                            <Button asChild size="sm" variant="ghost">
                                <Link to={`/topics/${topic.id}`}>
                                    Open
                                    <ArrowRight className="ml-2 h-4 w-4" />
                                </Link>
                            </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </div>
        </main>
      </div>
    </div>
  );
}
