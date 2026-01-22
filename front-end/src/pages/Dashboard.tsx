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
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
  const [newTopic, setNewTopic] = useState({
    id: "",
    description: "",
    keywords: "",
    subreddits: "",
  });
  const [isDialogOpen, setIsDialogOpen] = useState(false);

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

  const handleCreateTopic = async () => {
    try {
      await api.createTopic({
        id: newTopic.id,
        description: newTopic.description,
        keywords: newTopic.keywords.split(",").map((k) => k.trim()),
        subreddits: newTopic.subreddits.split(",").map((s) => s.trim()),
        update_frequency_seconds: 60,
        is_active: true,
      });
      setIsDialogOpen(false);
      fetchData();
      setNewTopic({ id: "", description: "", keywords: "", subreddits: "" });
    } catch (error) {
        console.error("Failed to create topic:", error);
      alert("Failed to create topic");
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
                    <Dialog open={isDialogOpen} onOpenChange={setIsDialogOpen}>
                      <DialogTrigger asChild>
                        <Button size="sm" className="h-8 gap-1">
                            <Plus className="h-3.5 w-3.5" />
                            <span className="sr-only sm:not-sr-only sm:whitespace-nowrap">
                            Add Topic
                            </span>
                        </Button>
                      </DialogTrigger>
                      <DialogContent className="sm:max-w-[425px]">
                        <DialogHeader>
                          <DialogTitle>Add New Topic</DialogTitle>
                          <DialogDescription>
                            Create a new topic to track keywords across subreddits.
                          </DialogDescription>
                        </DialogHeader>
                        <div className="grid gap-4 py-4">
                          <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="id" className="text-right">
                              ID
                            </Label>
                            <Input
                              id="id"
                              value={newTopic.id}
                              onChange={(e) => setNewTopic({ ...newTopic, id: e.target.value })}
                              className="col-span-3"
                              placeholder="e.g., ai-agents"
                            />
                          </div>
                          <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="description" className="text-right">
                              Description
                            </Label>
                            <Input
                              id="description"
                              value={newTopic.description}
                              onChange={(e) => setNewTopic({ ...newTopic, description: e.target.value })}
                              className="col-span-3"
                              placeholder="Topic description"
                            />
                          </div>
                           <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="keywords" className="text-right">
                              Keywords
                            </Label>
                            <Input
                              id="keywords"
                              value={newTopic.keywords}
                              onChange={(e) => setNewTopic({ ...newTopic, keywords: e.target.value })}
                              className="col-span-3"
                              placeholder="comma, separated, keywords"
                            />
                          </div>
                           <div className="grid grid-cols-4 items-center gap-4">
                            <Label htmlFor="subreddits" className="text-right">
                              Subreddits
                            </Label>
                            <Input
                              id="subreddits"
                              value={newTopic.subreddits}
                              onChange={(e) => setNewTopic({ ...newTopic, subreddits: e.target.value })}
                              className="col-span-3"
                              placeholder="technology, artificial"
                            />
                          </div>
                        </div>
                        <DialogFooter>
                          <Button onClick={handleCreateTopic}>Create Topic</Button>
                        </DialogFooter>
                      </DialogContent>
                    </Dialog>
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
