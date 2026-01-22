import { Link, Outlet, useLocation } from "react-router-dom";
import { Home, PanelLeft } from "lucide-react";

import { ThemeProvider } from "@/components/theme-provider";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

export default function Layout() {
  const location = useLocation();

  const isActive = (path: string) => location.pathname === path;

  const NavLink = ({ to, icon: Icon, label }: { to: string; icon: any; label: string }) => {
     const active = isActive(to);
     return (
        <Tooltip>
            <TooltipTrigger asChild>
                <Link
                    to={to}
                    className={`flex h-9 w-9 items-center justify-center rounded-lg transition-colors md:h-8 md:w-8 ${
                        active 
                        ? "bg-accent text-accent-foreground" 
                        : "text-muted-foreground hover:text-foreground"
                    }`}
                >
                    <Icon className="h-5 w-5" />
                    <span className="sr-only">{label}</span>
                </Link>
            </TooltipTrigger>
            <TooltipContent side="right">{label}</TooltipContent>
        </Tooltip>
     );
  };

  return (
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      <TooltipProvider>
        <div className="flex min-h-screen w-full flex-col bg-muted/40">
            {/* Desktop Sidebar */}
            <aside className="fixed inset-y-0 left-0 z-10 hidden w-14 flex-col border-r bg-background sm:flex">
                <nav className="flex flex-col items-center gap-4 px-2 sm:py-5">
                    <NavLink to="/" icon={Home} label="Topics" />
                </nav>
            </aside>

            {/* Mobile Header & Content Area */}
            <div className="flex flex-col sm:gap-4 sm:pl-14">
                <header className="sticky top-0 z-30 flex h-14 items-center gap-4 border-b bg-background px-4 sm:static sm:h-auto sm:border-0 sm:bg-transparent sm:px-6">
                    <Sheet>
                        <SheetTrigger asChild>
                            <Button size="icon" variant="outline" className="sm:hidden">
                                <PanelLeft className="h-5 w-5" />
                                <span className="sr-only">Toggle Menu</span>
                            </Button>
                        </SheetTrigger>
                        <SheetContent side="left" className="sm:max-w-xs">
                            <nav className="grid gap-6 text-lg font-medium">
                                <Link
                                    to="/"
                                    className="flex items-center gap-4 px-2.5 text-muted-foreground hover:text-foreground"
                                >
                                    <Home className="h-5 w-5" />
                                    Topics
                                </Link>
                            </nav>
                        </SheetContent>
                    </Sheet>
                    
                    {/* Breadcrumb / Title Placeholder for Mobile */}
                    <div className="sm:hidden font-semibold text-lg">Reddit Insight</div>

                </header>
                <main className="p-4 sm:p-0">
                    <Outlet />
                </main>
            </div>
        </div>
      </TooltipProvider>
    </ThemeProvider>
  );
}
