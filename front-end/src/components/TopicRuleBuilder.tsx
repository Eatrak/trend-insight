import { useState } from "react";
import { Plus, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

interface TopicRuleBuilderProps {
  groups: string[][];
  onChange: (groups: string[][]) => void;
}

export function TopicRuleBuilder({ groups, onChange }: TopicRuleBuilderProps) {
  const [newTermInputs, setNewTermInputs] = useState<string[]>(
    groups.map(() => ""),
  );

  const handleAddGroup = () => {
    onChange([...groups, []]);
    setNewTermInputs([...newTermInputs, ""]);
  };

  const handleRemoveGroup = (groupIndex: number) => {
    const newGroups = groups.filter((_, i) => i !== groupIndex);
    const newInputs = newTermInputs.filter((_, i) => i !== groupIndex);
    onChange(newGroups);
    setNewTermInputs(newInputs);
  };

  const handleAddTerm = (groupIndex: number) => {
    const term = newTermInputs[groupIndex].trim();
    if (!term) return;

    if (groups[groupIndex].includes(term)) {
      // Avoid duplicates
      setNewTermInputs((prev) => {
        const next = [...prev];
        next[groupIndex] = "";
        return next;
      });
      return;
    }

    const newGroups = [...groups];
    newGroups[groupIndex] = [...newGroups[groupIndex], term];
    onChange(newGroups);

    setNewTermInputs((prev) => {
      const next = [...prev];
      next[groupIndex] = "";
      return next;
    });
  };

  const handleKeyDown = (e: React.KeyboardEvent, groupIndex: number) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleAddTerm(groupIndex);
    }
  };

  const handleRemoveTerm = (groupIndex: number, termIndex: number) => {
    const newGroups = [...groups];
    newGroups[groupIndex] = newGroups[groupIndex].filter(
      (_, i) => i !== termIndex,
    );
    onChange(newGroups);
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-medium">Matching Rules</h3>
      </div>

      {groups.length === 0 && (
        <div className="text-center p-6 border border-dashed rounded-lg text-muted-foreground text-sm">
          No rules defined. Content will never match.
          <br />
          <Button variant="link" onClick={handleAddGroup}>
            + Add First Rule
          </Button>
        </div>
      )}

      {groups.map((group, groupIdx) => (
        <div key={groupIdx} className="space-y-4">
          {groupIdx > 0 && (
            <div className="relative flex items-center py-2">
              <div className="flex-grow border-t border-muted"></div>
              <span className="flex-shrink-0 mx-2 text-[10px] font-bold text-muted-foreground uppercase tracking-widest">
                AND
              </span>
              <div className="flex-grow border-t border-muted"></div>
            </div>
          )}

          <Card className="relative overflow-hidden border-l-4 border-l-primary/70">
            <Button
              variant="ghost"
              size="icon"
              className="absolute top-2 right-2 h-6 w-6 text-muted-foreground hover:text-destructive"
              onClick={() => handleRemoveGroup(groupIdx)}
            >
              <X className="h-3 w-3" />
            </Button>

            <CardHeader className="py-3 px-4 bg-muted/30">
              <CardTitle className="text-xs font-semibold uppercase tracking-wider text-muted-foreground flex items-center gap-2">
                {groupIdx === 0 ? "First Rule (Base)" : "Next Rule"}
              </CardTitle>
            </CardHeader>

            <CardContent className="p-4 space-y-3">
              <div className="flex flex-wrap items-center gap-2">
                {group.map((term, termIdx) => (
                  <div key={termIdx} className="flex items-center gap-2">
                    {termIdx > 0 && (
                      <span className="text-[10px] font-bold text-muted-foreground uppercase">
                        OR
                      </span>
                    )}
                    <Badge
                      variant="secondary"
                      className="px-2 py-1 gap-1 text-sm font-normal"
                    >
                      {term}
                      <button
                        type="button"
                        className="text-muted-foreground hover:text-destructive transition-colors focus:outline-none"
                        onClick={(e) => {
                          e.stopPropagation();
                          handleRemoveTerm(groupIdx, termIdx);
                        }}
                      >
                        <X className="h-3 w-3" />
                      </button>
                    </Badge>
                  </div>
                ))}
                {group.length === 0 && (
                  <span className="text-sm text-muted-foreground italic">
                    No terms yet (won't match anything)
                  </span>
                )}
              </div>

              <div className="flex gap-2 items-center">
                {group.length > 0 && (
                  <span className="text-[10px] font-bold text-muted-foreground uppercase">
                    OR
                  </span>
                )}
                <Input
                  placeholder="Add term..."
                  value={newTermInputs[groupIdx]}
                  onChange={(e) => {
                    const val = e.target.value;
                    setNewTermInputs((prev) => {
                      const next = [...prev];
                      next[groupIdx] = val;
                      return next;
                    });
                  }}
                  onKeyDown={(e) => handleKeyDown(e, groupIdx)}
                  className="h-8 text-sm"
                />
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-8 px-2"
                  onClick={() => handleAddTerm(groupIdx)}
                >
                  <Plus className="h-4 w-4" />
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      ))}

      <Button
        onClick={handleAddGroup}
        variant="outline"
        className="w-full border-dashed gap-2 text-muted-foreground hover:text-primary"
      >
        <Plus className="h-4 w-4" />
        Add AND rule
      </Button>
    </div>
  );
}
