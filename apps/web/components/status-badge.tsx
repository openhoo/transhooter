import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

const statusStyles: Record<string, string> = {
  scheduled: "border-transparent bg-secondary text-secondary-foreground",
  lobby: "border-transparent bg-primary text-primary-foreground",
  "in-progress": "border-transparent bg-verified text-verified-foreground",
  active: "border-transparent bg-verified text-verified-foreground",
  finalizing: "border-transparent bg-muted text-muted-foreground",
  archived: "border-border bg-transparent text-foreground",
  cancelled: "border-border bg-transparent text-muted-foreground line-through",
};

function statusLabel(status: string) {
  return status
    .split(/[-_]/u)
    .filter(Boolean)
    .map((part) => `${part.charAt(0).toUpperCase()}${part.slice(1)}`)
    .join(" ");
}

export function StatusBadge({ status, className }: { status: string; className?: string }) {
  return (
    <Badge
      variant="outline"
      className={cn("font-medium", statusStyles[status] ?? "border-border bg-transparent", className)}
    >
      {statusLabel(status)}
    </Badge>
  );
}
