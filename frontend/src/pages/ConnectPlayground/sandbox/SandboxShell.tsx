import {
  Plug,
  Loader2,
  ChevronLeft,
  ChevronRight,
  RotateCw,
  Lock,
  Share,
  Plus,
} from "lucide-react";
import { cn } from "@/lib/utils";

interface SandboxShellProps {
  onOpenConnect: () => void;
  isLoading: boolean;
}

function BarButton({ children }: { children: React.ReactNode }) {
  return (
    <div className="w-7 h-7 flex items-center justify-center rounded-md text-foreground/20">
      {children}
    </div>
  );
}

export function SandboxShell({ onOpenConnect, isLoading }: SandboxShellProps) {
  return (
    <div className="shrink-0">
      {/* Browser frame */}
      <div className="w-full rounded-xl border border-border/60 shadow-sm overflow-hidden">
        {/* Tab bar */}
        <div className="flex items-center gap-2 px-3 pt-2.5 pb-0 bg-muted/80">
          <div className="flex gap-1.5 shrink-0 pl-0.5">
            <div className="w-3 h-3 rounded-full bg-[#ff5f57]" />
            <div className="w-3 h-3 rounded-full bg-[#febc2e]" />
            <div className="w-3 h-3 rounded-full bg-[#28c840]" />
          </div>
          {/* Active tab */}
          <div className="flex items-center gap-2 px-3 py-1.5 bg-muted rounded-t-lg min-w-0 max-w-[200px]">
            <div className="w-3.5 h-3.5 rounded bg-primary/30 flex items-center justify-center shrink-0">
              <span className="text-[7px] font-bold text-primary">A</span>
            </div>
            <span className="text-[11px] text-foreground/50 font-medium truncate select-none">
              Your App
            </span>
          </div>
          <BarButton><Plus className="h-3 w-3" /></BarButton>
        </div>

        {/* Toolbar */}
        <div className="flex items-center gap-1 px-2.5 py-1.5 bg-muted border-b border-border/40">
          <BarButton><ChevronLeft className="h-3.5 w-3.5" /></BarButton>
          <BarButton><ChevronRight className="h-3.5 w-3.5" /></BarButton>
          <BarButton><RotateCw className="h-3 w-3" /></BarButton>
          {/* Address bar */}
          <div className="flex-1 flex items-center justify-center gap-1.5 h-7 rounded-md bg-background px-3 mx-1 border border-border/30">
            <Lock className="h-2.5 w-2.5 text-foreground/25 shrink-0" />
            <span className="text-[11px] text-foreground/40 font-medium select-none truncate">
              your-app.com
            </span>
          </div>
          <BarButton><Share className="h-3 w-3" /></BarButton>
        </div>

        {/* Content area with CTA */}
        <div className="flex flex-col items-center justify-center py-10 px-6 bg-background">
          <span className="text-[10px] uppercase tracking-[0.15em] text-foreground/20 font-medium mb-4">
            Your application
          </span>

          <button
            onClick={onOpenConnect}
            disabled={isLoading}
            className={cn(
              "flex items-center gap-2.5 px-7 py-3 rounded-xl",
              "bg-primary text-primary-foreground",
              "text-sm font-semibold",
              "shadow-lg shadow-primary/20 hover:shadow-xl hover:shadow-primary/25",
              "hover:scale-[1.02] active:scale-[0.98]",
              "transition-all duration-150",
              "disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:scale-100",
            )}
          >
            {isLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Plug className="h-4 w-4" />
            )}
            Connect your apps
          </button>

          <p className="text-[11px] text-foreground/20 mt-3 text-center">
            Click to open the Connect modal
          </p>
        </div>
      </div>
    </div>
  );
}
