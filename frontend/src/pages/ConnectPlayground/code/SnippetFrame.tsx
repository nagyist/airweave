import { useState, useMemo } from "react";
import { Check, Copy } from "lucide-react";
import { cn } from "@/lib/utils";
import { posthog } from "@/lib/posthog-provider";
import { useTheme } from "@/lib/theme-provider";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark, oneLight } from "react-syntax-highlighter/dist/esm/styles/prism";

export interface Tab {
  id: string;
  label: string;
  code: string;
  language?: string;
}

interface SnippetFrameProps {
  label: string;
  tabs: Tab[];
}

function makeTransparent(base: Record<string, unknown>) {
  return {
    ...base,
    'pre[class*="language-"]': {
      ...(base['pre[class*="language-"]'] as Record<string, unknown>),
      background: "transparent",
      margin: 0,
      padding: 0,
    },
    'code[class*="language-"]': {
      ...(base['code[class*="language-"]'] as Record<string, unknown>),
      background: "transparent",
    },
  };
}

const darkStyle = makeTransparent(oneDark);
const lightStyle = makeTransparent(oneLight);

export function SnippetFrame({ label, tabs }: SnippetFrameProps) {
  const [activeTab, setActiveTab] = useState(tabs[0]?.id ?? "");
  const [copied, setCopied] = useState(false);
  const { resolvedTheme } = useTheme();
  const isDark = resolvedTheme === "dark";

  const active = tabs.find((t) => t.id === activeTab) ?? tabs[0];
  const syntaxStyle = useMemo(() => (isDark ? darkStyle : lightStyle), [isDark]);

  const handleCopy = () => {
    if (!active) return;
    navigator.clipboard.writeText(active.code);
    posthog.capture("connect_code_copied", {
      snippet_type: label.toLowerCase(),
      language: active.id,
    });
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  return (
    <div
      className={cn(
        "flex flex-col h-full rounded-xl overflow-hidden border",
        isDark
          ? "bg-muted/50 border-border/50"
          : "bg-[#0d1117] border-transparent",
      )}
    >
      {/* Tab bar */}
      <div
        className={cn(
          "flex items-center justify-between px-3 py-2 border-b shrink-0",
          isDark
            ? "bg-muted/80 border-border/40"
            : "bg-[#161b22] border-white/5",
        )}
      >
        <div className="flex items-center gap-3">
          <span
            className={cn(
              "text-[10px] font-medium uppercase tracking-wider",
              isDark ? "text-foreground/25" : "text-white/20",
            )}
          >
            {label}
          </span>
          <div className="flex gap-0.5">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={cn(
                  "px-2.5 py-1 rounded-md text-xs font-medium transition-colors",
                  isDark
                    ? activeTab === tab.id
                      ? "bg-foreground/10 text-foreground/70"
                      : "text-foreground/30 hover:text-foreground/50"
                    : activeTab === tab.id
                      ? "bg-white/10 text-white/70"
                      : "text-white/30 hover:text-white/50",
                )}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </div>
        <button
          onClick={handleCopy}
          className={cn(
            "transition-colors p-1 shrink-0",
            isDark
              ? "text-foreground/20 hover:text-foreground/50"
              : "text-white/20 hover:text-white/50",
          )}
        >
          {copied ? (
            <Check className="h-3.5 w-3.5 text-emerald-400" />
          ) : (
            <Copy className="h-3.5 w-3.5" />
          )}
        </button>
      </div>

      {/* Code */}
      <div className="flex-1 overflow-auto px-3 py-2">
        <SyntaxHighlighter
          language={active?.language ?? "javascript"}
          style={syntaxStyle}
          customStyle={{
            fontSize: "0.75rem",
            lineHeight: "1.6",
            background: "transparent",
            margin: 0,
            padding: 0,
          }}
          codeTagProps={{
            style: {
              fontSize: "0.75rem",
              fontFamily:
                'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
            },
          }}
          wrapLongLines={false}
          showLineNumbers={false}
        >
          {active?.code ?? ""}
        </SyntaxHighlighter>
      </div>
    </div>
  );
}
