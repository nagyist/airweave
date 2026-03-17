import { useState } from "react";
import { Check, Copy } from "lucide-react";
import { cn } from "@/lib/utils";
import { posthog } from "@/lib/posthog-provider";
import { useTheme } from "@/lib/theme-provider";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark } from "react-syntax-highlighter/dist/esm/styles/prism";

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

const highlightStyle = {
  ...oneDark,
  'pre[class*="language-"]': {
    ...(oneDark['pre[class*="language-"]'] as Record<string, unknown>),
    background: "transparent",
    margin: 0,
    padding: 0,
  },
  'code[class*="language-"]': {
    ...(oneDark['code[class*="language-"]'] as Record<string, unknown>),
    background: "transparent",
  },
};

export function SnippetFrame({ label, tabs }: SnippetFrameProps) {
  const [activeTab, setActiveTab] = useState(tabs[0]?.id ?? "");
  const [copied, setCopied] = useState(false);
  const { resolvedTheme } = useTheme();
  const isDark = resolvedTheme === "dark";

  const active = tabs.find((t) => t.id === activeTab) ?? tabs[0];

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
        "flex flex-col h-full rounded-xl overflow-hidden bg-[#0d1117]",
        isDark ? "ring-1 ring-border/40" : "",
      )}
    >
      {/* Tab bar */}
      <div className="flex items-center justify-between px-3 py-2 bg-[#161b22] border-b border-white/5 shrink-0">
        <div className="flex items-center gap-3">
          <span className="text-[10px] font-medium uppercase tracking-wider text-white/20">
            {label}
          </span>
          <div className="flex gap-0.5">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={cn(
                  "px-2.5 py-1 rounded-md text-xs font-medium transition-colors",
                  activeTab === tab.id
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
          className="text-white/20 hover:text-white/50 transition-colors p-1 shrink-0"
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
          style={highlightStyle}
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
