import React from "react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Label } from "@/components/ui/label";

export type SearchType = "vector" | "graph" | "hybrid";

interface SearchTypeSelectorProps {
  value: SearchType;
  onChange: (value: SearchType) => void;
  disabled?: boolean;
}

export function SearchTypeSelector({ value, onChange, disabled = false }: SearchTypeSelectorProps) {
  return (
    <div className="space-y-2">
      <Label htmlFor="search-type">Search Type</Label>
      <Select
        value={value}
        onValueChange={(val) => onChange(val as SearchType)}
        disabled={disabled}
      >
        <SelectTrigger id="search-type" className="w-full">
          <SelectValue placeholder="Select search type" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="vector">Vector Search</SelectItem>
          <SelectItem value="graph">Graph Search</SelectItem>
          <SelectItem value="hybrid">Hybrid Search</SelectItem>
        </SelectContent>
      </Select>
      <p className="text-xs text-muted-foreground">
        {value === "vector" && "Search using embeddings for semantic similarity"}
        {value === "graph" && "Search using graph relationships between entities"}
        {value === "hybrid" && "Combine vector and graph search for comprehensive results"}
      </p>
    </div>
  );
} 