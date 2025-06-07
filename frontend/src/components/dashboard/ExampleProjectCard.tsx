import { Button } from "@/components/ui/button";
import { ExternalLink, Github } from "lucide-react";

interface ExampleProjectCardProps {
  id: number | string;
  title: string;
  description: string;
  onClick?: () => void;
  icon?: React.ReactNode;
}

export const ExampleProjectCard = ({
  id,
  title,
  description,
  onClick,
  icon = <Github className="h-5 w-5 text-primary" />
}: ExampleProjectCardProps) => {
  return (
    <div
      className="bg-card border border-border rounded-lg hover:border-border/60 hover:shadow-md transition-all overflow-hidden group cursor-pointer"
      onClick={onClick}
    >
      <div className="p-5">
        <div className="flex items-start gap-3">
          <div className="flex-shrink-0 mt-0.5">
            <div className="p-2 rounded-md bg-primary/5 group-hover:bg-primary/10 transition-all">
              {icon}
            </div>
          </div>
          <div className="flex-1 min-w-0">
            <h3 className="font-medium text-base group-hover:text-primary transition-colors">{title}</h3>
            <p className="text-sm text-muted-foreground mt-1">{description}</p>
          </div>
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 -mt-1 -mr-2 group-hover:bg-primary/10 group-hover:text-primary transition-all"
          >
            <ExternalLink className="h-3.5 w-3.5 text-muted-foreground group-hover:h-4 group-hover:w-4 transition-all" />
          </Button>
        </div>
      </div>
    </div>
  );
};

export default ExampleProjectCard;
