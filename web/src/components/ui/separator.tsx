import * as React from "react";

import { cn } from "../../lib/utils";

export interface SeparatorProps extends React.HTMLAttributes<HTMLDivElement> {
  orientation?: "horizontal" | "vertical";
}

const Separator = React.forwardRef<HTMLDivElement, SeparatorProps>(
  ({ className, orientation = "horizontal", ...props }, ref) => (
    <div
      ref={ref}
      className={cn("ui-separator", orientation === "vertical" && "ui-separator-vertical", className)}
      role="separator"
      aria-orientation={orientation}
      {...props}
    />
  ),
);
Separator.displayName = "Separator";

export { Separator };
