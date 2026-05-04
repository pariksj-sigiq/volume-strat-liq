import * as React from "react";

import { cn } from "../../lib/utils";

export interface ProgressProps extends React.HTMLAttributes<HTMLDivElement> {
  value?: number | null;
}

const Progress = React.forwardRef<HTMLDivElement, ProgressProps>(
  ({ className, value = 0, ...props }, ref) => {
    const safeValue = Math.min(100, Math.max(0, Number(value) || 0));

    return (
      <div
        ref={ref}
        className={cn("ui-progress", className)}
        role="progressbar"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={safeValue}
        {...props}
      >
        <div className="ui-progress-indicator" style={{ transform: `translateX(-${100 - safeValue}%)` }} />
      </div>
    );
  },
);
Progress.displayName = "Progress";

export { Progress };
