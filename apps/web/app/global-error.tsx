"use client";

import { ErrorRecovery } from "./error";

type GlobalErrorProps = {
  error: Error & { digest?: string };
  reset: () => void;
};

export default function GlobalError(props: GlobalErrorProps) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-background p-4 text-foreground sm:p-8">
        <ErrorRecovery {...props} />
      </body>
    </html>
  );
}
