"use client";

import { ErrorRecovery } from "./error";

type GlobalErrorProps = {
  error: Error & { digest?: string };
  reset: () => void;
};

export default function GlobalError(props: GlobalErrorProps) {
  return (
    <html lang="en">
      <body>
        <ErrorRecovery {...props} />
      </body>
    </html>
  );
}
