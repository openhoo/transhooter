"use client";

type ErrorPageProps = {
  error: Error & { digest?: string };
  reset: () => void;
};

export function ErrorRecovery({ reset }: ErrorPageProps) {
  return (
    <section className="empty flat">
      <p className="eyebrow">Data unavailable</p>
      <h1>This view could not be loaded.</h1>
      <p className="muted">
        The consultation service did not return verified data. No media or consent state was
        changed.
      </p>
      <button className="button" type="button" onClick={reset}>
        Try again
      </button>
    </section>
  );
}

export default ErrorRecovery;
