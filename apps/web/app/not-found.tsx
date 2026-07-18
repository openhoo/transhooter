import Link from "next/link";

export default function NotFound() {
  return (
    <section className="empty flat">
      <p className="eyebrow">Not found</p>
      <h1>This consultation view is unavailable.</h1>
      <p className="muted">It may have ended, been deleted, or not belong to your account.</p>
      <Link className="button" href="/consultations">
        Return to consultations
      </Link>
    </section>
  );
}
