import { cn } from "@/lib/utils";

export function BrandSeal({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 40 40"
      fill="none"
      aria-hidden="true"
      className={cn("size-9 shrink-0", className)}
    >
      <rect x="1.5" y="1.5" width="37" height="37" rx="4" className="fill-primary" />
      <rect
        x="4.5"
        y="4.5"
        width="31"
        height="31"
        rx="2.5"
        className="stroke-primary-foreground/40"
        strokeWidth="1"
      />
      <path d="M11 15h8v7h-5l-3 4v-4z" className="fill-primary-foreground" />
      <path d="M29 25h-8v-7h5l3-4v4z" className="fill-primary-foreground/60" />
    </svg>
  );
}

export function BrandWordmark({ className }: { className?: string }) {
  return (
    <span className={cn("flex items-center gap-3", className)}>
      <BrandSeal />
      <span className="flex flex-col leading-none">
        <span className="font-serif text-lg font-semibold tracking-tight text-foreground">
          Transhooter
        </span>
        <span className="mt-0.5 text-[10px] font-medium uppercase tracking-[0.18em] text-muted-foreground">
          Interpreted consultations
        </span>
      </span>
    </span>
  );
}
