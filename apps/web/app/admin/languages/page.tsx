import type { Metadata } from "next";
import { LanguageAdmin } from "@/components/language-admin";
import { requirePageData } from "@/lib/server-application";

export const metadata: Metadata = { title: "Provider languages" };
export const dynamic = "force-dynamic";

type LanguagesPageData = {
  directions: React.ComponentProps<typeof LanguageAdmin>["directions"];
};

export default async function LanguagesPage() {
  const data = await requirePageData<LanguagesPageData>("admin.languages.list");

  return (
    <div className="flex flex-col gap-6">
      <header className="border-b border-border pb-5">
        <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Administration</p>
        <h1 className="mt-1 font-serif text-2xl font-semibold tracking-tight text-foreground md:text-3xl">Language administration</h1>
        <p className="mt-2 max-w-2xl text-sm leading-relaxed text-muted-foreground">
          Configure complete provider directions for new consultations. Every direction remains bound to its verified profile revision; ready and active rooms keep their frozen revision.
        </p>
      </header>
      <LanguageAdmin directions={data.directions} />
    </div>
  );
}
