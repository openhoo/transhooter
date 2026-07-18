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
    <div className="stack">
      <div>
        <p className="eyebrow">Provider capability catalog</p>
        <h1>Languages</h1>
        <p className="lede">
          Enable only complete, freshly probed directions. Existing ready and active rooms keep
          their frozen revision.
        </p>
      </div>
      <LanguageAdmin directions={data.directions} />
    </div>
  );
}
