import { createRoute } from "@/app/api/_route";

export const dynamic = "force-dynamic";

export const GET = createRoute("admin.languages.list");
export const POST = createRoute("admin.languages.update");
