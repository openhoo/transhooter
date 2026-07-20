import { createRoute } from "@/app/api/_route";

export const dynamic = "force-dynamic";

export const GET = createRoute("consultations.list");
export const POST = createRoute("consultations.create");
