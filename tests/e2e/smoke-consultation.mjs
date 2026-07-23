import { runConsultationBrowserWorkflow } from "./consultation-browser-workflow.mjs";
import { createConsultationHarness } from "./consultation-harness.mjs";

const workflow = await runConsultationBrowserWorkflow({
  harness: createConsultationHarness(),
  browserMode: "headless-smoke",
  employeeMedia: {
    video: "/workspace/tests/fixtures/consultation.y4m",
    audio: "/workspace/tests/fixtures/en-good-morning.wav",
  },
  customerMedia: {
    video: "/workspace/tests/fixtures/consultation.y4m",
    audio: "/workspace/tests/fixtures/de-guten-morgen.wav",
  },
});
await workflow.close();
