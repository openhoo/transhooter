import { createConsultationHarness } from "./consultation-harness.mjs";
import { runConsultationBrowserWorkflow } from "./consultation-browser-workflow.mjs";

function requiredEnvironment(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

let workflow;
try {
  const harness = createConsultationHarness();
  workflow = await runConsultationBrowserWorkflow({
    harness,
    browserMode: "headed-demo",
    employeeMedia: {
      video: requiredEnvironment("E2E_EMPLOYEE_VIDEO_FILE"),
      audio: requiredEnvironment("E2E_EMPLOYEE_AUDIO_FILE"),
    },
    customerMedia: {
      video: requiredEnvironment("E2E_CUSTOMER_VIDEO_FILE"),
      audio: requiredEnvironment("E2E_CUSTOMER_AUDIO_FILE"),
    },
  });
  console.log(`Provider profile: ${harness.expectedProfile}`);
  console.log(`Archive URL: ${workflow.archiveUrl}`);
  console.log(`Proof: ${JSON.stringify(workflow.proof)}`);
} finally {
  await workflow?.close();
}
