import { env } from "./config/env.js";
import { createApp } from "./app.js";
import { startIppFinalizerScheduler } from "./lib/ipp-finalizer.js";
import { startRedMonthCalendarScheduler } from "./lib/red-month-calendar.js";

const app = createApp();
app.listen(env.PORT, () => {
  console.log(`Backend running on port ${env.PORT}`);
  startRedMonthCalendarScheduler();
  startIppFinalizerScheduler();
});
