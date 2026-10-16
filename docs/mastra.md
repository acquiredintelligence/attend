# Give a Mastra agent attention

This guide puts attend in front of a [Mastra](https://mastra.ai) agent. attend decides when your app's events are worth the agent's time, and one small sink turns each decision into an `agent.generate` call or into context for the next call. It was checked against `@mastra/core` 1.78.

Install both packages:

```sh
npm install @acquiredintelligence/attend @mastra/core
```

## Wire the agent to a sink

```ts
import { Agent } from '@mastra/core/agent';
import { createAttend, defaultNotice } from '@acquiredintelligence/attend';
import { jevJudge } from '@acquiredintelligence/attend/jev';

const agent = new Agent({
  id: 'repo-agent',
  name: 'Repo agent',
  instructions: 'You help the team keep the main branch green.',
  model: 'openai/gpt-5-mini',
});

const notes: string[] = [];

const attend = createAttend({
  kind: { 'comment.created': 'flush', 'ci.test_failed': 'closing', 'deploy.finished': 'closing' },
  judge: jevJudge({ apiKey: process.env.TYPESAFE_API_KEY ?? '' }),
  sink: async (decision) => {
    const notice = defaultNotice(decision);
    if (decision.route === 'note') {
      notes.push(notice);
      return;
    }
    const { text } = await agent.generate(notice, { system: notes.splice(0).join('\n\n') });
    console.log(`[${decision.route}] ${text}`);
  },
});

void attend.run();

// Where your app already handles these events, such as a webhook handler or a CI callback:
await attend.push('repo:acme/shop', { name: 'ci.test_failed', text: 'checkout applies the coupon: failed' });
await attend.push('repo:acme/shop', { name: 'comment.created', text: 'sam: can you look?' });
```

- `interrupt`, `queue` and `ask` call the agent with the notice. If your app shows the user a prompt, send `ask` there instead.
- `note` is kept and handed to the next call as a system message, so background events inform the agent without waking it.
- `jevJudge` needs a TypeSafe key. Any [judge](judges.md) works here, including a plain function while you build.

The failed test waits in the window. The comment settles it, and the agent hears both lines in one call.
