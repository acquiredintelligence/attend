/**
 * `llmJudge` on Claude with structured output. The README shows this with `@anthropic-ai/sdk`;
 * this file declares the part of that client the judge reads, so it typechecks without the SDK.
 */
import { PermanentError } from 'attend';
import { llmJudge } from 'attend/llm';

interface Anthropic {
  messages: {
    create(
      body: {
        model: string;
        max_tokens: number;
        system: string;
        messages: { role: 'user'; content: string }[];
        output_config: { effort: 'low' | 'medium' | 'high' | 'xhigh' | 'max'; format: { type: 'json_schema'; schema: Record<string, unknown> } };
      },
      options: { signal: AbortSignal },
    ): Promise<{ stop_reason: string | null; content: ({ type: 'text'; text: string } | { type: 'thinking'; thinking: string })[] }>;
  };
}

export function claudeJudge(anthropic: Anthropic) {
  return llmJudge({
    complete: async ({ system, prompt, schema, signal }) => {
      const res = await anthropic.messages.create(
        {
          model: 'claude-opus-5-5',
          max_tokens: 1024,
          system,
          messages: [{ role: 'user', content: prompt }],
          output_config: { effort: 'low', format: { type: 'json_schema', schema } },
        },
        { signal },
      );
      if (res.stop_reason === 'refusal') throw new PermanentError('judge refused');
      const text = res.content.find((b) => b.type === 'text');
      return JSON.parse(text?.type === 'text' ? text.text : 'null');
    },
  });
}
