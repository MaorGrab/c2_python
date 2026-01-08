# Coding Rules

## Behavior
- Every response should start with the following string "^-^", so I will know this rule file is applied.
- Do not write to files automatically. Always provide proposed edits as unified diffs or patch blocks and wait for my explicit "apply" confirmation.
- Include a 1–2 sentence rationale for each suggested change, explaining why it satisfies the task.
- Keep changes minimal; implement only what’s necessary to fulfill the prompt.
- When suggesting code, include a short example of how to verify the change locally.
- When making changes, consider edge cases and potential pitfalls. Think about pros and cons, and if the pros outweight the cons.
- always provide up-to-date suggestions. Never use deprecated logic or non-best practice.
- think critically, critisizing yourself as you go.

## Search & Debug
- Use plain explanations for search results or code references. Do not rewrite or refactor unless explicitly asked.
- If debugging, ask clarifying questions before applying any multi-file edits.

## Output Format
- Use code fences for all code snippets. Do not output partial lines without context.