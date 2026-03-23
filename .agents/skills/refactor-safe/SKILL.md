---
name: refactor-safe
description: Use when making internal changes while preserving behavior.
---

1. Preserve public API behavior unless explicitly asked otherwise.
2. Keep changes narrow and file-local when possible.
3. Avoid broad package restructuring.
4. Update only the minimum tests needed for the current feature.
5. Summarize risks and remaining gaps after edits.