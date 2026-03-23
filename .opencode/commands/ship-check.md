---
description: Run a pre-commit quality pass
agent: build
---

For the current change:
1. inspect git diff
2. run relevant Go tests if appropriate
3. identify missing tests or scope creep
4. produce a release-risk summary