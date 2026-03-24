import type { Plugin } from "@opencode-ai/plugin"

type Word = {
  cooked: string
  end: number
}

type Assignment = {
  key: string
  value: string
}

function isWhitespace(ch: string) {
  return /\s/.test(ch)
}

function skipWhitespace(input: string, index: number) {
  let i = index
  while (i < input.length && isWhitespace(input[i])) i++
  return i
}

function readShellWord(input: string, from: number): Word | null {
  let i = skipWhitespace(input, from)
  if (i >= input.length) return null

  let cooked = ""
  let mode: "normal" | "single" | "double" = "normal"

  while (i < input.length) {
    const ch = input[i]

    if (mode === "normal") {
      if (isWhitespace(ch)) break

      if (ch === "'") {
        mode = "single"
        i++
        continue
      }

      if (ch === '"') {
        mode = "double"
        i++
        continue
      }

      if (ch === "\\") {
        i++
        if (i < input.length) {
          cooked += input[i]
          i++
          continue
        }
        cooked += "\\"
        break
      }

      cooked += ch
      i++
      continue
    }

    if (mode === "single") {
      if (ch === "'") {
        mode = "normal"
        i++
        continue
      }
      cooked += ch
      i++
      continue
    }

    if (ch === '"') {
      mode = "normal"
      i++
      continue
    }

    if (ch === "\\") {
      i++
      if (i < input.length) {
        cooked += input[i]
        i++
        continue
      }
      cooked += "\\"
      break
    }

    cooked += ch
    i++
  }

  return {
    cooked,
    end: i,
  }
}

function isEnvAssignmentToken(token: string) {
  const eq = token.indexOf("=")
  if (eq <= 0) return false

  const key = token.slice(0, eq)
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(key)
}

function splitLeadingEnvAssignments(input: string) {
  let cursor = 0
  const assignments: Assignment[] = []

  while (true) {
    const word = readShellWord(input, cursor)
    if (!word) break
    if (!isEnvAssignmentToken(word.cooked)) break

    const eq = word.cooked.indexOf("=")
    assignments.push({
      key: word.cooked.slice(0, eq),
      value: word.cooked.slice(eq + 1),
    })

    cursor = word.end
  }

  const commandStart = skipWhitespace(input, cursor)
  const command = input.slice(commandStart).trim()

  return { assignments, command }
}

function enqueue<T>(queue: T[], item: T) {
  queue.push(item)
}

function dequeue<T>(queue: T[]) {
  if (queue.length === 0) return null
  return queue.shift() ?? null
}

export const NormalizeInlineEnvPlugin: Plugin = async () => {
  const pendingEnvQueue: Record<string, string>[] = []

  return {
    "tool.execute.before": async (input: any, output: any) => {
      if (input.tool !== "bash") return

      const original = output?.args?.command
      if (typeof original !== "string") return

      const trimmed = original.trim()
      if (!trimmed) return

      const { assignments, command } = splitLeadingEnvAssignments(trimmed)
      if (assignments.length === 0) return
      if (!command) {
        throw new Error("Command is empty after stripping leading env assignments")
      }

      enqueue(
        pendingEnvQueue,
        Object.fromEntries(assignments.map((item) => [item.key, item.value])),
      )

      output.args.command = command
    },

    "shell.env": async (_input: any, output: any) => {
      const nextEnv = dequeue(pendingEnvQueue)
      if (!nextEnv) return

      output.env = {
        ...(output.env ?? {}),
        ...nextEnv,
      }
    },
  }
}
