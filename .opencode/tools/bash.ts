import { tool } from "@opencode-ai/plugin"

type Word = {
  raw: string
  cooked: string
  start: number
  end: number
}

function isWhitespace(ch: string) {
  return /\s/.test(ch)
}

function skipWhitespace(input: string, index: number) {
  let i = index
  while (i < input.length && isWhitespace(input[i])) i++
  return i
}

/**
 * Read one shell word with simple support for:
 * - spaces
 * - single quotes
 * - double quotes
 * - backslash escaping
 *
 * This is not a full POSIX shell parser.
 * It is intended to reliably detect leading KEY=VALUE tokens.
 */
function readShellWord(input: string, from: number): Word | null {
  let i = skipWhitespace(input, from)
  if (i >= input.length) return null

  const start = i
  let raw = ""
  let cooked = ""
  let mode: "normal" | "single" | "double" = "normal"

  while (i < input.length) {
    const ch = input[i]

    if (mode === "normal") {
      if (isWhitespace(ch)) break

      raw += ch

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
          raw += input[i]
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
      raw += ch
      if (ch === "'") {
        mode = "normal"
        i++
        continue
      }
      cooked += ch
      i++
      continue
    }

    raw += ch

    if (ch === '"') {
      mode = "normal"
      i++
      continue
    }

    if (ch === "\\") {
      i++
      if (i < input.length) {
        raw += input[i]
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
    raw,
    cooked,
    start,
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
  const assignments: Array<{ raw: string; key: string; value: string }> = []

  while (true) {
    const word = readShellWord(input, cursor)
    if (!word) break
    if (!isEnvAssignmentToken(word.cooked)) break

    const eq = word.cooked.indexOf("=")
    assignments.push({
      raw: word.raw,
      key: word.cooked.slice(0, eq),
      value: word.cooked.slice(eq + 1),
    })

    cursor = word.end
  }

  const commandStart = skipWhitespace(input, cursor)
  const command = input.slice(commandStart).trim()

  return { assignments, command }
}

function buildNormalizedScript(
  assignments: Array<{ raw: string }>,
  command: string,
) {
  const exportLines = assignments.map((item) => `export ${item.raw}`)
  return [...exportLines, command].join("\n")
}

function formatPreview(
  assignments: Array<{ key: string; value: string }>,
  command: string,
) {
  const lines: string[] = []

  if (assignments.length > 0) {
    lines.push(
      `[normalized-env] ${assignments
        .map((x) => `${x.key}=${JSON.stringify(x.value)}`)
        .join(" ")}`,
    )
  }

  lines.push(`$ ${command}`)
  return lines.join("\n")
}

export default tool({
  description:
    "Normalize leading KEY=VALUE prefixes into export statements, then run the real command",

  args: {
    command: tool.schema.string().describe("Shell command to execute"),
  },

  async execute(args, context) {
    const original = (args.command || "").trim()
    if (!original) {
      return "blocked: empty command"
    }

    const { assignments, command } = splitLeadingEnvAssignments(original)

    if (!command) {
      return "blocked: command is empty after stripping leading env assignments"
    }

    const script = buildNormalizedScript(assignments, command)

    const proc = Bun.spawn({
      cmd: ["bash", "-lc", script],
      cwd: context.directory,
      env: {
        ...process.env,
      },
      stdout: "pipe",
      stderr: "pipe",
    })

    const stdoutPromise = new Response(proc.stdout).text()
    const stderrPromise = new Response(proc.stderr).text()
    const exitCodePromise = proc.exited

    const [stdout, stderr, exitCode] = await Promise.all([
      stdoutPromise,
      stderrPromise,
      exitCodePromise,
    ])

    const out: string[] = []
    out.push(formatPreview(assignments, command))

    if (stdout.trim()) {
      out.push(stdout.trimEnd())
    }

    if (stderr.trim()) {
      out.push(stderr.trimEnd())
    }

    out.push(`[exit_code] ${exitCode}`)

    return out.join("\n\n")
  },
})
