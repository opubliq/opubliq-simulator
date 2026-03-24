import { useEffect, useMemo, useState } from 'react'
import './App.css'

const RAW_SUPABASE_URL = import.meta.env.VITE_SUPABASE_URL as string | undefined
const SUPABASE_PUBLISHABLE_KEY =
  (import.meta.env.VITE_SUPABASE_ANON_KEY ??
    import.meta.env.VITE_SUPABASE_PUBLISHABLE_KEY ??
    '') as string

function normalizeSupabaseUrl(url: string | undefined) {
  if (!url) {
    return ''
  }

  return url
    .trim()
    .replace(/\/+$/, '')
    .replace(/\/functions\/v1$/, '')
}

/**
 * Normalize Unicode text to NFC (Composed) form.
 * This ensures consistent representation of accented characters and special symbols.
 * For example: "é" (decomposed: e + ́) becomes "é" (composed)
 */
function normalizeText(text: string): string {
  return text.normalize('NFC')
}

const SUPABASE_URL = normalizeSupabaseUrl(RAW_SUPABASE_URL)

function fnUrl(name: string) {
  return `${SUPABASE_URL}/functions/v1/${name}`
}

type PipelineStep =
  | 'idle'
  | 'step1_semantic_search'
  | 'step2_fetch_predictions'
  | 'step3_llm_sampling'
  | 'step4_aggregate'
  | 'success'
  | 'error'

const STEP_LABELS: Record<PipelineStep, string> = {
  idle: '',
  step1_semantic_search: 'Recherche sémantique...',
  step2_fetch_predictions: 'Chargement des prédictions historiques...',
  step3_llm_sampling: 'Simulation des strates...',
  step4_aggregate: 'Agrégation des résultats...',
  success: 'Simulation terminée.',
  error: '',
}

const PIPELINE_FLOW: PipelineStep[] = [
  'step1_semantic_search',
  'step2_fetch_predictions',
  'step3_llm_sampling',
  'step4_aggregate',
]

interface LlmStrateResult {
  strate_age_group: string
  strate_langue: string
  strate_region: string
  strate_genre: string
  llm_response: Record<string, unknown> | null
  had_prior: boolean
  error: string | null
}

interface Step3Progress {
  completed: number
  total: number
}

function formatStep3ProgressLabel(progress: Step3Progress | null): string | null {
  if (!progress || progress.total <= 0) {
    return null
  }

  const completed = Math.min(Math.max(progress.completed, 0), progress.total)
  const percent = Math.round((completed / progress.total) * 100)
  const strateWord = completed === 1 ? 'strate' : 'strates'
  return `${completed} ${strateWord} / ${progress.total} (${percent}%)`
}

function getStepLabel(step: PipelineStep, step3Progress: Step3Progress | null): string {
  if (step !== 'step3_llm_sampling') {
    return STEP_LABELS[step]
  }

  const progressLabel = formatStep3ProgressLabel(step3Progress)
  if (!progressLabel) {
    return STEP_LABELS[step]
  }

  return `Simulation des strates… ${progressLabel}`
}

export interface SimulationResult {
  question: string
  question_type: 'multinomial' | 'numeric'
  national_distribution: Record<string, number>
  national_margin_of_error: number
  strate_results: {
    strate_age_group: string
    strate_langue: string
    strate_region: string
    strate_genre: string
    weight: number | null
    llm_response: Record<string, unknown> | null
    had_prior: boolean
    error: string | null
  }[]
  meta: {
    total_strates: number
    successful_strates: number
    failed_strates: number
  }
}

interface ApiCallLog {
  step: string
  status: number | null
  duration_ms: number
  request_payload: unknown
  response_payload: unknown
  error: string | null
}

interface PipelineExecutionLog {
  semantic_search: ApiCallLog | null
  fetch_strate_predictions: ApiCallLog | null
  llm_prompt_dry_run: ApiCallLog | null
  llm_sampling: ApiCallLog | null
  aggregate_final_distribution: ApiCallLog | null
}

interface SimulationLogEntry {
  id: string
  created_at: string
  status: 'success' | 'error'
  question: string
  context: string
  choices: string[]
  error_message: string | null
  result: SimulationResult | null
  pipeline: PipelineExecutionLog
}

const SESSION_LOG_STORAGE_KEY = 'opubliq.simulator.session-logs.v1'
const SESSION_LOG_LIMIT = 20

type PageId = 'simulateur' | 'session_logs' | 'methodology'

function MethodologyPage() {
  return (
    <section className="mt-8 flex flex-col gap-6">
      <div className="sim-card">
        <h2 className="text-xl font-semibold tracking-tight">
          Comment la simulation transforme une question en résultat
        </h2>
        <p className="mt-3 text-sm text-base-content/70">
          Le simulateur suit un pipeline en quatre couches: recherche sémantique, priors historiques,
          simulation LLM par strate, puis agrégation finale.
        </p>
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <div className="sim-card">
          <h3 className="text-sm font-medium">Les 4 étapes</h3>
          <ol className="mt-4 flex flex-col gap-3 text-sm text-base-content/70">
            <li className="rounded-lg border border-base-300/60 bg-base-200/35 px-3 py-2">
              <strong>1. Recherche sémantique</strong> — On retrouve les questions historiques proches.
            </li>
            <li className="rounded-lg border border-base-300/60 bg-base-200/35 px-3 py-2">
              <strong>2. Priors historiques</strong> — On charge les prédictions par strate.
            </li>
            <li className="rounded-lg border border-base-300/60 bg-base-200/35 px-3 py-2">
              <strong>3. Simulation LLM</strong> — Chaque strate reçoit un prompt calibré.
            </li>
            <li className="rounded-lg border border-base-300/60 bg-base-200/35 px-3 py-2">
              <strong>4. Agrégation</strong> — On combine pour produire la distribution nationale.
            </li>
          </ol>
        </div>
        <div className="sim-card">
          <h3 className="text-sm font-medium">À retenir</h3>
          <ul className="mt-4 flex flex-col gap-2 text-sm text-base-content/70">
            <li className="rounded-lg border border-base-300/60 px-3 py-2">
              Le simulateur cherche une estimation plausible, pas une vérité absolue.
            </li>
            <li className="rounded-lg border border-base-300/60 px-3 py-2">
              Une strate = un point de vue simulé, pas un répondant réel.
            </li>
            <li className="rounded-lg border border-base-300/60 px-3 py-2">
              Le contexte récent peut déplacer la sortie même si l'historique est similaire.
            </li>
          </ul>
        </div>
      </div>
    </section>
  )
}

class PipelineExecutionError extends Error {
  executionLog: PipelineExecutionLog

  constructor(message: string, executionLog: PipelineExecutionLog) {
    super(message)
    this.name = 'PipelineExecutionError'
    this.executionLog = executionLog
  }
}

function buildEmptyExecutionLog(): PipelineExecutionLog {
  return {
    semantic_search: null,
    fetch_strate_predictions: null,
    llm_prompt_dry_run: null,
    llm_sampling: null,
    aggregate_final_distribution: null,
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function extractErrorMessage(payload: unknown, fallback: string): string {
  if (isRecord(payload) && typeof payload.error === 'string' && payload.error.trim()) {
    return payload.error
  }
  return fallback
}

function serializeError(error: unknown): string {
  if (error instanceof Error) {
    return error.message
  }
  return String(error)
}

function asApiCallLog(
  step: string,
  requestPayload: unknown,
  status: number | null,
  durationMs: number,
  responsePayload: unknown,
  error: string | null,
): ApiCallLog {
  return {
    step,
    status,
    duration_ms: durationMs,
    request_payload: requestPayload,
    response_payload: responsePayload,
    error,
  }
}

async function invokeEdgeFunction(
  name: string,
  payload: unknown,
  headers: HeadersInit,
): Promise<{
  status: number
  ok: boolean
  durationMs: number
  responsePayload: unknown
}> {
  const startedAt = performance.now()
  const response = await fetch(fnUrl(name), {
    method: 'POST',
    headers,
    body: JSON.stringify(payload),
  })

  const durationMs = Math.round(performance.now() - startedAt)
  const rawBody = await response.text()

  if (!rawBody.trim()) {
    return { status: response.status, ok: response.ok, durationMs, responsePayload: null }
  }

  try {
    return {
      status: response.status,
      ok: response.ok,
      durationMs,
      responsePayload: JSON.parse(rawBody),
    }
  } catch {
    return {
      status: response.status,
      ok: response.ok,
      durationMs,
      responsePayload: { raw_text: rawBody },
    }
  }
}

async function invokeLlmSamplingWithProgress(
  payload: Record<string, unknown>,
  headers: HeadersInit,
  onProgress: (progress: Step3Progress) => void,
): Promise<{
  status: number
  ok: boolean
  durationMs: number
  responsePayload: unknown
}> {
  const startedAt = performance.now()
  const response = await fetch(fnUrl('llm-strate-sampling'), {
    method: 'POST',
    headers,
    body: JSON.stringify({ ...payload, stream_progress: true }),
  })

  const duration = () => Math.round(performance.now() - startedAt)

  if (!response.ok) {
    const rawBody = await response.text()
    try {
      const parsed = rawBody.trim() ? JSON.parse(rawBody) : null
      return { status: response.status, ok: false, durationMs: duration(), responsePayload: parsed }
    } catch {
      return {
        status: response.status,
        ok: false,
        durationMs: duration(),
        responsePayload: rawBody.trim() ? { raw_text: rawBody } : null,
      }
    }
  }

  const contentType = response.headers.get('content-type') ?? ''
  if (!contentType.includes('text/event-stream') || !response.body) {
    const rawBody = await response.text()
    try {
      const parsed = rawBody.trim() ? JSON.parse(rawBody) : null
      return { status: response.status, ok: true, durationMs: duration(), responsePayload: parsed }
    } catch {
      return {
        status: response.status,
        ok: false,
        durationMs: duration(),
        responsePayload: { error: 'Réponse JSON invalide pour étape 3' },
      }
    }
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let completePayload: unknown = null

  try {
    const processEvent = (eventBlock: string) => {
      const lines = eventBlock
        .split('\n')
        .map(line => line.trimEnd())
        .filter(Boolean)
      const eventTypeLine = lines.find(line => line.startsWith('event:'))
      const dataLines = lines.filter(line => line.startsWith('data:'))

      if (!eventTypeLine || dataLines.length === 0) {
        return null
      }

      const eventType = eventTypeLine.slice('event:'.length).trim()
      const dataText = dataLines.map(line => line.slice('data:'.length).trimStart()).join('\n')
      const payloadData = JSON.parse(dataText) as unknown

      if (
        eventType === 'progress' &&
        isRecord(payloadData) &&
        typeof payloadData.completed === 'number' &&
        typeof payloadData.total === 'number'
      ) {
        onProgress({ completed: payloadData.completed, total: payloadData.total })
      }

      if (eventType === 'complete' && isRecord(payloadData)) {
        completePayload = payloadData
      }

      if (eventType === 'error' && isRecord(payloadData) && typeof payloadData.error === 'string') {
        return { error: payloadData.error }
      }

      return null
    }

    while (true) {
      const { value, done } = await reader.read()
      buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done })

      let boundaryIndex = buffer.indexOf('\n\n')
      while (boundaryIndex >= 0) {
        const eventBlock = buffer.slice(0, boundaryIndex)
        buffer = buffer.slice(boundaryIndex + 2)
        const eventError = processEvent(eventBlock)
        if (eventError) {
          return {
            status: response.status,
            ok: false,
            durationMs: duration(),
            responsePayload: { error: eventError.error },
          }
        }
        boundaryIndex = buffer.indexOf('\n\n')
      }

      if (done) {
        if (buffer.trim()) {
          const eventError = processEvent(buffer)
          if (eventError) {
            return {
              status: response.status,
              ok: false,
              durationMs: duration(),
              responsePayload: { error: eventError.error },
            }
          }
        }
        break
      }
    }
  } catch {
    return {
      status: response.status,
      ok: false,
      durationMs: duration(),
      responsePayload: { error: 'Flux de progression invalide pour étape 3' },
    }
  }

  if (!isRecord(completePayload) || !Array.isArray(completePayload.strate_results)) {
    return {
      status: response.status,
      ok: false,
      durationMs: duration(),
      responsePayload: { error: 'Flux de progression incomplet pour étape 3' },
    }
  }

  return {
    status: response.status,
    ok: true,
    durationMs: duration(),
    responsePayload: completePayload,
  }
}

function loadSessionLogs(): SimulationLogEntry[] {
  if (typeof window === 'undefined') {
    return []
  }

  try {
    const raw = window.localStorage.getItem(SESSION_LOG_STORAGE_KEY)
    if (!raw) {
      return []
    }

    const parsed = JSON.parse(raw)
    if (!Array.isArray(parsed)) {
      return []
    }

    return parsed as SimulationLogEntry[]
  } catch {
    return []
  }
}

function formatJson(value: unknown): string {
  if (value === undefined) {
    return 'undefined'
  }
  return JSON.stringify(value, null, 2)
}

function first120(value: string): string {
  const text = value.trim()
  if (text.length <= 120) {
    return text
  }
  return `${text.slice(0, 117)}...`
}

async function runPipeline(
  question: string,
  context: string,
  choices: string[] | undefined,
  onStep: (step: PipelineStep) => void,
  onStep3Progress: (progress: Step3Progress) => void,
): Promise<{ result: SimulationResult; executionLog: PipelineExecutionLog }> {
  if (!SUPABASE_URL) {
    throw new Error(
      "Configuration manquante: definissez VITE_SUPABASE_URL (Netlify: Site settings -> Environment variables).",
    )
  }

  const executionLog = buildEmptyExecutionLog()
  const headers: HeadersInit = { 'Content-Type': 'application/json' }
  if (SUPABASE_PUBLISHABLE_KEY) {
    ;(headers as Record<string, string>).Authorization = `Bearer ${SUPABASE_PUBLISHABLE_KEY}`
  }

  try {
    onStep('step1_semantic_search')
    const step1Request = { question }
    const step1Call = await invokeEdgeFunction('semantic-search', step1Request, headers)
    executionLog.semantic_search = asApiCallLog(
      'semantic-search',
      step1Request,
      step1Call.status,
      step1Call.durationMs,
      step1Call.responsePayload,
      step1Call.ok ? null : extractErrorMessage(step1Call.responsePayload, 'Erreur inconnue'),
    )

    if (!step1Call.ok) {
      throw new PipelineExecutionError(
         `Étape 1 (recherche sémantique) : ${extractErrorMessage(step1Call.responsePayload, 'Erreur inconnue')}`,
        executionLog,
      )
    }

    const step1 = step1Call.responsePayload as { results?: Array<Record<string, unknown>> }
    if (!step1.results || step1.results.length === 0) {
      throw new PipelineExecutionError(
        'Aucune question historique pertinente trouvée pour simuler cette question. Essayez une question plus proche des thèmes couverts par les sondages disponibles.',
        executionLog,
      )
    }

    onStep('step2_fetch_predictions')
    const step2Request = { results: step1.results }
    const step2Call = await invokeEdgeFunction('fetch-strate-predictions', step2Request, headers)
    executionLog.fetch_strate_predictions = asApiCallLog(
      'fetch-strate-predictions',
      step2Request,
      step2Call.status,
      step2Call.durationMs,
      step2Call.responsePayload,
      step2Call.ok ? null : extractErrorMessage(step2Call.responsePayload, 'Erreur inconnue'),
    )

    if (!step2Call.ok) {
      throw new PipelineExecutionError(
         `Étape 2 (prédictions historiques) : ${extractErrorMessage(step2Call.responsePayload, 'Erreur inconnue')}`,
        executionLog,
      )
    }

    const step2 = step2Call.responsePayload as { predictions?: unknown[] }

    const dryRunRequest = {
      predictions: step2.predictions,
      question,
      context,
      choices,
      dry_run: true,
    }
    try {
      const dryRunCall = await invokeEdgeFunction('llm-strate-sampling', dryRunRequest, headers)
      executionLog.llm_prompt_dry_run = asApiCallLog(
        'llm-strate-sampling (dry_run)',
        dryRunRequest,
        dryRunCall.status,
        dryRunCall.durationMs,
        dryRunCall.responsePayload,
        dryRunCall.ok ? null : extractErrorMessage(dryRunCall.responsePayload, 'Erreur inconnue'),
      )
    } catch (error) {
      executionLog.llm_prompt_dry_run = asApiCallLog(
        'llm-strate-sampling (dry_run)',
        dryRunRequest,
        null,
        0,
        null,
        serializeError(error),
      )
    }

    onStep('step3_llm_sampling')
    const step3Request = {
      predictions: step2.predictions,
      question,
      context,
      choices,
    }
    const step3Call = await invokeLlmSamplingWithProgress(step3Request, headers, onStep3Progress)
    executionLog.llm_sampling = asApiCallLog(
      'llm-strate-sampling',
      step3Request,
      step3Call.status,
      step3Call.durationMs,
      step3Call.responsePayload,
      step3Call.ok ? null : extractErrorMessage(step3Call.responsePayload, 'Erreur inconnue'),
    )

    if (!step3Call.ok) {
      throw new PipelineExecutionError(
         `Étape 3 (simulation LLM) : ${extractErrorMessage(step3Call.responsePayload, 'Erreur inconnue')}`,
        executionLog,
      )
    }

    const step3 = step3Call.responsePayload as { strate_results?: LlmStrateResult[] }
    const strateResults = step3.strate_results ?? []
    const failedCount = strateResults.filter(r => r.error !== null).length
    if (strateResults.length === 0 || failedCount === strateResults.length) {
      const sampleError = strateResults[0]?.error ?? 'unknown'
       throw new PipelineExecutionError(`Toutes les strates ont échoué. Exemple : ${sampleError}`, executionLog)
    }

    onStep('step4_aggregate')
    const step4Request = { question, strate_results: strateResults }
    const step4Call = await invokeEdgeFunction('aggregate-final-distribution', step4Request, headers)
    executionLog.aggregate_final_distribution = asApiCallLog(
      'aggregate-final-distribution',
      step4Request,
      step4Call.status,
      step4Call.durationMs,
      step4Call.responsePayload,
      step4Call.ok ? null : extractErrorMessage(step4Call.responsePayload, 'Erreur inconnue'),
    )

    if (!step4Call.ok) {
      throw new PipelineExecutionError(
         `Étape 4 (agrégation) : ${extractErrorMessage(step4Call.responsePayload, 'Erreur inconnue')}`,
        executionLog,
      )
    }

    return {
      result: step4Call.responsePayload as SimulationResult,
      executionLog,
    }
  } catch (error) {
    if (error instanceof PipelineExecutionError) {
      throw error
    }

    throw new PipelineExecutionError(serializeError(error), executionLog)
  }
}

function App() {
  const [question, setQuestion] = useState('')
  const [contexte, setContexte] = useState('')
  const [choicesText, setChoicesText] = useState('')
  const [activePage, setActivePage] = useState<PageId>('simulateur')

  const [pipelineStep, setPipelineStep] = useState<PipelineStep>('idle')
  const [step3Progress, setStep3Progress] = useState<Step3Progress | null>(null)
  const [errorMessage, setErrorMessage] = useState('')
  const [result, setResult] = useState<SimulationResult | null>(null)
  const [sessionLogs, setSessionLogs] = useState<SimulationLogEntry[]>(() => loadSessionLogs())
  const [selectedLogId, setSelectedLogId] = useState<string | null>(null)

  useEffect(() => {
    if (typeof window === 'undefined') {
      return
    }
    window.localStorage.setItem(SESSION_LOG_STORAGE_KEY, JSON.stringify(sessionLogs))
  }, [sessionLogs])

  const effectiveSelectedLogId =
    selectedLogId && sessionLogs.some(log => log.id === selectedLogId)
      ? selectedLogId
      : (sessionLogs[0]?.id ?? null)

  const selectedLog = useMemo(
    () => sessionLogs.find(log => log.id === effectiveSelectedLogId) ?? null,
    [effectiveSelectedLogId, sessionLogs],
  )

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    const choices = choicesText
      .split('\n')
      .map(c => c.trim())
      .filter(c => c.length > 0)
      .map(c => normalizeText(c))

    setResult(null)
    setErrorMessage('')
    setStep3Progress(null)
    setPipelineStep('step1_semantic_search')

    try {
      const simulationRun = await runPipeline(
        normalizeText(question.trim()),
        normalizeText(contexte.trim()),
        choices.length > 0 ? choices : undefined,
        setPipelineStep,
        setStep3Progress,
      )
      setResult(simulationRun.result)
      setPipelineStep('success')

      const newLog: SimulationLogEntry = {
        id: crypto.randomUUID(),
        created_at: new Date().toISOString(),
        status: 'success',
        question: normalizeText(question.trim()),
        context: normalizeText(contexte.trim()),
        choices,
        error_message: null,
        result: simulationRun.result,
        pipeline: simulationRun.executionLog,
      }

      setSessionLogs(prevLogs => [newLog, ...prevLogs].slice(0, SESSION_LOG_LIMIT))
      setSelectedLogId(newLog.id)
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      setErrorMessage(message)
      setPipelineStep('error')

      const failedLog: SimulationLogEntry = {
        id: crypto.randomUUID(),
        created_at: new Date().toISOString(),
        status: 'error',
        question: normalizeText(question.trim()),
        context: normalizeText(contexte.trim()),
        choices,
        error_message: normalizeText(message),
        result: null,
        pipeline: error instanceof PipelineExecutionError ? error.executionLog : buildEmptyExecutionLog(),
      }

      setSessionLogs(prevLogs => [failedLog, ...prevLogs].slice(0, SESSION_LOG_LIMIT))
      setSelectedLogId(failedLog.id)
    }
  }

  const isLoading = !['idle', 'success', 'error'].includes(pipelineStep)
  const canSubmit = question.trim() && contexte.trim() && !isLoading
  const currentStepIndex = PIPELINE_FLOW.indexOf(pipelineStep)
  const normalizedChoices = choicesText
    .split('\n')
    .map(c => c.trim())
    .filter(c => c.length > 0)

  const scoredQuestions =
    selectedLog &&
    isRecord(selectedLog.pipeline.semantic_search?.response_payload) &&
    Array.isArray(selectedLog.pipeline.semantic_search.response_payload.results)
      ? (selectedLog.pipeline.semantic_search.response_payload.results as Array<Record<string, unknown>>)
      : []

  const stratePrompts =
    selectedLog &&
    isRecord(selectedLog.pipeline.llm_prompt_dry_run?.response_payload) &&
    Array.isArray(selectedLog.pipeline.llm_prompt_dry_run.response_payload.strate_prompts)
      ? (selectedLog.pipeline.llm_prompt_dry_run.response_payload.strate_prompts as Array<Record<string, unknown>>)
      : []

  const strateReasonings =
    selectedLog &&
    isRecord(selectedLog.pipeline.llm_sampling?.response_payload) &&
    Array.isArray(selectedLog.pipeline.llm_sampling.response_payload.strate_results)
      ? (selectedLog.pipeline.llm_sampling.response_payload.strate_results as Array<Record<string, unknown>>)
      : []

  return (
    <div className="sim-page min-h-screen">
      <div className="mx-auto flex max-w-6xl flex-col px-4 py-8 sm:px-8 sm:py-10 lg:px-10">
        <header className="sim-hero">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="sim-kicker">Projection Nationale</p>
            <div className="sim-switch" role="tablist" aria-label="Navigation principale">
              <button
                type="button"
                className={`btn btn-sm ${activePage === 'simulateur' ? 'btn-primary' : 'btn-ghost'}`}
                onClick={() => setActivePage('simulateur')}
                >
                Simulateur
              </button>
              <button
                type="button"
                className={`btn btn-sm ${activePage === 'session_logs' ? 'btn-primary' : 'btn-ghost'}`}
                onClick={() => setActivePage('session_logs')}
              >
                Logs ({sessionLogs.length})
              </button>
              <button
                type="button"
                className={`btn btn-sm ${activePage === 'methodology' ? 'btn-primary' : 'btn-ghost'}`}
                onClick={() => setActivePage('methodology')}
              >
                Méthodologie
              </button>
            </div>
          </div>

          <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl">
            {activePage === 'simulateur' ? 'Simulateur de sondage' : activePage === 'session_logs' ? 'Logs' : 'Méthodologie'}
          </h1>

          <p className="max-w-3xl text-sm text-base-content/70 sm:text-base">
           {activePage === 'simulateur'
             ? 'Structurez votre question, fournissez le contexte, puis laissez le pipeline estimer une distribution nationale.'
             : activePage === 'session_logs'
             ? 'Retrouvez l\'historique des simulations de la session et inspectez les payloads détaillés de chaque étape.'
             : 'Une vue claire du pipeline: filtrage sémantique, priors historiques, simulation LLM par strate puis agrégation finale.'}
          </p>
        </header>

        {activePage === 'simulateur' ? (
          <main className="mt-8 grid gap-6 lg:grid-cols-[minmax(0,1.55fr)_minmax(280px,1fr)] lg:items-start">
            <section className="flex flex-col gap-6">
              <form onSubmit={handleSubmit} className="flex flex-col gap-5">
                <div className="sim-card">
                  <label className="text-sm font-medium" htmlFor="question">Question</label>
                  <input
                    id="question"
                    type="text"
                    className="input input-bordered mt-2 w-full"
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    placeholder="Ex: Êtes-vous pour ou contre la réforme du mode de scrutin?"
                    required
                  />
                  <p className="mt-2 text-xs text-base-content/50">Formulez une question claire et unique pour des resultats plus stables.</p>
                </div>

                <div className="sim-card">
                  <div className="flex items-end justify-between gap-3">
                    <label className="text-sm font-medium" htmlFor="choices">
                      Choix de réponse <span className="text-xs text-base-content/40">(optionnel)</span>
                    </label>
                    <span className="text-xs text-base-content/50">{normalizedChoices.length} choix détectés</span>
                  </div>
                  <textarea
                    id="choices"
                    className="textarea textarea-bordered mt-2 w-full text-sm leading-relaxed"
                    rows={4}
                    value={choicesText}
                    onChange={(e) => setChoicesText(e.target.value)}
                    placeholder="Un choix par ligne, ex:\nTout à fait d'accord\nPlutôt d'accord\nPlutôt en désaccord\nTout à fait en désaccord\nNe sait pas"
                  />
                   <p className="mt-2 text-xs text-base-content/50">Laissez vide pour que l'IA infère les options de réponse.</p>
                </div>

                <div className="sim-card">
                  <label className="text-sm font-medium" htmlFor="contexte">Contexte</label>
                  <textarea
                    id="contexte"
                    className="textarea textarea-bordered mt-2 w-full text-sm leading-relaxed"
                    rows={12}
                    value={contexte}
                    onChange={(e) => setContexte(e.target.value)}
                    placeholder="Collez ici les articles, rapports ou tout autre texte de contexte..."
                    required
                  />
                   <p className="mt-2 text-xs text-base-content/50">Ajoutez les informations utiles : faits, chiffres, citations et angle d'analyse.</p>
                </div>

                <div className="sim-card">
                  <button type="submit" className="btn btn-primary w-full sm:w-fit" disabled={!canSubmit}>
                    {isLoading ? 'Simulation en cours...' : 'Lancer la simulation'}
                  </button>

                  {isLoading && (
                    <div className="mt-4 flex flex-col gap-2">
                      <progress className="progress progress-primary w-full" />
                      <p className="text-sm text-base-content/60">{getStepLabel(pipelineStep, step3Progress)}</p>
                    </div>
                  )}

                  {pipelineStep === 'error' && (
                    <div role="alert" className="alert alert-error mt-4 text-sm">
                      <span>{errorMessage}</span>
                    </div>
                  )}

                  {pipelineStep === 'success' && result && (
                    <div className="mt-4 flex flex-col gap-2">
                       <div role="alert" className={`alert text-sm ${result.meta.failed_strates === 0 ? 'alert-success' : 'alert-warning'}`}>
                         <span>
                           Simulation terminée — {result.meta.successful_strates}/{result.meta.total_strates} strates réussies.
                           {result.meta.failed_strates > 0 && ` (${result.meta.failed_strates} échouées)`}
                         </span>
                       </div>
                      {result.meta.failed_strates > 0 && (
                        <details className="text-xs text-base-content/50">
                          <summary className="cursor-pointer select-none">Détails des erreurs ({result.meta.failed_strates})</summary>
                          <ul className="mt-1 flex flex-col gap-1 pl-2">
                            {result.strate_results.filter(s => s.error).map((s, i) => (
                              <li key={i}>
                                <details>
                                  <summary className="cursor-pointer select-none font-medium">
                                    {s.strate_age_group} - {s.strate_langue} - {s.strate_region} - {s.strate_genre}
                                  </summary>
                                  <pre className="mt-1 pl-2 font-mono whitespace-pre-wrap break-all">{s.error}</pre>
                                </details>
                              </li>
                            ))}
                          </ul>
                        </details>
                      )}
                    </div>
                  )}
                </div>
              </form>

              {result && (
                <div className="sim-card">
                   <h2 className="text-sm font-medium">Distribution nationale estimée</h2>
                  <pre className="mt-3 max-h-96 overflow-auto rounded-lg bg-base-200/80 p-4 text-xs">
                    {JSON.stringify(result.national_distribution, null, 2)}
                  </pre>
                </div>
              )}
            </section>

            <aside className="lg:sticky lg:top-6">
              <div className="sim-card mb-5">
                 <h2 className="text-sm font-medium">Parcours de la simulation</h2>
                <ol className="mt-4 flex flex-col gap-3">
                  {PIPELINE_FLOW.map((step, index) => {
                    const isComplete = pipelineStep === 'success' || currentStepIndex > index
                    const isCurrent = isLoading && currentStepIndex === index

                    return (
                      <li key={step} className={`rounded-lg border px-3 py-2 text-sm transition ${isCurrent ? 'border-primary/60 bg-primary/10 text-primary' : isComplete ? 'border-success/30 bg-success/10 text-success' : 'border-base-300/70 text-base-content/65'}`}>
                        {getStepLabel(step, step3Progress)}
                      </li>
                    )
                  })}
                </ol>
              </div>

              <div className="sim-card">
                 <h2 className="text-sm font-medium">Aperçu des entrées</h2>
                <dl className="mt-4 grid grid-cols-2 gap-x-3 gap-y-2 text-sm">
                  <dt className="text-base-content/55">Question</dt>
                  <dd className="text-right">{question.trim().length > 0 ? 'Renseignée' : 'Vide'}</dd>
                   <dt className="text-base-content/55">Choix</dt>
                   <dd className="text-right">{normalizedChoices.length || 'Auto'}</dd>
                   <dt className="text-base-content/55">Contexte</dt>
                   <dd className="text-right">{contexte.trim().length} caractères</dd>
                </dl>
              </div>
            </aside>
          </main>
        ) : activePage === 'session_logs' ? (
          <main className="mt-8 grid gap-6 lg:grid-cols-[minmax(260px,0.95fr)_minmax(0,1.45fr)] lg:items-start">
            <section className="sim-card lg:sticky lg:top-6">
              <div className="flex items-center justify-between gap-2">
                 <h2 className="text-sm font-medium">Historique de la session</h2>
                <button
                  type="button"
                  className="btn btn-xs btn-ghost"
                  onClick={() => setSessionLogs([])}
                  disabled={sessionLogs.length === 0}
                >
                  Vider
                </button>
              </div>

              {sessionLogs.length === 0 ? (
                 <p className="mt-4 text-sm text-base-content/60">Aucune simulation enregistrée dans cette session.</p>
              ) : (
                <ul className="mt-4 flex max-h-[70vh] flex-col gap-2 overflow-auto pr-1">
                  {sessionLogs.map(log => {
                    const isSelected = selectedLog?.id === log.id
                    return (
                      <li key={log.id}>
                        <button
                          type="button"
                          className={`sim-log-list-item ${isSelected ? 'sim-log-list-item-active' : ''}`}
                          onClick={() => setSelectedLogId(log.id)}
                        >
                          <div className="flex items-center justify-between gap-3">
                            <span className="line-clamp-1 text-left text-sm font-medium">{first120(log.question)}</span>
                            <span className={`badge badge-xs ${log.status === 'success' ? 'badge-success' : 'badge-error'}`}>
                              {log.status}
                            </span>
                          </div>
                          <p className="mt-1 text-left text-xs text-base-content/55">
                            {new Date(log.created_at).toLocaleString('fr-CA')}
                          </p>
                        </button>
                      </li>
                    )
                  })}
                </ul>
              )}
            </section>

            <section className="sim-card sim-logs-detail">
              {!selectedLog ? (
                <p className="text-sm text-base-content/60">Sélectionnez une simulation pour afficher les détails.</p>
              ) : (
                <div className="flex flex-col gap-5">
                  <div>
                     <h2 className="text-sm font-medium">Simulation sélectionnée</h2>
                    <p className="mt-2 text-sm">{selectedLog.question}</p>
                    <p className="mt-1 text-xs text-base-content/60">
                      {new Date(selectedLog.created_at).toLocaleString('fr-CA')} - {selectedLog.status}
                    </p>
                    {selectedLog.error_message && (
                      <div role="alert" className="alert alert-error mt-3 text-sm">
                        <span>{selectedLog.error_message}</span>
                      </div>
                    )}
                  </div>

                  <details className="sim-json-block" open>
                    <summary>Entrées utilisateur</summary>
                    <pre>{formatJson({ question: selectedLog.question, context: selectedLog.context, choices: selectedLog.choices })}</pre>
                  </details>

                  <div className="sim-log-section">
                     <h3 className="text-sm font-medium">Questions filtrées et scorées</h3>
                    {scoredQuestions.length === 0 ? (
                      <p className="mt-2 text-xs text-base-content/60">Aucune question retournée.</p>
                    ) : (
                      <ul className="mt-2 flex flex-col gap-2">
                        {scoredQuestions.map((item, index) => (
                          <li key={`${String(item.id ?? index)}-${index}`} className="rounded-lg border border-base-300/60 px-3 py-2 text-xs">
                            <p className="font-medium">{String(item.text ?? `Question ${index + 1}`)}</p>
                            <p className="mt-1 text-base-content/70">
                              ID: {String(item.id ?? 'n/a')} - Points LLM: {String(item.llm_points ?? 0)} - Similarite cosine: {String(item.cosine_similarity ?? 'n/a')}
                            </p>
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>

                  <div className="sim-log-section">
                     <h3 className="text-sm font-medium">Prompts par strate</h3>
                     {stratePrompts.length === 0 ? (
                       <p className="mt-2 text-xs text-base-content/60">Prompt d'essai indisponible pour cette simulation.</p>
                    ) : (
                      <details className="sim-json-block mt-2">
                         <summary>Afficher {stratePrompts.length} invites de prompt</summary>
                        <ul className="mt-3 flex flex-col gap-2">
                          {stratePrompts.map((prompt, index) => {
                            const strateLabel = [
                              prompt.strate_age_group,
                              prompt.strate_langue,
                              prompt.strate_region,
                              prompt.strate_genre,
                            ]
                              .filter(Boolean)
                              .join(' - ')

                            return (
                              <li key={`${strateLabel || 'strate'}-${index}`}>
                                <details className="sim-json-block">
                                  <summary>{strateLabel || `Strate ${index + 1}`}</summary>
                                  <pre>{String(prompt.prompt ?? '')}</pre>
                                </details>
                              </li>
                            )
                          })}
                        </ul>
                      </details>
                    )}
                  </div>

                  <div className="sim-log-section">
                     <h3 className="text-sm font-medium">Raisonnements API par strate</h3>
                     {strateReasonings.length === 0 ? (
                       <p className="mt-2 text-xs text-base-content/60">Aucune réponse LLM disponible.</p>
                    ) : (
                      <details className="sim-json-block mt-2">
                        <summary>Afficher les raisonnements ({strateReasonings.length})</summary>
                        <ul className="mt-3 flex flex-col gap-2">
                          {strateReasonings.map((entry, index) => {
                            const llmResponse = isRecord(entry.llm_response) ? entry.llm_response : null
                            const reasoning = llmResponse && typeof llmResponse.raisonnement === 'string'
                              ? llmResponse.raisonnement
                              : null
                            const strateLabel = [
                              entry.strate_age_group,
                              entry.strate_langue,
                              entry.strate_region,
                              entry.strate_genre,
                            ]
                              .filter(Boolean)
                              .join(' - ')

                            return (
                              <li key={`${strateLabel || 'reasoning'}-${index}`} className="rounded-lg border border-base-300/60 px-3 py-2 text-xs">
                                <p className="font-medium">{strateLabel || `Strate ${index + 1}`}</p>
                                <p className="mt-1 whitespace-pre-wrap text-base-content/75">
                                  {reasoning ?? String(entry.error ?? 'Raisonnement non disponible')}
                                </p>
                              </li>
                            )
                          })}
                        </ul>
                      </details>
                    )}
                  </div>

                  <div className="sim-log-section">
                    <h3 className="text-sm font-medium">Payloads complets des étapes</h3>
                    {(
                      [
                        selectedLog.pipeline.semantic_search,
                        selectedLog.pipeline.fetch_strate_predictions,
                        selectedLog.pipeline.llm_prompt_dry_run,
                        selectedLog.pipeline.llm_sampling,
                        selectedLog.pipeline.aggregate_final_distribution,
                      ] as Array<ApiCallLog | null>
                    )
                      .filter((step): step is ApiCallLog => step !== null)
                      .map(step => (
                        <details key={step.step} className="sim-json-block mt-2">
                          <summary>
                            {step.step} - status {step.status ?? 'n/a'} - {step.duration_ms}ms
                          </summary>
                          <pre>{formatJson(step)}</pre>
                        </details>
                      ))}
                  </div>

                  <details className="sim-json-block">
                    <summary>JSON complet de la simulation</summary>
                    <pre>{formatJson(selectedLog)}</pre>
                  </details>
                </div>
              )}
            </section>
          </main>
        ) : (
          <MethodologyPage />
        )}
      </div>
    </div>
  )
}

export default App
