import { useEffect, useMemo, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
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

function buildEdgeHeaders(): HeadersInit {
  const headers: HeadersInit = { 'Content-Type': 'application/json' }
  if (SUPABASE_PUBLISHABLE_KEY) {
    ;(headers as Record<string, string>).Authorization = `Bearer ${SUPABASE_PUBLISHABLE_KEY}`
  }
  return headers
}

type PipelineStep =
  | 'idle'
  | 'step0_context_processing'
  | 'step1_semantic_search'
  | 'step2_fetch_predictions'
  | 'step3_llm_sampling'
  | 'step4_aggregate'
  | 'success'
  | 'error'

const STEP_LABELS: Record<PipelineStep, string> = {
  idle: '',
  step0_context_processing: 'Traitement du contexte...',
  step1_semantic_search: 'Recherche sémantique...',
  step2_fetch_predictions: 'Chargement des prédictions historiques...',
  step3_llm_sampling: 'Simulation des strates...',
  step4_aggregate: 'Agrégation des résultats...',
  success: 'Simulation terminée.',
  error: '',
}

const PIPELINE_FLOW: PipelineStep[] = [
  'step0_context_processing',
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

interface MultinomialLlmResponse {
  distribution: Record<string, number>
  margin_of_error: number
}

interface NumericLlmResponse {
  mean: number
  margin_of_error: number
}

type UiLlmResponse = MultinomialLlmResponse | NumericLlmResponse

type SesDimensionKey = 'strate_age_group' | 'strate_langue' | 'strate_region' | 'strate_genre'

interface NationalChartDatum {
  option: string
  value: number
}

interface SesChartDatum {
  segment: string
  margin_of_error: number
  strate_count: number
  [option: string]: number | string
}

interface TooltipPayloadEntry {
  color?: string
  name?: unknown
  payload?: unknown
  value?: unknown
}

interface TooltipCardProps {
  active?: boolean
  label?: string | number
  mode: 'national' | 'ses'
  payload?: TooltipPayloadEntry[]
}

const SES_DIMENSIONS: Array<{ key: SesDimensionKey; label: string }> = [
  { key: 'strate_age_group', label: 'Âge' },
  { key: 'strate_langue', label: 'Langue' },
  { key: 'strate_region', label: 'Région' },
  { key: 'strate_genre', label: 'Genre' },
]

const RESPONSE_PALETTE = ['#56d1d7', '#f0695a', '#f7b267', '#8bd3dd', '#b8c0ff', '#89d99d', '#f4a4c0']

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
  national_distribution: Record<string, number> | { mean: number }
  national_margin_of_error: number
  strate_results: {
    strate_age_group: string
    strate_langue: string
    strate_region: string
    strate_genre: string
    weight: number | null
    llm_response: UiLlmResponse | null
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
  context_pipeline: ApiCallLog | null
  semantic_search: ApiCallLog | null
  fetch_strate_predictions: ApiCallLog | null
  llm_prompt_dry_run: ApiCallLog | null
  llm_sampling: ApiCallLog | null
  aggregate_final_distribution: ApiCallLog | null
}

interface ContextFilePayload {
  name: string
  mime_type: string
  size_bytes: number
  content_base64: string
}

interface ContextPipelineRequest {
  raw_text: string
  urls: string[]
  files: ContextFilePayload[]
}

interface ContextPipelineResponse {
  extracted_text?: string
  summary_factual?: string
}

interface SemanticSearchResult {
  id: number
  text: string
  scale_type: string | null
  var_name: string | null
  prefix: string | null
  survey_id: number
  choices: Record<string, string> | null
  cosine_similarity: number
  llm_points: number
}

interface SemanticSearchResponse {
  question: string
  top_k: number
  total_points_assigned?: number
  results: SemanticSearchResult[]
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
const DEFAULT_PROMPT_CONTEXT = 'Aucun contexte supplementaire'
const MAX_CONTEXT_FILES = 8
const MAX_CONTEXT_FILE_SIZE_BYTES = 5 * 1024 * 1024
const MAX_CONTEXT_TOTAL_BYTES = 12 * 1024 * 1024
const CONTEXT_FILE_ACCEPT = '.pdf,.txt,.md,.markdown,.csv,.json,.xml,.html,.htm,.rtf,text/plain,text/markdown,application/pdf'
const CONTEXT_FILE_ALLOWED_EXTENSIONS = new Set([
  '.pdf',
  '.txt',
  '.md',
  '.markdown',
  '.csv',
  '.json',
  '.xml',
  '.html',
  '.htm',
  '.rtf',
])
const CONTEXT_FILE_ALLOWED_MIME_TYPES = new Set([
  'application/pdf',
  'text/plain',
  'text/markdown',
  'text/csv',
  'application/json',
  'application/xml',
  'text/xml',
  'text/html',
  'application/rtf',
  'text/rtf',
])

type PageId = 'simulateur' | 'session_logs' | 'methodology' | 'data_catalogue'

const PAGE_TITLE: Record<PageId, string> = {
  simulateur: 'Simulateur de sondage',
  session_logs: 'Logs',
  methodology: 'Méthodologie',
  data_catalogue: 'Catalogue de données',
}

const PAGE_DESCRIPTION: Record<PageId, string> = {
  simulateur:
    'Structurez votre question, fournissez le contexte, puis laissez le pipeline estimer une distribution nationale.',
  session_logs:
    "Retrouvez l'historique des simulations de la session et inspectez les payloads détaillés de chaque étape.",
  methodology:
    'Une vue claire du pipeline: filtrage sémantique, priors historiques, simulation LLM par strate puis agrégation finale.',
  data_catalogue:
    'Explorez les questions historiques du dataset via une recherche sémantique pour identifier les thèmes déjà couverts.',
}

const CATALOGUE_SUGGESTIONS = ['économie', 'immigration', 'santé', 'logement', 'éducation', 'inflation']

function parseContextUrls(urlsText: string): string[] {
  return urlsText
    .split('\n')
    .map(url => normalizeText(url.trim()))
    .filter(url => {
      if (!url) {
        return false
      }

      try {
        const parsedUrl = new URL(url)
        return parsedUrl.protocol === 'https:' || parsedUrl.protocol === 'http:'
      } catch {
        return false
      }
    })
}

function isAcceptedContextFile(file: File): boolean {
  const lowerName = file.name.toLowerCase()
  const lastDotIndex = lowerName.lastIndexOf('.')
  const extension = lastDotIndex >= 0 ? lowerName.slice(lastDotIndex) : ''

  return CONTEXT_FILE_ALLOWED_EXTENSIONS.has(extension) || CONTEXT_FILE_ALLOWED_MIME_TYPES.has(file.type)
}

function formatCapturedContext(rawContext: string, urls: string[], contextFiles: Array<{ name: string }>): string {
  const sections: string[] = []

  if (rawContext.trim()) {
    sections.push(`Texte libre:\n${rawContext.trim()}`)
  }

  if (urls.length > 0) {
    sections.push(`URLs:\n${urls.map(url => `- ${url}`).join('\n')}`)
  }

  if (contextFiles.length > 0) {
    sections.push(`Fichiers:\n${contextFiles.map(file => `- ${file.name}`).join('\n')}`)
  }

  return sections.join('\n\n')
}

function arrayBufferToBase64(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer)
  const chunkSize = 0x8000
  let binary = ''

  for (let i = 0; i < bytes.length; i += chunkSize) {
    const chunk = bytes.subarray(i, i + chunkSize)
    binary += String.fromCharCode(...chunk)
  }

  return btoa(binary)
}

async function serializeContextFiles(files: File[]): Promise<ContextFilePayload[]> {
  const serialized = await Promise.all(
    files.map(async (file): Promise<ContextFilePayload> => {
      const buffer = await file.arrayBuffer()
      return {
        name: file.name,
        mime_type: file.type || 'application/octet-stream',
        size_bytes: file.size,
        content_base64: arrayBufferToBase64(buffer),
      }
    }),
  )

  return serialized
}

function MethodologyPage() {
  return (
    <section className="mt-8 flex flex-col gap-6">
      <div className="sim-card">
        <h2 className="text-xl font-semibold tracking-tight">
          Comment la simulation transforme une question en résultat
        </h2>
        <p className="mt-3 text-sm text-base-content/70">
          Le simulateur suit un pipeline en cinq couches: traitement du contexte, recherche sémantique,
          priors historiques, simulation LLM par strate, puis agrégation finale.
        </p>
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <div className="sim-card">
          <h3 className="text-sm font-medium">Les 5 étapes</h3>
          <ol className="mt-4 flex flex-col gap-3 text-sm text-base-content/70">
            <li className="rounded-lg border border-base-300/60 bg-base-200/35 px-3 py-2">
              <strong>1. Traitement du contexte</strong> — Texte, URLs et PDFs sont extraits puis résumés.
            </li>
            <li className="rounded-lg border border-base-300/60 bg-base-200/35 px-3 py-2">
              <strong>2. Recherche sémantique</strong> — On retrouve les questions historiques proches.
            </li>
            <li className="rounded-lg border border-base-300/60 bg-base-200/35 px-3 py-2">
              <strong>3. Priors historiques</strong> — On charge les prédictions par strate.
            </li>
            <li className="rounded-lg border border-base-300/60 bg-base-200/35 px-3 py-2">
              <strong>4. Simulation LLM</strong> — Chaque strate reçoit un prompt calibré.
            </li>
            <li className="rounded-lg border border-base-300/60 bg-base-200/35 px-3 py-2">
              <strong>5. Agrégation</strong> — On combine pour produire la distribution nationale.
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
    context_pipeline: null,
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

function toPercent(value: number): number {
  return Number((value * 100).toFixed(2))
}

function formatPercent(value: number): string {
  return `${value.toFixed(1)}%`
}

function formatSimilarity(value: number): string {
  const clamped = Math.max(0, Math.min(1, value))
  return `${Math.round(clamped * 100)}%`
}

function formatSegmentLabel(value: string): string {
  return value
    .replace(/_/g, ' ')
    .replace(/\b\w/g, letter => letter.toUpperCase())
}

function parseSemanticSearchResponse(payload: unknown): SemanticSearchResponse {
  if (!isRecord(payload) || !Array.isArray(payload.results)) {
    return { question: '', top_k: 0, results: [] }
  }

  const results = payload.results
    .filter(isRecord)
    .map(
      (item): SemanticSearchResult => ({
        id: typeof item.id === 'number' ? item.id : 0,
        text: typeof item.text === 'string' ? item.text : '',
        scale_type: typeof item.scale_type === 'string' ? item.scale_type : null,
        var_name: typeof item.var_name === 'string' ? item.var_name : null,
        prefix: typeof item.prefix === 'string' ? item.prefix : null,
        survey_id: typeof item.survey_id === 'number' ? item.survey_id : 0,
        choices: isRecord(item.choices)
          ? Object.fromEntries(
              Object.entries(item.choices)
                .filter(([, value]) => typeof value === 'string')
                .map(([key, value]) => [key, String(value)]),
            )
          : null,
        cosine_similarity: typeof item.cosine_similarity === 'number' ? item.cosine_similarity : 0,
        llm_points: typeof item.llm_points === 'number' ? item.llm_points : 0,
      }),
    )
    .filter(item => item.text.trim().length > 0)

  return {
    question: typeof payload.question === 'string' ? payload.question : '',
    top_k: typeof payload.top_k === 'number' ? payload.top_k : results.length,
    total_points_assigned:
      typeof payload.total_points_assigned === 'number' ? payload.total_points_assigned : undefined,
    results,
  }
}

function isMultinomialLlmResponse(value: unknown): value is MultinomialLlmResponse {
  if (!isRecord(value) || !isRecord(value.distribution)) {
    return false
  }

  const margin = value.margin_of_error
  if (typeof margin !== 'number' || Number.isNaN(margin)) {
    return false
  }

  return Object.values(value.distribution).every(entry => typeof entry === 'number' && Number.isFinite(entry))
}

function getResponseOptions(result: SimulationResult): string[] {
  const nationalDistribution = result.national_distribution
  const nationalOptions =
    'mean' in nationalDistribution
      ? []
      : Object.keys(nationalDistribution).filter(option => option !== 'mean')
  if (nationalOptions.length > 0) {
    return nationalOptions
  }

  const options = new Set<string>()
  for (const strate of result.strate_results) {
    if (isMultinomialLlmResponse(strate.llm_response)) {
      for (const option of Object.keys(strate.llm_response.distribution)) {
        options.add(option)
      }
    }
  }
  return Array.from(options)
}

function buildNationalChartData(result: SimulationResult, options: string[]): NationalChartDatum[] {
  const nationalDistribution = result.national_distribution
  if ('mean' in nationalDistribution) {
    return []
  }

  return options.map(option => {
    const rawValue = nationalDistribution[option]
    const value = typeof rawValue === 'number' && Number.isFinite(rawValue) ? toPercent(rawValue) : 0
    return { option, value }
  })
}

function buildSesChartData(result: SimulationResult, dimension: SesDimensionKey, options: string[]): SesChartDatum[] {
  interface SegmentAccumulator {
    segment: string
    totalWeight: number
    strateCount: number
    marginSquareSum: number
    optionSums: Record<string, number>
  }

  const bySegment = new Map<string, SegmentAccumulator>()

  for (const strate of result.strate_results) {
    if (!isMultinomialLlmResponse(strate.llm_response)) {
      continue
    }

    const weight = typeof strate.weight === 'number' && Number.isFinite(strate.weight) ? strate.weight : null
    if (weight === null || weight <= 0) {
      continue
    }

    const segment = String(strate[dimension])
    const existing = bySegment.get(segment)
    const current = existing ?? {
      segment,
      totalWeight: 0,
      strateCount: 0,
      marginSquareSum: 0,
      optionSums: Object.fromEntries(options.map(option => [option, 0])),
    }

    for (const option of options) {
      const probability = strate.llm_response.distribution[option] ?? 0
      current.optionSums[option] += probability * weight
    }

    current.totalWeight += weight
    current.strateCount += 1
    current.marginSquareSum += (weight * strate.llm_response.margin_of_error) ** 2
    bySegment.set(segment, current)
  }

  return Array.from(bySegment.values())
    .map(segmentData => {
      const row: SesChartDatum = {
        segment: formatSegmentLabel(segmentData.segment),
        margin_of_error:
          segmentData.totalWeight > 0
            ? toPercent(Math.sqrt(segmentData.marginSquareSum) / segmentData.totalWeight)
            : 0,
        strate_count: segmentData.strateCount,
      }

      for (const option of options) {
        row[option] =
          segmentData.totalWeight > 0
            ? toPercent(segmentData.optionSums[option] / segmentData.totalWeight)
            : 0
      }

      return row
    })
    .sort((a, b) => String(a.segment).localeCompare(String(b.segment), 'fr'))
}

function TooltipCard({ active, label, mode, payload }: TooltipCardProps) {
  if (!active || !payload || payload.length === 0) {
    return null
  }

  const rowPayload = payload[0]?.payload
  const margin =
    isRecord(rowPayload) && typeof rowPayload.margin_of_error === 'number' ? rowPayload.margin_of_error : null
  const strateCount =
    isRecord(rowPayload) && typeof rowPayload.strate_count === 'number' ? rowPayload.strate_count : null

  return (
    <div className="sim-chart-tooltip">
      {label && <p className="sim-chart-tooltip-title">{String(label)}</p>}
      <ul className="sim-chart-tooltip-values">
        {payload.map((entry) => {
          if (typeof entry.value !== 'number' || (typeof entry.name !== 'string' && typeof entry.name !== 'number')) {
            return null
          }

          const name = String(entry.name)

          return (
            <li key={name}>
              <span style={{ color: entry.color ?? 'inherit' }}>{name}</span>
              <span>{formatPercent(entry.value)}</span>
            </li>
          )
        })}
      </ul>
      {mode === 'ses' && margin !== null && (
        <p className="sim-chart-tooltip-meta">
          Marge d'erreur: ±{formatPercent(margin)}{strateCount !== null ? ` · ${strateCount} strate(s)` : ''}
        </p>
      )}
    </div>
  )
}

async function runPipeline(
  question: string,
  contextRequest: ContextPipelineRequest,
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
    onStep('step0_context_processing')
    const contextCall = await invokeEdgeFunction('context-pipeline', contextRequest, headers)
    executionLog.context_pipeline = asApiCallLog(
      'context-pipeline',
      contextRequest,
      contextCall.status,
      contextCall.durationMs,
      contextCall.responsePayload,
      contextCall.ok ? null : extractErrorMessage(contextCall.responsePayload, 'Erreur inconnue'),
    )

    const contextResponse = isRecord(contextCall.responsePayload)
      ? (contextCall.responsePayload as ContextPipelineResponse)
      : null

    const fallbackContext = formatCapturedContext(
      contextRequest.raw_text,
      contextRequest.urls,
      contextRequest.files.map(file => ({ name: file.name })),
    )

    const contextSummary =
      typeof contextResponse?.summary_factual === 'string' && contextResponse.summary_factual.trim().length > 0
        ? contextResponse.summary_factual
        : typeof contextResponse?.extracted_text === 'string' && contextResponse.extracted_text.trim().length > 0
        ? contextResponse.extracted_text
        : fallbackContext.trim().length > 0
        ? fallbackContext
        : DEFAULT_PROMPT_CONTEXT

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
      context: contextSummary,
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
      context: contextSummary,
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
  const [contextUrlsText, setContextUrlsText] = useState('')
  const [contextFiles, setContextFiles] = useState<File[]>([])
  const [choicesText, setChoicesText] = useState('')
  const [activePage, setActivePage] = useState<PageId>('simulateur')

  const [pipelineStep, setPipelineStep] = useState<PipelineStep>('idle')
  const [step3Progress, setStep3Progress] = useState<Step3Progress | null>(null)
  const [errorMessage, setErrorMessage] = useState('')
  const [result, setResult] = useState<SimulationResult | null>(null)
  const [sessionLogs, setSessionLogs] = useState<SimulationLogEntry[]>(() => loadSessionLogs())
  const [selectedLogId, setSelectedLogId] = useState<string | null>(null)
  const [catalogueQuery, setCatalogueQuery] = useState('')
  const [catalogueTopK, setCatalogueTopK] = useState(10)
  const [catalogueResults, setCatalogueResults] = useState<SemanticSearchResult[]>([])
  const [catalogueLastQuery, setCatalogueLastQuery] = useState('')
  const [catalogueTotalPoints, setCatalogueTotalPoints] = useState<number | null>(null)
  const [catalogueError, setCatalogueError] = useState('')
  const [isCatalogueLoading, setIsCatalogueLoading] = useState(false)

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

  const normalizedContextUrls = useMemo(
    () => parseContextUrls(contextUrlsText),
    [contextUrlsText],
  )

  function handleContextFileSelection(event: React.ChangeEvent<HTMLInputElement>) {
    const selectedFiles = Array.from(event.target.files ?? []).filter(isAcceptedContextFile)

    if (selectedFiles.length === 0) {
      return
    }

    const oversizedFiles = selectedFiles.filter(file => file.size > MAX_CONTEXT_FILE_SIZE_BYTES)
    if (oversizedFiles.length > 0) {
      setErrorMessage(
        `Certains fichiers depassent ${(MAX_CONTEXT_FILE_SIZE_BYTES / (1024 * 1024)).toFixed(0)}MB et ont ete ignores.`,
      )
    }

    const sizeAcceptedFiles = selectedFiles.filter(file => file.size <= MAX_CONTEXT_FILE_SIZE_BYTES)

    const knownFiles = new Set(contextFiles.map(file => `${file.name}-${file.size}-${file.lastModified}`))
    const dedupedNewFiles = sizeAcceptedFiles.filter(file => {
      const key = `${file.name}-${file.size}-${file.lastModified}`
      return !knownFiles.has(key)
    })

    const nextFiles = [...contextFiles, ...dedupedNewFiles].slice(0, MAX_CONTEXT_FILES)
    const totalBytes = nextFiles.reduce((sum, file) => sum + file.size, 0)
    if (totalBytes > MAX_CONTEXT_TOTAL_BYTES) {
      setErrorMessage(
        `Le total des fichiers depasse ${(MAX_CONTEXT_TOTAL_BYTES / (1024 * 1024)).toFixed(0)}MB. Retirez des fichiers pour continuer.`,
      )
      event.target.value = ''
      return
    }

    setContextFiles(nextFiles)
    event.target.value = ''
  }

  function removeContextFile(fileIndex: number) {
    setContextFiles(prev => prev.filter((_, index) => index !== fileIndex))
  }

  async function runCatalogueSearch(rawQuery: string) {
    const normalizedQuery = normalizeText(rawQuery.trim())

    if (!normalizedQuery) {
      setCatalogueError('Saisissez une question ou un thème pour lancer la recherche.')
      setCatalogueResults([])
      setCatalogueLastQuery('')
      setCatalogueTotalPoints(null)
      return
    }

    if (!SUPABASE_URL) {
      setCatalogueError(
        'Configuration manquante: definissez VITE_SUPABASE_URL (Netlify: Site settings -> Environment variables).',
      )
      setCatalogueResults([])
      setCatalogueLastQuery(normalizedQuery)
      setCatalogueTotalPoints(null)
      return
    }

    setIsCatalogueLoading(true)
    setCatalogueError('')
    setCatalogueLastQuery(normalizedQuery)

    try {
      const response = await invokeEdgeFunction(
        'semantic-search',
        { question: normalizedQuery, top_k: catalogueTopK },
        buildEdgeHeaders(),
      )

      if (!response.ok) {
        throw new Error(extractErrorMessage(response.responsePayload, 'Erreur inconnue'))
      }

      const parsed = parseSemanticSearchResponse(response.responsePayload)
      setCatalogueResults(parsed.results)
      setCatalogueTotalPoints(parsed.total_points_assigned ?? null)
    } catch (error) {
      setCatalogueResults([])
      setCatalogueTotalPoints(null)
      setCatalogueError(error instanceof Error ? error.message : String(error))
    } finally {
      setIsCatalogueLoading(false)
    }
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    const normalizedQuestion = normalizeText(question.trim())
    const normalizedRawContext = normalizeText(contexte.trim())
    const capturedContext = formatCapturedContext(
      normalizedRawContext,
      normalizedContextUrls,
      contextFiles,
    )
    const choices = choicesText
      .split('\n')
      .map(c => c.trim())
      .filter(c => c.length > 0)
      .map(c => normalizeText(c))
    const totalContextFileBytes = contextFiles.reduce((sum, file) => sum + file.size, 0)

    if (totalContextFileBytes > MAX_CONTEXT_TOTAL_BYTES) {
      setErrorMessage(
        `Le total des fichiers depasse ${(MAX_CONTEXT_TOTAL_BYTES / (1024 * 1024)).toFixed(0)}MB.`,
      )
      setPipelineStep('error')
      return
    }

    setResult(null)
    setErrorMessage('')
    setStep3Progress(null)
    setPipelineStep('step0_context_processing')

    try {
      const contextFilesPayload = await serializeContextFiles(contextFiles)
      const contextRequest: ContextPipelineRequest = {
        raw_text: normalizedRawContext,
        urls: normalizedContextUrls,
        files: contextFilesPayload,
      }

      const simulationRun = await runPipeline(
        normalizedQuestion,
        contextRequest,
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
        question: normalizedQuestion,
        context: capturedContext,
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
        question: normalizedQuestion,
        context: capturedContext,
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
  const hasContextInput =
    contexte.trim().length > 0 || normalizedContextUrls.length > 0 || contextFiles.length > 0
  const canSubmit = question.trim() && hasContextInput && !isLoading
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

  const responseOptions = useMemo(
    () => (result && result.question_type === 'multinomial' ? getResponseOptions(result) : []),
    [result],
  )

  const responseColorMap = useMemo(
    () => Object.fromEntries(responseOptions.map((option, index) => [option, RESPONSE_PALETTE[index % RESPONSE_PALETTE.length]])),
    [responseOptions],
  )

  const nationalChartData = useMemo(
    () => (result && result.question_type === 'multinomial' ? buildNationalChartData(result, responseOptions) : []),
    [result, responseOptions],
  )

  const sesChartDataByDimension = useMemo(() => {
    if (!result || result.question_type !== 'multinomial') {
      return [] as Array<{ key: SesDimensionKey; label: string; data: SesChartDatum[] }>
    }

    return SES_DIMENSIONS.map(dimension => ({
      key: dimension.key,
      label: dimension.label,
      data: buildSesChartData(result, dimension.key, responseOptions),
    }))
  }, [result, responseOptions])

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
              <button
                type="button"
                className={`btn btn-sm ${activePage === 'data_catalogue' ? 'btn-primary' : 'btn-ghost'}`}
                onClick={() => setActivePage('data_catalogue')}
              >
                Catalogue
              </button>
            </div>
          </div>

          <h1 className="text-2xl font-semibold tracking-tight sm:text-3xl">
            {PAGE_TITLE[activePage]}
          </h1>

          <p className="max-w-3xl text-sm text-base-content/70 sm:text-base">
            {PAGE_DESCRIPTION[activePage]}
          </p>
        </header>

        {activePage === 'simulateur' ? (
          <>
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
                  <p className="mt-2 text-xs text-base-content/60">
                    Texte, URLs et fichiers sont extraits et normalises par la fonction Edge de contexte avant la simulation LLM.
                  </p>
                  <textarea
                    id="contexte"
                    className="textarea textarea-bordered mt-2 w-full text-sm leading-relaxed"
                    rows={4}
                    value={contexte}
                    onChange={(e) => setContexte(e.target.value)}
                    placeholder="Texte libre de contexte (optionnel si vous ajoutez une URL ou un fichier)..."
                  />
                  <label className="mt-4 block text-sm font-medium" htmlFor="context-urls">URLs (une par ligne)</label>
                  <textarea
                    id="context-urls"
                    className="textarea textarea-bordered mt-2 w-full text-sm leading-relaxed"
                    rows={8}
                    value={contextUrlsText}
                    onChange={(e) => setContextUrlsText(e.target.value)}
                    placeholder="https://exemple.com/article-1\nhttps://exemple.com/article-2"
                  />
                  <p className="mt-2 text-xs text-base-content/50">
                    Les URL valides (http/https) sont gardées pour la prochaine étape backend.
                  </p>

                  <label className="mt-4 block text-sm font-medium" htmlFor="context-files">Fichiers</label>
                  <input
                    id="context-files"
                    type="file"
                    accept={CONTEXT_FILE_ACCEPT}
                    multiple
                    className="file-input file-input-bordered mt-2 w-full text-sm"
                    onChange={handleContextFileSelection}
                  />
                  <p className="mt-2 text-xs text-base-content/50">
                    Jusqu'à {MAX_CONTEXT_FILES} fichiers peuvent être attachés (max {(MAX_CONTEXT_FILE_SIZE_BYTES / (1024 * 1024)).toFixed(0)}MB par fichier, {(MAX_CONTEXT_TOTAL_BYTES / (1024 * 1024)).toFixed(0)}MB total).
                  </p>

                  {contextFiles.length > 0 && (
                    <ul className="mt-3 flex flex-col gap-2">
                      {contextFiles.map((file, index) => (
                        <li key={`${file.name}-${file.size}-${file.lastModified}`} className="flex items-center justify-between gap-3 rounded-lg border border-base-300/70 px-3 py-2 text-xs">
                          <span className="truncate">{file.name}</span>
                          <button
                            type="button"
                            className="btn btn-xs btn-ghost"
                            onClick={() => removeContextFile(index)}
                          >
                            Retirer
                          </button>
                        </li>
                      ))}
                    </ul>
                  )}
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
                    <dt className="text-base-content/55">Texte libre</dt>
                    <dd className="text-right">{contexte.trim().length} caractères</dd>
                    <dt className="text-base-content/55">URLs</dt>
                    <dd className="text-right">{normalizedContextUrls.length}</dd>
                    <dt className="text-base-content/55">Fichiers</dt>
                    <dd className="text-right">{contextFiles.length}</dd>
                </dl>
              </div>
            </aside>
          </main>

            {result && result.question_type === 'multinomial' && (
              <section className="mt-6 grid gap-6">
              <div className="sim-card">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <h2 className="text-base font-semibold">Distribution nationale estimée</h2>
                  <p className="text-xs text-base-content/60">
                    Marge d'erreur nationale: ±{formatPercent(toPercent(result.national_margin_of_error))}
                  </p>
                </div>
                <div className="mt-4 h-80 w-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={nationalChartData} layout="vertical" margin={{ top: 8, right: 16, left: 6, bottom: 4 }}>
                      <CartesianGrid strokeDasharray="4 4" stroke="color-mix(in oklch, var(--color-base-content) 24%, transparent)" />
                      <XAxis type="number" domain={[0, 100]} tickFormatter={formatPercent} />
                      <YAxis type="category" dataKey="option" width={180} interval={0} />
                      <Tooltip
                        cursor={{ fill: 'color-mix(in oklch, var(--color-primary) 18%, transparent)' }}
                        content={(props) => <TooltipCard {...props} mode="national" />}
                      />
                      <Bar dataKey="value" name="National" radius={[0, 8, 8, 0]}>
                        {nationalChartData.map(entry => (
                          <Cell key={entry.option} fill={responseColorMap[entry.option] ?? RESPONSE_PALETTE[0]} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>

              <div className="grid gap-6 xl:grid-cols-2">
                {sesChartDataByDimension.map(dimension => (
                  <div key={dimension.key} className="sim-card">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <h3 className="text-sm font-semibold">Répartition par {dimension.label}</h3>
                      <p className="text-xs text-base-content/60">Barres empilées par option de réponse</p>
                    </div>
                    <div className="mt-4 h-[24rem] w-full">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={dimension.data} margin={{ top: 8, right: 8, left: 0, bottom: 24 }}>
                          <CartesianGrid strokeDasharray="4 4" stroke="color-mix(in oklch, var(--color-base-content) 24%, transparent)" />
                          <XAxis dataKey="segment" interval={0} angle={-18} textAnchor="end" height={56} />
                          <YAxis domain={[0, 100]} tickFormatter={formatPercent} />
                          <Tooltip
                            cursor={{ fill: 'color-mix(in oklch, var(--color-primary) 18%, transparent)' }}
                            content={(props) => <TooltipCard {...props} mode="ses" />}
                          />
                          <Legend />
                          {responseOptions.map(option => (
                            <Bar
                              key={`${dimension.key}-${option}`}
                              dataKey={option}
                              stackId="responses"
                              fill={responseColorMap[option] ?? RESPONSE_PALETTE[0]}
                            />
                          ))}
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                ))}
              </div>

              <details className="sim-json-block">
                <summary>Données brutes (distribution nationale)</summary>
                <pre>{formatJson(result.national_distribution)}</pre>
              </details>
              </section>
            )}

            {result && result.question_type === 'numeric' && (
              <section className="mt-6">
                <div className="sim-card">
                  <h2 className="text-base font-semibold">Résultat numérique national</h2>
                  <p className="mt-2 text-sm text-base-content/70">
                    Moyenne estimée: <strong>{('mean' in result.national_distribution ? result.national_distribution.mean : 0).toFixed(2)}</strong>
                  </p>
                  <p className="text-sm text-base-content/70">
                    Marge d'erreur nationale: ±{result.national_margin_of_error.toFixed(2)}
                  </p>
                </div>
              </section>
            )}
          </>
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
                        selectedLog.pipeline.context_pipeline ?? null,
                        selectedLog.pipeline.semantic_search ?? null,
                        selectedLog.pipeline.fetch_strate_predictions ?? null,
                        selectedLog.pipeline.llm_prompt_dry_run ?? null,
                        selectedLog.pipeline.llm_sampling ?? null,
                        selectedLog.pipeline.aggregate_final_distribution ?? null,
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
        ) : activePage === 'data_catalogue' ? (
          <main className="mt-8 grid gap-6 lg:grid-cols-[minmax(0,1.25fr)_minmax(260px,0.85fr)] lg:items-start">
            <section className="sim-card">
              <form
                className="flex flex-col gap-4"
                onSubmit={(event) => {
                  event.preventDefault()
                  void runCatalogueSearch(catalogueQuery)
                }}
              >
                <div className="flex flex-col gap-2">
                  <label className="text-sm font-medium" htmlFor="catalogue-query">Rechercher une question</label>
                  <input
                    id="catalogue-query"
                    type="text"
                    className="input input-bordered w-full"
                    value={catalogueQuery}
                    onChange={(event) => setCatalogueQuery(event.target.value)}
                    placeholder="Ex: Êtes-vous en faveur d'un impôt sur les grandes fortunes?"
                  />
                </div>

                <div className="flex flex-wrap items-end gap-3">
                  <label className="text-xs text-base-content/60" htmlFor="catalogue-topk">
                    Nombre de résultats
                  </label>
                  <select
                    id="catalogue-topk"
                    className="select select-bordered select-sm"
                    value={catalogueTopK}
                    onChange={(event) => setCatalogueTopK(Number(event.target.value))}
                  >
                    <option value={5}>5</option>
                    <option value={10}>10</option>
                    <option value={15}>15</option>
                  </select>
                  <button type="submit" className="btn btn-primary btn-sm" disabled={isCatalogueLoading}>
                    {isCatalogueLoading ? 'Recherche...' : 'Lancer la recherche'}
                  </button>
                </div>
              </form>

              {catalogueError && (
                <div role="alert" className="alert alert-error mt-4 text-sm">
                  <span>{catalogueError}</span>
                </div>
              )}

              {isCatalogueLoading && (
                <div className="mt-4 flex flex-col gap-2">
                  <progress className="progress progress-primary w-full" />
                  <p className="text-xs text-base-content/60">Recherche des questions les plus proches en cours...</p>
                </div>
              )}

              {!isCatalogueLoading && catalogueLastQuery && catalogueResults.length === 0 && !catalogueError && (
                <p className="mt-4 text-sm text-base-content/60">Aucun résultat pertinent trouvé pour cette requête.</p>
              )}

              {!isCatalogueLoading && catalogueLastQuery && catalogueResults.length > 0 && (
                <ul className="mt-5 flex flex-col gap-3">
                  {catalogueResults.map((item) => {
                    const choiceEntries = item.choices ? Object.entries(item.choices) : []
                    const previewChoices = choiceEntries.slice(0, 3)

                    return (
                      <li key={item.id} className="rounded-xl border border-base-300/70 bg-base-100/75 px-4 py-3">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <p className="text-xs text-base-content/60">Question #{item.id}</p>
                          <div className="flex items-center gap-2">
                            <span className="badge badge-outline badge-sm">{item.llm_points} pts</span>
                            <span className="badge badge-ghost badge-sm">Similarité {formatSimilarity(item.cosine_similarity)}</span>
                          </div>
                        </div>
                        <p className="mt-2 text-sm font-medium">{item.text}</p>
                        <p className="mt-2 text-xs text-base-content/65">
                          Survey {item.survey_id}
                          {item.var_name ? ` · ${item.var_name}` : ''}
                          {item.prefix ? ` · ${item.prefix}` : ''}
                          {item.scale_type ? ` · ${item.scale_type}` : ''}
                        </p>
                        {previewChoices.length > 0 && (
                          <p className="mt-2 text-xs text-base-content/60">
                            {previewChoices.map(([key, value]) => `${key}: ${value}`).join(' · ')}
                            {choiceEntries.length > previewChoices.length ? ' · …' : ''}
                          </p>
                        )}
                      </li>
                    )
                  })}
                </ul>
              )}

              {!isCatalogueLoading && !catalogueLastQuery && (
                <p className="mt-4 text-sm text-base-content/60">
                  Entrez une question ou un thème pour explorer les items historiques disponibles dans le dataset.
                </p>
              )}
            </section>

            <aside className="sim-card lg:sticky lg:top-6">
              <h2 className="text-sm font-medium">Repères rapides</h2>
              <dl className="mt-4 grid grid-cols-2 gap-x-3 gap-y-2 text-sm">
                <dt className="text-base-content/55">Dernière requête</dt>
                <dd className="text-right text-xs">{catalogueLastQuery || '—'}</dd>
                <dt className="text-base-content/55">Résultats</dt>
                <dd className="text-right">{catalogueResults.length}</dd>
                <dt className="text-base-content/55">Points couverts</dt>
                <dd className="text-right">{catalogueTotalPoints ?? 'n/a'} / 100</dd>
              </dl>

              <div className="mt-5 border-t border-base-300/60 pt-4">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-base-content/60">Thèmes suggérés</h3>
                <div className="mt-3 flex flex-wrap gap-2">
                  {CATALOGUE_SUGGESTIONS.map((theme) => (
                    <button
                      key={theme}
                      type="button"
                      className="btn btn-xs btn-ghost"
                      onClick={() => {
                        setCatalogueQuery(theme)
                        void runCatalogueSearch(theme)
                      }}
                    >
                      {theme}
                    </button>
                  ))}
                </div>
              </div>
            </aside>
          </main>
        ) : (
          <MethodologyPage />
        )}
      </div>
    </div>
  )
}

export default App
