import { useState } from 'react'
import './App.css'

const SUPABASE_URL = import.meta.env.VITE_SUPABASE_URL as string
const SUPABASE_ANON_KEY = import.meta.env.VITE_SUPABASE_ANON_KEY as string

function fnUrl(name: string) {
  return `${SUPABASE_URL}/functions/v1/${name}`
}

// ---------------------------------------------------------------------------
// Pipeline state
// ---------------------------------------------------------------------------

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
  step1_semantic_search: 'Recherche sémantique…',
  step2_fetch_predictions: 'Chargement des prédictions historiques…',
  step3_llm_sampling: 'Simulation des 48 strates…',
  step4_aggregate: 'Agrégation des résultats…',
  success: 'Simulation terminée.',
  error: '',
}

interface LlmStrateResult {
  strate_age_group: string
  strate_langue: string
  strate_region: string
  strate_genre: string
  llm_response: Record<string, unknown> | null
  had_prior: boolean
  error: string | null
}

// Output type from Step 4 — used by the results UI (issue 2ch)
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

// ---------------------------------------------------------------------------
// Pipeline runner
// ---------------------------------------------------------------------------

async function runPipeline(
  question: string,
  context: string,
  choices: string[] | undefined,
  onStep: (step: PipelineStep) => void,
): Promise<SimulationResult> {
  const supabaseAuthHeader = { Authorization: `Bearer ${SUPABASE_ANON_KEY}` }
  const json = { 'Content-Type': 'application/json' }

  // Step 1 — semantic search (OpenRouter: embeddings + LLM scoring, clé côté serveur)
  onStep('step1_semantic_search')
  const r1 = await fetch(fnUrl('semantic-search'), {
    method: 'POST',
    headers: { ...json, ...supabaseAuthHeader },
    body: JSON.stringify({ question }),
  })
  if (!r1.ok) {
    const err = await r1.json().catch(() => ({ error: r1.statusText }))
    throw new Error(`Étape 1 (recherche sémantique) : ${err.error ?? r1.statusText}`)
  }
  const step1 = await r1.json()

  // Step 2 — fetch strate predictions
  onStep('step2_fetch_predictions')
  const r2 = await fetch(fnUrl('fetch-strate-predictions'), {
    method: 'POST',
    headers: { ...json, ...supabaseAuthHeader },
    body: JSON.stringify({ results: step1.results }),
  })
  if (!r2.ok) {
    const err = await r2.json().catch(() => ({ error: r2.statusText }))
    throw new Error(`Étape 2 (prédictions historiques) : ${err.error ?? r2.statusText}`)
  }
  const step2 = await r2.json()

  // Step 3 — LLM sampling via edge function (clé OpenRouter côté serveur)
  onStep('step3_llm_sampling')
  const r3 = await fetch(fnUrl('llm-strate-sampling'), {
    method: 'POST',
    headers: { ...json, ...supabaseAuthHeader },
    body: JSON.stringify({
      predictions: step2.predictions,
      question,
      context,
      choices,
    }),
  })
  if (!r3.ok) {
    const err = await r3.json().catch(() => ({ error: r3.statusText }))
    throw new Error(`Étape 3 (simulation LLM) : ${err.error ?? r3.statusText}`)
  }
  const step3 = await r3.json()
  const strateResults: LlmStrateResult[] = step3.strate_results

  // Guard: fail early if all strates errored
  const failedCount = strateResults.filter(r => r.error !== null).length
  if (failedCount === strateResults.length) {
    const sampleError = strateResults[0]?.error ?? 'unknown'
    throw new Error(`Toutes les strates ont échoué. Exemple : ${sampleError}`)
  }

  // Step 4 — aggregate
  onStep('step4_aggregate')
  const r4 = await fetch(fnUrl('aggregate-final-distribution'), {
    method: 'POST',
    headers: { ...json, ...supabaseAuthHeader },
    body: JSON.stringify({ question, strate_results: strateResults }),
  })
  if (!r4.ok) {
    const err = await r4.json().catch(() => ({ error: r4.statusText }))
    throw new Error(`Étape 4 (agrégation) : ${err.error ?? r4.statusText}`)
  }

  return r4.json() as Promise<SimulationResult>
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

function App() {
  const [question, setQuestion] = useState('')
  const [contexte, setContexte] = useState('')
  const [choicesText, setChoicesText] = useState('')

  const [pipelineStep, setPipelineStep] = useState<PipelineStep>('idle')
  const [errorMessage, setErrorMessage] = useState('')
  const [result, setResult] = useState<SimulationResult | null>(null)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    const choices = choicesText
      .split('\n')
      .map(c => c.trim())
      .filter(c => c.length > 0)

    setResult(null)
    setErrorMessage('')
    setPipelineStep('step1_semantic_search')

    try {
      const simulationResult = await runPipeline(
        question.trim(),
        contexte.trim(),
        choices.length > 0 ? choices : undefined,
        setPipelineStep,
      )
      setResult(simulationResult)
      setPipelineStep('success')
    } catch (err) {
      setErrorMessage(err instanceof Error ? err.message : String(err))
      setPipelineStep('error')
    }
  }

  const isLoading = !['idle', 'success', 'error'].includes(pipelineStep)
  const canSubmit = question.trim() && contexte.trim() && !isLoading

  return (
    <div className="min-h-screen flex flex-col max-w-5xl mx-auto border-x border-base-300">
      <header className="px-16 py-8 border-b border-base-300 flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">Simulateur de sondage</h1>
      </header>

      <main className="flex-1 px-16 py-12">
        <form onSubmit={handleSubmit} className="flex flex-col gap-10 max-w-2xl">
          <div className="flex flex-col gap-2">
            <label className="text-sm font-medium" htmlFor="question">Question</label>
            <input
              id="question"
              type="text"
              className="input input-bordered w-full"
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              placeholder="Ex: Êtes-vous pour ou contre la réforme du mode de scrutin?"
              required
            />
          </div>

          <div className="flex flex-col gap-2">
            <label className="text-sm font-medium" htmlFor="choices">
              Choix de réponse <span className="text-xs text-base-content/40">(optionnel)</span>
            </label>
            <textarea
              id="choices"
              className="textarea textarea-bordered w-full text-sm leading-relaxed"
              rows={3}
              value={choicesText}
              onChange={(e) => setChoicesText(e.target.value)}
              placeholder="Un choix par ligne, ex:&#10;Tout à fait d'accord&#10;Plutôt d'accord&#10;Plutôt en désaccord&#10;Tout à fait en désaccord&#10;Ne sait pas"
            />
            <p className="text-xs text-base-content/40">
              Laissez vide pour que l'IA infère les choix appropriés
            </p>
          </div>

          <div className="flex flex-col gap-2">
            <label className="text-sm font-medium" htmlFor="contexte">Contexte</label>
            <textarea
              id="contexte"
              className="textarea textarea-bordered w-full text-sm leading-relaxed"
              rows={14}
              value={contexte}
              onChange={(e) => setContexte(e.target.value)}
              placeholder="Collez ici les articles, rapports ou tout autre texte de contexte..."
              required
            />
          </div>

          <div className="flex flex-col gap-4">
            <button type="submit" className="btn btn-primary" disabled={!canSubmit}>
              {isLoading ? 'Simulation en cours…' : 'Lancer la simulation →'}
            </button>

            {/* Pipeline status */}
            {isLoading && (
              <div className="flex flex-col gap-2">
                <progress className="progress progress-primary w-full" />
                <p className="text-sm text-base-content/60">{STEP_LABELS[pipelineStep]}</p>
              </div>
            )}

            {pipelineStep === 'error' && (
              <div role="alert" className="alert alert-error text-sm">
                <span>{errorMessage}</span>
              </div>
            )}

            {pipelineStep === 'success' && result && (
              <div className="flex flex-col gap-2">
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
                              {s.strate_age_group} · {s.strate_langue} · {s.strate_region} · {s.strate_genre}
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

        {/* Placeholder for results (issue 2ch) */}
        {result && (
          <div className="mt-12 max-w-2xl">
            <pre className="text-xs bg-base-200 p-4 rounded overflow-auto max-h-96">
              {JSON.stringify(result.national_distribution, null, 2)}
            </pre>
          </div>
        )}
      </main>
    </div>
  )
}

export default App
