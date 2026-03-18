import { useState, useEffect } from 'react'
import './App.css'

function App() {
  const [question, setQuestion] = useState('')
  const [contexte, setContexte] = useState('')
  const [choicesText, setChoicesText] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [apiKeyVisible, setApiKeyVisible] = useState(false)

  useEffect(() => {
    const stored = localStorage.getItem('gemini_api_key')
    if (stored) setApiKey(stored)
  }, [])

  function handleApiKeySave() {
    localStorage.setItem('gemini_api_key', apiKey)
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    const choices = choicesText
      .split('\n')
      .map(c => c.trim())
      .filter(c => c.length > 0)
    const payload = {
      question: question.trim(),
      context: contexte.trim(),
      choices: choices.length > 0 ? choices : undefined,
    }
    console.log('Payload:', payload)
  }

  const canSubmit = question.trim() && contexte.trim() && apiKey.trim()

  return (
    <div className="min-h-screen flex flex-col max-w-5xl mx-auto border-x border-base-300">
      <header className="px-16 py-8 border-b border-base-300 flex items-center justify-between">
        <h1 className="text-2xl font-semibold tracking-tight">Simulateur de sondage</h1>
        <div className="flex items-center gap-2">
          <span className="text-xs text-base-content/40">Clé API Gemini</span>
          <input
            id="api-key"
            type={apiKeyVisible ? 'text' : 'password'}
            className="input input-bordered input-xs w-48"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder="AIza..."
            onBlur={handleApiKeySave}
          />
          <button
            type="button"
            className="btn btn-ghost btn-xs text-base-content/40"
            onClick={() => setApiKeyVisible((v) => !v)}
            aria-label={apiKeyVisible ? 'Masquer' : 'Afficher'}
          >
            {apiKeyVisible ? '🙈' : '👁'}
          </button>
        </div>
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

          <div>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit}>
              Lancer la simulation →
            </button>
          </div>
        </form>
      </main>

    </div>
  )
}

export default App
