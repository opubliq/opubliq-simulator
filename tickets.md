UI
- Possibilité de voir l'output clean en graph de chaque log (donc on peut revoir les graphs clean d'anciennes runs)
- Dans la page simulateur, on devrait pouvoir développer une petit fenetre qui montre les questions filtrées et leur pondération de façon clean (pas style log). Peut-etre dans Parcours de la simulation?


BACKEND
- On pourrait impliquer les LLM un peu plus dans la recherche sémantique. Par exemple, si je pose la question "il devrait y avoir un TGV Toronto-Québec", le TGV est lié thématiquement au transport, environnement, etc. Un LLM devrait venir avoir un role pour ajuster ce qui est envoyé à semantic-search, ou qqchose du genre.