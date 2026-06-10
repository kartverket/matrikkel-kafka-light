# ADR-0001: Autentisering via SPIFFE vs AzureAD

- Status: Vedtatt
- Dato: 2026-06-10

## Kontekst

Applikasjonen må sikre at kun autoriserte applikasjoner og brukere kan kalle dens endepunkter.

Organisasjonen tilbyr støtte for Azure AD Application Registrations gjennom Azurerator sin `AzureAdApplication`, og SPIFFE gjennom Istio sin `AuthorizationPolicy`.

Behov:

- Kun forhåndsgodkjente applikasjoner skal kunne kalle applikasjonen.
- Utviklere skal kunne kalle applikasjonen.
- En identitet må kunne videreføres til applikasjonskoden for autorisering.

Vi hadde en diskusjon med Team Tilgangsstyring om hvilken tilnærming som best dekker applikasjonens behov. Team Tilgangsstyring hadde ingen sterk preferanse.

## Beslutning

Vi fortsetter med kun Azure AD.

## Konsekvenser

Positive konsekvenser:

- Brukere og applikasjoner autentiseres gjennom samme mekanisme.
- Vi kan benytte eksisterende biblioteksstøtte i Ktor.

Negative konsekvenser:

- Alle applikasjoner må registrere seg i Azure AD.
- Avhengighet til Azure AD.

## Vurderte alternativer

### Kun Azure AD

Godkjent.

Løsningen oppfyller alle behovene.

### Kun SPIFFE

Forkastet fordi løsningen ikke er fullt integrert i plattformen og flytter deler av sikkerhetsansvaret ut av applikasjonen.

Det er teoretisk mulig å autentisere både applikasjoner og brukere ved å definere en Istio `RequestAuthentication`. Dette er imidlertid primært dokumentert av Istio selv, og er vanskelig å teste lokalt siden Istio kun er tilgjengelig i Kubernetes-miljøer.

Propagering av identitet gjøres automatisk vha Http-Header `X-Forwarded-Client-Cert` og vil gi en mer lesbar identitet sammenlignet med AzureAD clientId.

### Azure AD og SPIFFE

Forkastet fordi den ekstra sikkerheten som SPIFFE gir ikke kan konfigureres på en måte som er transparent for applikasjonen.
Om man fjerner behovet for at utviklere skal kalle applikasjonen, så kan både "Kun SPIFFE" og "Azure AD og SPIFFE" være gode alternativer.