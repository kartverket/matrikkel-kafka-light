
# Broker

## Legge til topics

### Topic Access Control List

For at en applikasjon skal kunne legges til i `TopicAccessControlList` må applikasjonen som skal legges til ha en `AzureAdApplication` for å kunne kommunesere med `MachineToMacine` token.
For å konfigurere applikasjonen med en `Azure AD` applikasjon trenger man kun å legge til:
```
  azure:
    application:
      enabled: true
```
i `.skip/<miljø>.yaml` gitt at man bruker `matrikkel-actions/heimdall-deploy` for å deploye applikasjonen.

Da kan man finne `Service Principal Object Id` for `AzureAdApplication` til applikasjonen, og bruke dette som "identiteten" til applikasjonen som skal produsere eller konsumere fra en topic.

Dette må konfigureres per miljø man skal legge til.

Hent ut `Service Principal Object Id` fra Config i [Configuration.kt](./src/main/kotlin/no/kartverket/matrikkel/broker/config/Configuration.kt) og legg det til i `TopicAccessControlList` for Topicen som konsument eller produsent.

### Hvordan finne `Service Principal Object Id` via kubectl:

Trenger kubectl for å logge inn på clusteret, følg [guiden til skip](https://skip.kartverket.no/docs/kom-i-gang/praktisk-intro/kubernetes/logge-inn-p%C3%A5-cluster#6-valgfritt-gi-contexten-et-enklere-navn).

1. `gcloud auth login` i terminal, logg inn med kartverket bruker
2. `kubectx` → Velge cluster (dev eller prod)
3. `kubens` → Velge namspace
4. `kubectl get AzureAdApplication` -> Liste opp `AzureAdApplications` som er konfigurert i namespacet
5. `kubectl describe AzureAdApplication <navn på applikasjonen (ofte repo navn)>` -> Utfyllende informasjon
6. Se etter verdien `Service Principal Object Id`
7. Legg til `Service Principal Object Id` inn som miljøvariabel til appen i [.skip/<miljø>.yaml](../../.skip), med navnekonvensjonen `AZURE_AD_SERVICE_PRINCIPAL_<NAVN_PA_APPLIKASJON>`

### Hvordan finne `Service Principal Object Id` via Azure portal:

1. Logg inn på [Azure Portalen](https://azure.microsoft.com/en-us/get-started/azure-portal) med kartverket bruker.
2. Søk etter appen med format `<CLUSTER>:<NAMESPACE>:<APP NAVN>` og velg `Service Principal` ikke `Application`
3. `Service Principal Object Id` skal være listet opp som `Object ID`
4. Legg til `Service Principal Object Id` inn som miljøvariabel til appen i [.skip/<miljø>.yaml](../../.skip), med navnekonvensjonen `AZURE_AD_SERVICE_PRINCIPAL_<NAVN_PA_APPLIKASJON>`

