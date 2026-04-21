# Réassurance IFRS 17 — Liaison Rétrocession & Recouvrement de Sinistres

**Version :** 1.0
**Auteur :** SukHee Lee
**Date :** Avril 2026
**Stack :** dbt + Databricks + Delta Lake

---

## Résumé

Ce projet implémente un pipeline de données pour la **mesure IFRS 17 en réassurance**, axé sur les structures qui rendent le reporting réassurance particulièrement complexe : les relations entre entités, les transitions d'état de rentabilité, et le recouvrement de sinistres par rétrocession.

Le pipeline suit l'évolution trimestrielle de la CSM (Contractual Service Margin) et de la LC (Loss Component) au niveau des GoC (Groups of Contracts) de l'activité assumée, et modélise le recouvrement via les traités rétro — le tout dans un pipeline dbt reproductible sur Databricks.

### Sorties du Pipeline

| Sortie | Description | Modèle MART |
|--------|-------------|-------------|
| Rapport AoC par GoC | Mouvement CSM/LC/LRC par GoC × trimestre × étape AoC | mart_goc_aoc_quarterly |
| Rapport AoC par traité | Détail au niveau traité avec CSM release | mart_treaty_aoc_quarterly |
| Résumé annuel | Ouverture → mouvement → clôture par GoC | mart_goc_annual_summary |
| Détail AoC annuel | Ventilation par étape AoC pour explication des écarts | mart_annual_aoc_detail |
| Vue P&L | Résultat trimestriel brut / cédé / net | mart_pnl_quarterly |

### Utilisateurs des Sorties

| Équipe | Besoin | Modèle |
|--------|--------|--------|
| Reporting IFRS 17 | État et mouvement CSM/LC par GoC | mart_goc_aoc_quarterly |
| Actuariat | Analyse de rentabilité par traité | mart_treaty_aoc_quarterly |
| Finance | P&L trimestriel et annuel | mart_pnl_quarterly, mart_goc_annual_summary |
| Gestion des risques | Efficacité du recouvrement rétro | mart_goc_aoc_quarterly (GoC rétro) |
| Direction | Vue d'ensemble de la rentabilité du portefeuille | mart_goc_annual_summary |

---

## Contexte du Projet — Partie 3 sur 5

Ce projet fait partie d'une série de 5 projets construisant une plateforme de données assurantielle complète :

| # | Projet | Branche | Pattern Pipeline | Statut |
|---|--------|---------|-----------------|--------|
| 1 | Medium-1 | P&C (Auto) — Provisionnement | Agrégation historique | ✅ Terminé |
| 2 | Medium-2 | Vie — BEL & Sensibilité | Projection prospective | ✅ Terminé |
| **3** | **Medium-3** | **Réassurance — IFRS 17 Rétrocession** | **Transition d'état** | **✅ En cours** |
| 4 | Medium-4 | À définir | À définir | Planifié |
| 5 | Large | Plateforme Analytics IFRS 17 sur Azure | E2E avec CI/CD | Planifié |

### Ce Qui Distingue Ce Projet

| Dimension | Medium-1 | Medium-2 | Medium-3 |
|-----------|----------|----------|----------|
| Moteur | Agrégation historique | Projection cashflow | Gestion d'état + liaison entités |
| Driver principal | Patterns de données | Hypothèses | Relations d'entités + événements de sinistre |
| Sortie principale | Réserve / ratio sinistres | BEL & sensibilités | Mouvement CSM/LC + recouvrement |
| Technique dbt | Modèles + tests basiques | Pipeline en couches + validation | **Macros + logique AoC paramétrée** |

---

## Domaine Métier

### La Problématique Réassurance

Un réassureur (Société A) assume des risques des cédantes et en transfère une partie par rétrocession. Sous IFRS 17 :

- Les **traités assumés** sont regroupés en **Groupes de Contrats (GoC)**, chacun mesuré en rentabilité
- Un GoC rentable porte une **CSM** (profit non acquis) ; un GoC onéreux porte une **Loss Component**
- Des **événements de sinistre** peuvent faire basculer un GoC de rentable à onéreux (ou inversement)
- Les **traités rétro** recouvrent une partie des sinistres, mesurés comme un GoC distinct

### Ce Que le Pipeline Modélise

Quatre scénarios GoC démontrent toutes les directions de transition :

| GoC | Début | Fin | Événement |
|-----|-------|-----|-----------|
| A | Rentable | Rentable | Sinistres mineurs absorbés par la CSM — référence |
| B | Rentable | **Onéreux** | Accident industriel Q2 épuise la CSM → LC reconnue |
| C | Onéreux | Onéreux (aggravé) | Portefeuille sous-tarifé avec expérience défavorable continue |
| D | Onéreux | **Rentable** | Expérience favorable Q3 reverse la LC → CSM restaurée |

### Structure Produit

- **Branche :** Vie Collective (couverture décès employés)
- **Type de traité :** Quote-Part (réassurance proportionnelle)
- **Modèle de mesure :** General Measurement Model (GMM/BBA)
- **Période du traité :** 2026 (première année de mesure)
- **Couverture sous-jacente :** Long terme (durée d'emploi) — GMM appliqué sur cette base

---

## Architecture du Pipeline

```
RAW (tables Delta)  →  STG (vues)  →  INT (vues)  →  VAL (vues)  →  MART (tables)
   5 tables              4 modèles      6 modèles      8 modèles      5 modèles
```

### Relations Entre Entités

```
GoC Assumé (1) ── contient ──► Traité Assumé (N)
                                      │
                                      │ N:1 (périmètre QS)
                                      ▼
                                Traité Rétro (1)
                                      │
                                      │ appartient à
                                      ▼
                                GoC Rétro (1) ── CSM/LC mesuré indépendamment
```

---

## Décisions de Conception Clés

### Convention de Signe

```
CASHFLOW :  + = profit (prime reçue)           - = sinistre (paiement)
BS :        CSM = négatif (profit non acquis)  LC = négatif (perte attendue)
            LRC = positif (recouvrement rétro) CSM Release = positif (reconnaissance profit)
```

### Règles de Transition d'État

Les transitions de rentabilité n'interviennent qu'à l'étape **VARIANCE**. Les étapes précédentes ne sont jamais modifiées rétroactivement.

| De | Condition | Vers | Action CSM | Action LC |
|----|-----------|------|------------|-----------|
| Rentable | cumul > 0 | Rentable | Variance → CSM | — |
| Rentable | cumul ≤ 0 | **Onéreux** | CSM entièrement épuisée | Reste → LC |
| Onéreux | cumul < 0 | Onéreux | — | Variance → LC |
| Onéreux | cumul ≥ 0 | **Rentable** | Reste → CSM | LC entièrement reversée |

### CSM Release

- **Niveau :** Traité (pas GoC) — pratique de marché
- **Fréquence :** Semestrielle (Q2, Q4) — paramétrée via variable dbt
- **Base :** Ratio coverage unit (S1 : 55%, S2 : 45%)
- **Proxy :** CSM GoC allouée aux traités par poids de contribution cashflow

### Recouvrement Rétro Basé sur le Delta

Le recouvrement n'est déclenché que par le **delta LC de la période courante** (lc_amount < 0), pas par la LC cumulée. Cela empêche le double comptage.

---

## Cadre de Validation

8 modèles de validation sur 3 niveaux. Tous les modèles retournent **0 ligne = PASS**.

| Niveau | Modèle | Vérification |
|--------|--------|-------------|
| L1 | val_bs_invariants | CSM/LC = conversion BS du cf_cumulative au CLOSING |
| L1 | val_profitability_consistency | Rentable → LC=0, Onéreux → CSM=0 |
| L1 | val_rollforward_continuity | OPENING = CLOSING du trimestre précédent |
| L2 | val_retro_recovery_identity | LRC = \|LC Assumé\| × taux de cession |
| L2 | val_release_timing | CSM release uniquement Q2/Q4, uniquement traités rentables |
| L2 | val_treaty_goc_reconciliation | Somme traités = niveau GoC |
| L2 | val_retro_no_double_recovery | Recouvrement cumulé ≤ sinistre cumulé × taux |
| L3 | val_scenario_expectation | GoC A/B/C/D finissent dans les états prévus |

---

## Simplifications vs. Production

| Ce Projet | Réalité Production |
|---|---|
| 4 GoC Assumés + 1 GoC Rétro | Centaines de GoC sur plusieurs branches |
| Quote-Part uniquement | QS + Excédent de Sinistres + Facultatif |
| 5 étapes AoC | 15+ étapes incluant variance financière, ajustement d'expérience |
| Cashflows reçus pré-actualisés | Gestion du taux d'actualisation, locked-in vs current |
| Pas de Risk Adjustment | Calcul RA avec niveaux de confiance |
| Un traité rétro par taux | Programmes rétro multi-couches |
| CSM release semestrielle, CU uniforme | Patterns CU par traité, fréquence de reporting flexible |
| Pas de comptabilisation GL | Génération d'écritures, intégration sous-ledger |
| Données synthétiques de test (38 lignes cashflow) | Données production des systèmes actuariels |
| Variables dbt pour paramètres | Tables de configuration avec contrôle de version |
| Pas de CI/CD | GitHub Actions → dbt test sur PR → Job Databricks sur merge |

---

## Stack Technique

- **dbt-core** ≥ 1.7
- **dbt-databricks** (production)
- **dbt-utils** — unique_combination_of_columns, tests génériques
- **dbt-expectations** — validation du nombre de lignes
- **Databricks SQL Warehouse** (Serverless)
- **Delta Lake** — ACID, enforcement de schéma, versioning
- **Python 3.10+** — Génération des données RAW (Notebook Databricks)

## Structure du Dépôt

```
reinsurance-ifrs17-retro-linkage-dbt/
├── models/
│   ├── sources/          # sources.yml
│   ├── staging/          # 4 modèles STG + schema.yml
│   ├── intermediate/     # 6 modèles INT + schema.yml
│   ├── validation/       # 8 modèles VAL + schema.yml
│   └── mart/             # 5 modèles MART + schema.yml
├── macros/               # Ordering AoC, détermination CSM/LC
├── notebooks/            # Génération données RAW (Databricks)
├── dbt_project.yml
├── packages.yml
├── README.md             # Anglais
├── README_FR.md          # Français
└── Medium_3_Final_Design_EN_v1.md  # Document de conception complet
```

## Exécution

```bash
# 1. Générer les données RAW (Notebook Databricks)
#    Exécuter notebooks/generate_raw_data.py dans Databricks

# 2. Installer les dépendances
dbt deps

# 3. Exécuter le pipeline
dbt run

# 4. Exécuter les tests
dbt test

# 5. Générer la documentation
dbt docs generate
dbt docs serve
```

---

## Résumé des Modèles

| Couche | Nombre | Matérialisation |
|--------|--------|----------------|
| RAW | 5 tables | Delta (Notebook Databricks) |
| STG | 4 modèles | vue |
| INT | 6 modèles | vue |
| VALIDATION | 8 modèles | vue |
| MART | 5 modèles | table |
| **Total** | **23 modèles dbt** | |

---

## Auteur

**SukHee Lee** — Actuarial Data Analyst | IFRS 17 · dbt · Databricks
Conception de pipelines de données assurantiels : provisionnement, IFRS 17, analytics engineering.

GitHub : github.com/SHLee5864
Medium : medium.com/@lsh5864