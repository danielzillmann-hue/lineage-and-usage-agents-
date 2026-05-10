#!/usr/bin/env bash
# One-time bootstrap: enable APIs, create Artifact Registry, grant IAM.
# Re-running is safe (idempotent).

set -euo pipefail

PROJECT="${PROJECT:-transformation-agent-demo}"
REGION="${REGION:-australia-southeast1}"
ACCOUNT="${ACCOUNT:-daniel.zillmann@intelia.com.au}"
AR_REPO="${AR_REPO:-lineage-agents}"
RESULTS_BUCKET="${RESULTS_BUCKET:-${PROJECT}-lineage-results}"
DEMO_BUCKET="${DEMO_BUCKET:-${PROJECT}-lineage-demo}"
RAW_DATASET="${RAW_DATASET:-migration_raw}"
DERIVED_DATASET="${DERIVED_DATASET:-migration_demo}"

GCLOUD="gcloud --account=$ACCOUNT --project=$PROJECT"

echo ">>> Enabling APIs"
$GCLOUD services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  firestore.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  aiplatform.googleapis.com \
  bigquery.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com

echo ">>> Creating Artifact Registry repo (if missing)"
if ! $GCLOUD artifacts repositories describe "$AR_REPO" --location="$REGION" >/dev/null 2>&1; then
  $GCLOUD artifacts repositories create "$AR_REPO" \
    --repository-format=docker \
    --location="$REGION" \
    --description="Lineage and usage agents containers"
else
  echo "    repo $AR_REPO already exists"
fi

echo ">>> Granting Cloud Run service account access (Vertex AI, Storage, Firestore)"
PROJECT_NUMBER=$($GCLOUD projects describe "$PROJECT" --format='value(projectNumber)')
RUN_SA="$PROJECT_NUMBER-compute@developer.gserviceaccount.com"

for ROLE in \
  roles/aiplatform.user \
  roles/storage.objectAdmin \
  roles/datastore.user \
  roles/secretmanager.secretAccessor \
  roles/bigquery.dataEditor \
  roles/bigquery.jobUser; do
  $GCLOUD projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:$RUN_SA" \
    --role="$ROLE" \
    --condition=None \
    --quiet >/dev/null
  echo "    bound $ROLE to $RUN_SA"
done

echo ">>> Bootstrapping Firestore in Native mode (if missing)"
if ! $GCLOUD firestore databases describe --database='(default)' >/dev/null 2>&1; then
  $GCLOUD firestore databases create --location="$REGION" --type=firestore-native
else
  echo "    Firestore (default) already provisioned"
fi

echo ">>> Creating GCS buckets (if missing)"
for B in "$RESULTS_BUCKET" "$DEMO_BUCKET"; do
  if ! gcloud --account=$ACCOUNT storage buckets describe "gs://$B" >/dev/null 2>&1; then
    gcloud --account=$ACCOUNT storage buckets create "gs://$B" \
      --project="$PROJECT" \
      --location="$REGION" \
      --uniform-bucket-level-access
    echo "    created gs://$B"
  else
    echo "    gs://$B already exists"
  fi
done

echo ">>> Creating BigQuery datasets (if missing)"
for D in "$RAW_DATASET" "$DERIVED_DATASET"; do
  if ! $GCLOUD --quiet alpha bq datasets describe "$D" >/dev/null 2>&1; then
    bq --account="$ACCOUNT" --project_id="$PROJECT" --location="$REGION" \
      mk --dataset --description="Lineage agents — $D" "$PROJECT:$D" 2>/dev/null \
      || echo "    (bq mk fell through; may need manual creation: bq mk --dataset --location=$REGION $PROJECT:$D)"
    echo "    created $PROJECT:$D"
  else
    echo "    dataset $D already exists"
  fi
done

echo ">>> Done. Run 'gcloud builds submit --config infra/cloudbuild.yaml --project=$PROJECT' to deploy."
