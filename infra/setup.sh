#!/usr/bin/env bash
# One-time bootstrap: enable APIs, create Artifact Registry, grant IAM.
# Re-running is safe (idempotent).

set -euo pipefail

PROJECT="${PROJECT:-dan-sandpit}"
REGION="${REGION:-australia-southeast1}"
ACCOUNT="${ACCOUNT:-daniel.zillmann@intelia.com.au}"
AR_REPO="${AR_REPO:-lineage-agents}"

GCLOUD="gcloud --account=$ACCOUNT --project=$PROJECT"

echo ">>> Enabling APIs"
$GCLOUD services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  firestore.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  aiplatform.googleapis.com

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
  roles/secretmanager.secretAccessor; do
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

echo ">>> Done. Run 'gcloud builds submit --config infra/cloudbuild.yaml' to deploy."
