
gcloud auth login
gcloud config set project $DEVSHELL_PROJECT_ID
gcloud iam service-accounts create runner
gcloud projects add-iam-policy-binding $DEVSHELL_PROJECT_ID --member "serviceAccount:runner@$DEVSHELL_PROJECT_ID.iam.gserviceaccount.com" --role "roles/owner"
gcloud iam service-accounts keys create cicd/gcp-key.json --iam-account runner@$DEVSHELL_PROJECT_ID.iam.gserviceaccount.com