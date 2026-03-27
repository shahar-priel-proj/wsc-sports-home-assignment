# AKS demo cluster (interview bonus)

Minimal **Azure Kubernetes Service (AKS)** setup to run the producer (`Job` / `CronJob`) and consumer (`Deployment`) . Costs are mostly **one small Linux VM** in the node pool plus **managed disks**; delete the resource group when you are done.

## Prerequisites

- [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) (`az`)
- An Azure subscription (student credits, trial, or pay-as-you-go)
- `kubectl` (optional until you connect: `az aks install-cli`)
- Shell snippets assume **bash** or **zsh**. On **fish**, use `set -gx VAR value` instead of `export VAR=value` (and avoid `#` on the same line as `export` in fish).

```bash
az login
az account set --subscription "<YOUR_SUBSCRIPTION_ID_OR_NAME>"
```

## 1. Variables (edit once)

Default below is **West Europe** (`westeurope`) and a dedicated resource group name—adjust **`$LOC`** / **`$RG`** if your Event Hubs / Storage live in another region (e.g. `eastus` + `rg-wsc-careers-demo`) to limit egress.

```bash
export RG="rg-wsc-careers-demo-we"
export LOC="westeurope"
export CLUSTER="aks-wsc-demo"
```

## 2. Resource group

```bash
az group create --name "$RG" --location "$LOC"
```

## 3. Create a small cluster

**Goals:** one node, smallest VM your subscription allows in that region, free tier when available.

```bash
az aks create \
  --resource-group "$RG" \
  --name "$CLUSTER" \
  --location "$LOC" \
  --node-count 1 \
  --node-vm-size Standard_B2s_v2 \
  --tier free \
  --generate-ssh-keys \
  --enable-managed-identity \
  --network-plugin azure
```

Notes:

- **`Standard_B2s_v2`** (B-series **v2**) is the usual small burstable choice; many subscriptions no longer offer the legacy name **`Standard_B2s`** (without `_v2`). For a bit more RAM, **`Standard_B2ms_v2`** or **`Standard_D2s_v5`** / **`Standard_D2s_v6`** also appear on typical allowlists. If pods **OOM**, bump VM size (homework clusters are disposable).
- If **`--tier free` is rejected**, drop **`--tier free`** and retry ([AKS pricing](https://azure.microsoft.com/pricing/details/kubernetes-service/)).
- **`docker_bridge_cidr ... will be ignored`** from the CLI is a **harmless** SDK warning; you can ignore it.
- **One node** is enough for a demo; it is **not** highly available.

### If you see: “VM size Standard_B2s is not allowed …” but the list includes `standard_b2s_v2`

Use the **exact** allowed name with Azure’s casing, e.g. **`Standard_B2s_v2`** (B-series **v2**). The older **`Standard_B2s`** SKU is often retired or blocked on newer regions/subscriptions even when **`B2s_v2`** is available.

### If you see: “VM size … is not allowed …” and the list looks nothing like B/Dv2

That means this **subscription + region** only allows certain SKUs (common on **corporate / restricted** tenants). The error lists what *is* allowed.

**Best fixes (try in order):**

1. **Try another region** if this one blocks the VM SKUs you need (e.g. **`eastus`** with a **new** `$RG` name if `rg-wsc-careers-demo-we` already exists in `westeurope`).
   - You **cannot** run `az group create` with the **same** `$RG` name in a new region if that resource group already exists elsewhere (Azure fixes the RG’s region at creation). Either:
     - **New name** for the other region, e.g. `export RG="rg-wsc-careers-demo-us"` and `export LOC="eastus"`, or
     - **Same RG, cluster elsewhere:** keep your existing `$RG` and set only `az aks create ... --location <other-region>` (cluster nodes in that region; RG metadata stays where the RG was created).
   Then run `az aks create` with **`Standard_B2s_v2`** (or another small SKU from the error list) again.

2. **Use a personal / MSDN / pay-as-you-go subscription** for the homework cluster if work only allows expensive SKUs.

3. **If you must use only the SKUs in the error message**, pick the **smallest** name you see (often something like **`Standard_DC2s_v3`** or **`Standard_EC2as_v5`**). Those are **not** B-series prices—treat the cluster as **short-lived** and **delete the resource group** when done. Example:
   ```bash
   --node-vm-size Standard_DC2s_v3
   ```
   (Exact name must match the list Azure returns, casing as Azure expects.)

See also: [AKS quotas, SKUs, and regions](https://aka.ms/aks/quotas-skus-regions).

## 4. Connect `kubectl`

```bash
az aks get-credentials --resource-group "$RG" --name "$CLUSTER" --overwrite-existing
kubectl get nodes
```

You should see **one** node in `Ready` state.

## 5. (Optional) Azure Container Registry + push images

For a real deploy you build the Docker images, push to ACR, and reference them in manifests. Sketch:

Pick an `ACR_NAME` that is **globally unique**, lowercase, letters and numbers only (Azure will reject a taken name).

On **new or restricted subscriptions**, register the resource provider **once** (needs a few minutes the first time):

```bash
az provider register --namespace Microsoft.ContainerRegistry --wait
```

If AKS creation ever fails with a similar “not registered” message, run `az provider register --namespace Microsoft.ContainerService --wait` as well.

```bash
export ACR_NAME="wscdemoacr"
az acr create --resource-group "$RG" --name "$ACR_NAME" --sku Basic
ACR_ID="$(az acr show -g "$RG" -n "$ACR_NAME" --query id -o tsv)"
az aks update -g "$RG" -n "$CLUSTER" --attach-acr "$ACR_ID"

# From repo root, after docker login via az:
az acr login --name "$ACR_NAME"
# AKS Linux nodes are amd64. On Apple Silicon (M1/M2/M3) you must build for linux/amd64 or pulls fail with
# "no match for platform in manifest".
docker build --platform linux/amd64 -f Dockerfile.producer -t "$ACR_NAME.azurecr.io/careers-producer:latest" .
docker build --platform linux/amd64 -f Dockerfile.consumer -t "$ACR_NAME.azurecr.io/careers-consumer:latest" .
docker push "$ACR_NAME.azurecr.io/careers-producer:latest"
docker push "$ACR_NAME.azurecr.io/careers-consumer:latest"
```

(Add Kubernetes `Secret` / `ConfigMap` YAML for Event Hubs + Blob separately—see `consumer/README.md` for env vars.)

### If `az acr create` fails with `MissingSubscriptionRegistration` (Microsoft.ContainerRegistry)

1. Run `az provider register --namespace Microsoft.ContainerRegistry --wait`.
2. Confirm: `az provider show -n Microsoft.ContainerRegistry --query registrationState -o tsv` → should print **`Registered`**.
3. Retry **`az acr create`** only. Do **not** run `az aks update --attach-acr` until the registry exists (otherwise you get `ResourceNotFound` for the ACR).
4. If `az aks update` asks to “reconcile” with **no** `--attach-acr` value, answer **`N`**, fix the ACR step, then run `az aks update ... --attach-acr "$ACR_ID"` again with a real id from `az acr show ... --query id -o tsv`.

You need permission to register providers (e.g. **Contributor** on the subscription, or ask an admin). See [aka.ms/rps-not-found](https://aka.ms/rps-not-found).

## 6. Deploy producer / consumer (bonus manifests)

**Security:** Never commit live connection strings or account keys. If any real keys appeared in chat, email, or a paste buffer, **rotate** them in Azure (Event Hubs SAS policy / Storage account key) before going to production.

### 6.1 Build and push images

Set `ACR_NAME` and push (same as §5), then edit **`k8s/kustomization.yaml`**: replace **`changeme`** (both `images[].newName` lines) with your ACR name—the hostname must be **`{name}.azurecr.io`** with **no underscores** (underscores in the registry part cause **`InvalidImageName`**).

### 6.2 Create the Secret (one time per cluster)

From the **repository root**:

```bash
cp k8s/secret-env.example k8s/secret.env
# Edit k8s/secret.env: one line per variable, format KEY=value (no quotes unless your shell needs them)
kubectl apply -f k8s/namespace.yaml
kubectl create secret generic wsc-careers-secrets \
  --namespace=wsc-careers \
  --from-env-file=k8s/secret.env
```

`k8s/secret.env` is gitignored—never commit it.

### 6.3 Apply ConfigMap, workloads, and image overrides

Still from the **repository root**:

```bash
kubectl apply -k k8s/
```

This applies `ConfigMap`, the one-shot **`Job`** (`careers-producer-once`), the **`CronJob`** (every 6 hours; set `suspend: true` in `producer-cronjob.yaml` if you only want manual runs), and the consumer **`Deployment`**.

### 6.4 Run producer again

Kubernetes **Job** names are unique. To scrape and publish again:

```bash
kubectl delete job -n wsc-careers careers-producer-once
kubectl apply -k k8s/
```

Or create a second Job with a different name.

### 6.5 Verify

```bash
kubectl get pods,jobs,cronjobs -n wsc-careers
kubectl logs -n wsc-careers deploy/careers-consumer -f
kubectl logs -n wsc-careers job/careers-producer-once
```

Tune **`k8s/configmap.yaml`** for topic, container name, `ENRICH_WITH_DUCKDB`, `SKIP_KAFKA`, `CAREERS_URL`, etc.—same variables you used with `docker run -e`.

### Pods show `InvalidImageName`

Common cause: the image reference is not a valid OCI/Docker name. **`YOUR_ACR_NAME.azurecr.io/...` is invalid** because **underscores are not allowed** in the registry hostname.

1. Set `k8s/kustomization.yaml` to **`yourrealacrname.azurecr.io/careers-producer`** (and consumer)—letters and numbers only in the ACR name, lowercase.
2. Check the rendered manifests: `kubectl kustomize k8s/ | grep image:` — each line should look like `wscdemoacr.azurecr.io/careers-consumer:latest`.
3. Re-apply: `kubectl apply -k k8s/`
4. Replace the stuck **Job** (immutable template): `kubectl delete job -n wsc-careers careers-producer-once` then `kubectl apply -k k8s/`
5. Restart the consumer: `kubectl rollout restart deployment/careers-consumer -n wsc-careers`

After fixing, **`ImagePullBackOff`** can still mean wrong ACR name, missing push, or AKS not attached to ACR—not `InvalidImageName`.

### Pods show `ImagePullBackOff` (pull fails)

The image reference is valid, but the node cannot download it. Work through these in order.

**1. See the exact error**

```bash
kubectl describe pod -n wsc-careers -l app=careers-consumer | sed -n '/Events:/,$p'
kubectl describe pod -n wsc-careers -l job-name=careers-producer-once | sed -n '/Events:/,$p'
```

Look for **401 / unauthorized** (AKS not allowed to pull from ACR), **404 / not found** (wrong name or tag), or **timeout**.

**2. Confirm images exist in ACR** (same subscription/tenant you use for `az`; replace `ACR_NAME`):

```bash
az acr repository list -n "$ACR_NAME" -o table
az acr repository show-tags -n "$ACR_NAME" --repository careers-producer -o table
az acr repository show-tags -n "$ACR_NAME" --repository careers-consumer -o table
```

If a repository or `latest` tag is missing, push from the repo root (§5):

```bash
az acr login --name "$ACR_NAME"
docker build --platform linux/amd64 -f Dockerfile.producer -t "$ACR_NAME.azurecr.io/careers-producer:latest" .
docker build --platform linux/amd64 -f Dockerfile.consumer -t "$ACR_NAME.azurecr.io/careers-consumer:latest" .
docker push "$ACR_NAME.azurecr.io/careers-producer:latest"
docker push "$ACR_NAME.azurecr.io/careers-consumer:latest"
```

**3. Confirm `kustomization.yaml` matches** the registry you pushed to:

```bash
kubectl kustomize k8s/ | grep 'image:'
```

The hostname must be **`$ACR_NAME.azurecr.io`** (same `ACR_NAME` as above).

**4. Confirm AKS can pull from ACR**

```bash
az aks check-acr --resource-group "$RG" --name "$CLUSTER" --acr "$ACR_NAME.azurecr.io"
```

If this fails, attach the registry again (see §5):

```bash
ACR_ID="$(az acr show -g "$RG" -n "$ACR_NAME" --query id -o tsv)"
az aks update -g "$RG" -n "$CLUSTER" --attach-acr "$ACR_ID"
```

Wait until the update finishes, then delete stuck pods so they are recreated:

```bash
kubectl delete pods -n wsc-careers --all
kubectl delete job -n wsc-careers careers-producer-once
kubectl apply -k k8s/
```

**5. Two consumer pods during rollout**

If you briefly see two `careers-consumer` pods, that is often a rolling update; it should settle to one replica. If one stays `ImagePullBackOff` forever, it is usually an old `ReplicaSet`—check `kubectl get rs -n wsc-careers` and the image on each RS.

**6. `no match for platform in manifest` (Apple Silicon Mac)**

You built **arm64** images locally; AKS nodes expect **linux/amd64**. Rebuild and push with **`--platform linux/amd64`** (see §5 `docker build` lines). If `docker build` complains, enable Docker **containerd** / **Rosetta** or use:

```bash
docker buildx build --platform linux/amd64 --push -f Dockerfile.producer -t "$ACR_NAME.azurecr.io/careers-producer:latest" .
docker buildx build --platform linux/amd64 --push -f Dockerfile.consumer -t "$ACR_NAME.azurecr.io/careers-consumer:latest" .
```

**7. `401 Unauthorized` when pulling from ACR**

Re-run **`az aks update ... --attach-acr "$ACR_ID"`** and **`az aks check-acr`**. Ensure ACR and AKS are in the same subscription (or grant **AcrPull** to the cluster’s kubelet managed identity on the registry).

## Save money when idle

- **Delete everything** when the demo is graded:  
  `az group delete --name "$RG" --yes --no-wait`
- **Stop/start** (if you need to pause; behavior and billing follow current Microsoft docs—confirm in portal):  
  `az aks stop --resource-group "$RG" --name "$CLUSTER"`  
  `az aks start --resource-group "$RG" --name "$CLUSTER"`

## Rough sanity check

- Expect on the order of **tens of USD/month** if the cluster runs 24/7 on a single small VM—**your invoice is the source of truth** (region, disk, tier, hours).
- For a **few days** of interview demo, cost is usually modest if you **delete the resource group** afterward.

Manifests in this folder: `configmap.yaml`, `producer-job.yaml`, `producer-cronjob.yaml`, `consumer-deployment.yaml`, `kustomization.yaml`, plus `secret-env.example` (copy to `secret.env` locally).
