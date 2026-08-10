# Testing the Shop reconcile flow (manual, against local kind)

**Goal:** verify the `shop-operator` correctly reconciles a `Shop` CR end-to-end —
creates the app Deployment, provisions the right database per tier, injects env,
reports status, scales on change, and cleans up on delete.

**Prereqs in the cluster:** CRDs installed (`make install`), and the CNPG +
OT-Container-Kit Redis operators running (they provide the `Cluster` and `Redis`
CRDs the operator creates).

## Setup

1. Install CRDs:
   ```bash
   make install
   ```

2. Run the operator locally against the kind cluster. Because there is **no real
   per-shop app image yet**, override the app image with a public placeholder so
   pods can actually start:
   ```bash
   SHOP_APP_IMAGE=nginx:alpine SHOP_APP_PORT=80 make run
   ```
   `SHOP_APP_IMAGE` / `SHOP_APP_PORT` are operator-level env vars read in
   `internal/controller/shop_controller.go` — they let us swap the app image
   without touching the CRD or every Shop CR.

3. Apply the test manifest (`config/samples/test_shop.yaml`) with a `Wallet`, a
   `DiscordChannel`, and two Shops:
   - `shop-standard` — `availability: standard`, `databaseType: standard` (Postgres/CNPG)
   - `shop-light-ha` — `availability: high`, `databaseType: light` (Redis)
   ```bash
   kubectl apply -f config/samples/test_shop.yaml
   ```

## What to verify

| Path            | How                                                              | Expected                                                                 |
|-----------------|-----------------------------------------------------------------|--------------------------------------------------------------------------|
| CR accepted     | `kubectl apply`                                                 | Wallet/DiscordChannel/Shops created (DiscordChannel requires `channelName` + `serverID`) |
| DB provisioning | `kubectl get cluster.postgresql.cnpg.io`, `kubectl get redis...` | CNPG `Cluster` healthy for standard; `Redis` CR for light                |
| App Deployment  | `kubectl get deploy` + `kubectl rollout status`                 | `shop-shop-standard` 2/2, `shop-shop-light-ha` 3/3 Running               |
| Status loop     | `kubectl get shop <name> -o jsonpath='{.status}'`              | `ready: true`, condition `ShopReady` once pods are up                     |
| Env wiring      | `kubectl set env deploy/<name> --list`                          | `SHOP_NAME`, `WALLET_REF`, `DISCORD_CHANNEL_REF`, tier-specific `DATABASE_*` |
| Scaling         | `kubectl patch shop ... availability: high`                     | replicas 2 → 3                                                            |
| Validation      | `kubectl apply --dry-run=server` with `availability: turbo`     | rejected: `Unsupported value`                                            |
| Cascade delete  | `kubectl delete shop <name>`                                    | Deployment/Service/DB removed via owner references                       |

### Example commands

```bash
# Watch pods come up
kubectl rollout status deploy/shop-shop-standard --timeout=120s
kubectl rollout status deploy/shop-shop-light-ha --timeout=120s
kubectl get pods -l app.kubernetes.io/managed-by=shop-operator

# Status flips to Ready
kubectl get shop shop-standard -o jsonpath='{.status}{"\n"}'

# Env wiring differs by DB tier
kubectl set env deploy/shop-shop-standard --list   # DATABASE_TYPE=standard + Postgres vars
kubectl set env deploy/shop-shop-light-ha --list   # DATABASE_TYPE=light + Redis vars

# Scaling via the CRD
kubectl patch shop shop-standard --type merge -p '{"spec":{"availability":"high"}}'
kubectl get deploy shop-shop-standard -w

# CRD validation guard
kubectl apply --dry-run=server -f - <<'EOF'
apiVersion: shophub.devops-siit.io/v1
kind: Shop
metadata: { name: shop-bad }
spec: { availability: turbo, walletRef: wallet-test, discordChannelRef: discordchannel-test }
EOF

# Cascade delete
kubectl delete shop shop-light-ha
kubectl get deploy,svc,redis.redis.redis.opstreelabs.in | grep light-ha
```

### Cleanup

```bash
kubectl delete -f config/samples/test_shop.yaml
```

## Gotchas worth knowing

1. **The default image `shophub/shop-app:latest` doesn't exist.** There is no single
   "shop-app" artifact — the app is split into microservices (auth/frontend/order/
   payment). For operator testing, `nginx:alpine` is a fine stand-in. Deciding what a
   Shop *actually* deploys is still an open design question.

2. **`SHOP_APP_IMAGE` only takes effect on the running operator process.** If an old
   `make run` is still up without the env var, every reconcile rewrites the Deployment
   back to the default image. Make sure only one operator runs, started with the env
   var. Confirm the live value with:
   ```bash
   kubectl get deploy shop-shop-standard -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
   ```

3. **`imagePullPolicy` isn't set on the app container**, so a `:latest` image defaults
   to `Always` and won't use a `kind load`ed local image. Using a non-`latest` tag
   (like `nginx:alpine`) avoids it. Candidate fix: set `IfNotPresent` in
   `reconcileDeployment`.

4. **On WSL**, the repo lives under `/mnt/c/...`, not `~` — `cd ~/...` fails.
