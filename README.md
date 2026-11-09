# scifetch-paper Hermes Plugin

Upgrade-safe SciFetch paper retrieval for stock Hermes. The plugin lives outside
the Hermes source checkout and uses only public plugin APIs.

## Install and connect

Install the standalone plugin in the Hermes profile that will receive paper
requests. The plugin's Git repository name is `hermes-scifetch-plugin`; its
registered Hermes name is `scifetch-paper`.

```bash
hermes plugins install YogurtBiu/hermes-scifetch-plugin
hermes plugins enable scifetch-paper
hermes plugins list
```

The plugin connects over HTTP to an existing SciFetch service. Ask the service
administrator for an address reachable from this computer. If the service listens only on another
computer's `127.0.0.1`, arrange a tunnel or an accessible service endpoint first.

For a personal Hermes instance, add the following settings to the intended
profile's `.env` and keep its existing settings. The default macOS/Linux profile
uses `~/.hermes/.env`. Replace the example URL with the administrator's address.

```dotenv
SCIFETCH_SERVER_URL=https://your-scifetch-host
SCIFETCH_TRUST_MODE=personal_instance
```

On Windows, use the target profile's official configuration commands so Hermes
chooses its own profile directory.

```powershell
hermes config set SCIFETCH_SERVER_URL https://your-scifetch-host
hermes config set SCIFETCH_TRUST_MODE personal_instance
```

The Store/MSIX installation keeps application code under `WindowsApps`, while
the ordinary default profile is commonly under `%LOCALAPPDATA%\hermes`.
`HERMES_HOME` and named profiles can select a different location. The plugin
uses Hermes' public profile APIs to locate its settings and state.

Use `personal_instance` when everyone authorized to use this Hermes profile may
share one SciFetch account. Each member can instead use their own Hermes
profile and pair with their own account. The [trust modes](#trust-modes) section
describes the account boundary and the current multi-user restriction.

In the same profile, run `hermes tools` and enable the `scifetch` toolset for the
chat platform you use. Restart that profile's gateway with its service manager
after publishing the plugin and configuration. Complete Hermes' own chat
authorization. In the SciFetch website, sign in with your own account and open
**个人设置 → 连接 Hermes → 生成配对码**. Enter your current website password in
the confirmation dialog and copy the one-time code. Then send these commands
to your own bot.

```text
/scifetch login <pairing_code>
/scifetch status
```

Pairing codes are single-use and normally expire after ten minutes.
Each computer pairs independently and keeps its
own installation/device identity. Keep administrator credentials on the server
administration side.

After login, send a DOI or supported paper URL as an ordinary chat message.
For example, send `10.1021/jacs.5c23137`. The acknowledgement supplies a request
ID. Query it with `/paper status <request_id>`. The deterministic gateway route
uses the original DOI/URL event, so ordinary DOI/URL messages are the supported
submission entry point. `/paper status`, `/paper summary` and `/paper replay`
inspect an existing request.

Runtime compatibility includes the exact audited Hermes
`0.22.5@bf0203a794a07aac3ff764972ab9087baa4e4f4e` point and the other exact
points listed under [Stock Compatibility](#stock-compatibility). Use Hermes'
Python environment with `httpx` available. Version `0.2.28` adds a Windows native
backend targeting x64 Hermes `0.23.0` and Python `3.14.9`. It uses OS file locks
and read-only process queries while macOS/Linux retain the POSIX backend.
The [Windows native workflow](https://github.com/YogurtBiu/hermes-scifetch-plugin/actions/workflows/windows.yml)
records the native installation and runtime contract results for each candidate.
Installing the plugin and delivering a native file are separate checks. File
delivery depends on the audited platform route and its accepted file/size limits.

The release's `SOURCE.json` records the source commit, plugin version and SHA256
of each published plugin file. See the official
[Hermes plugin guide](https://hermes-agent.nousresearch.com/docs/user-guide/features/plugins)
for the host's installation and enable controls.

## Updating a Git installation

For an installation already managed from this repository, update its code with
the target profile's official command and restart the gateway.

```bash
hermes plugins update scifetch-paper
```

Keep the same profile, saved installation identity and local delivery ledger.
Back up state only after stopping new submissions and draining active file
sends. Preserve receipts written after any older backup. A copied plugin
directory with the same name needs a reviewed migration before a Git install
can replace it. When moving to another computer, install into a fresh profile
and obtain a new pairing code.

If the plugin fails to load, check the actual gateway interpreter, profile,
enabled plugin list and `httpx` import. For `trusted_command_context_unavailable`,
check the selected trust mode and use an ordinary DOI/URL message for a new
request. The remaining sections describe transport and recovery details.

## Windows Git certificate download error

Git for Windows may report `CRYPT_E_REVOCATION_OFFLINE` while Schannel checks
certificate revocation. If this read-only OpenSSL probe returns a commit SHA,
the same repository can be reached with that TLS backend.

```powershell
git -c http.sslBackend=openssl ls-remote https://github.com/YogurtBiu/hermes-scifetch-plugin.git HEAD
```

The `-c` value applies to that Git process only. Save a repository-specific
setting so Git processes started by the Hermes installer use the same backend.

```powershell
git config --global 'http.https://github.com/YogurtBiu/hermes-scifetch-plugin.git.sslBackend' openssl
hermes plugins install YogurtBiu/hermes-scifetch-plugin
```

This setting matches the plugin repository URL and keeps certificate validation
enabled. To remove it, run the following command.

```powershell
git config --global --unset 'http.https://github.com/YogurtBiu/hermes-scifetch-plugin.git.sslBackend'
```

The configuration behavior is described in the official
[Git config reference](https://git-scm.com/docs/git-config).

## Trust Modes

The plugin has two mutually exclusive modes. The default is the safer
`multi_user` mode.

### Personal Instance

Use this only when one person controls the Hermes profile, or every user allowed
through that profile's gateway may share the same SciFetch account:

```bash
export SCIFETCH_TRUST_MODE=personal_instance
export SCIFETCH_SERVER_URL=https://scifetch.example.test
```

For a managed gateway, persist these values in the intended Hermes profile's
configuration. The plugin uses public `agent.secret_scope.get_secret` and
`hermes_constants.get_hermes_home`; an interactive shell's process exports are
not the active gateway profile. It never parses `.env` itself or infers the
trust mode from saved credentials. Missing or invalid scoped settings fail closed.

Then create a pairing code through SciFetch and run:

```text
/scifetch login <pairing_code>
10.1038/nature12345
```

The plugin generates an opaque installation ID and stores it with device
credentials under the current Hermes profile. SciFetch binds the device token
to `personal_instance + installation_id`; missing, forged, or mixed chat
principal headers are rejected.

Token rotation is single-flight for the whole profile across command/tool
handlers and background watchers. The current submission refreshes before old
pending deliveries are resumed. If refresh is rejected, no new request or
watcher starts; pending records wait in `auth_required` until a new
`/scifetch login` succeeds.

On the audited stock versions listed below, a complete payload containing only
explicit DOI/DOI URL, supported publisher or Weixin URL, arXiv URL/ID, or
Elsevier PII lines is model-free. The pre-auth hook stages a task-local
transport lease and rewrites the event to the internal `/paper` command. Stock
Hermes then performs its normal gateway authorization and slash ACL before the
command submits anything. Each line may independently begin with `!!`; order
and repeated inputs are preserved. Mixed prose, titles/citations, unknown URLs,
raw `/paper`, `multi_user`, and unknown Hermes versions abstain or fail closed
before SciFetch HTTP.

Ambiguous personal messages still use the registered `scifetch_paper_request`
tool and official `pre_llm_call` guidance. That hook itself makes no HTTP
request. When Hermes coalesces several rapid messages into one turn, guidance
requires one tool call per occurrence in source order, including repeats. The
official `transform_llm_output` hook returns exact plugin results in call order
so emoji and line breaks survive model mediation. Watchers scheduled by either
the deterministic command or the tool turn share an ACK gate appropriate to
that path and begin only after the complete ACK block plus a fixed 0.5-second
gateway delivery grace. This is ordering, not proof that the platform displayed
the ACK. The bounded 60-second fallback and cleanup paths prevent a permanent
wait. Official `on_session_end` and
`on_session_finalize` hooks discard any turn-scoped results left by an
interrupted, empty, or destroyed session; these cleanup hooks never call
SciFetch.
Because `pre_llm_call` cannot skip agent dispatch, only ambiguous/title-like
messages still depend on the model invoking the tool.
Verify that the `scifetch` plugin toolset is enabled for the target platform in
Hermes' official `hermes tools` UI; do this first in a temporary profile.

### Multi-User Audit

```bash
export SCIFETCH_TRUST_MODE=multi_user
```

This mode binds each request to a trusted platform/user/chat principal. Generic
`source` and session keywords are not accepted as that principal by this
integration. Until the official principal/message contracts are separately
audited and tested, every stateful SciFetch
command (`/paper`, `/scifetch login|status|logout`, and `/paper-digest`), plus
the free-form tool path, fails closed with zero SciFetch HTTP requests.

Multi-user command support waits for official
[Hermes PR #51596](https://github.com/NousResearch/hermes-agent/pull/51596) or
its released successor. Free-form messages separately wait for a future
official message context. Neither an unmerged PR nor a private Hermes patch is a
deployment dependency.

## Registered Surface

- Hooks: `pre_gateway_dispatch`, `pre_llm_call`, `transform_llm_output`,
  `on_session_end`, `on_session_finalize`.
- Commands: `/scifetch`, `/paper`.
- Tool: `scifetch_paper_request`.
- Optional command: `/paper-digest` when
  `SCIFETCH_PERSONAL_DIGEST_ENABLED=1`.

`pre_gateway_dispatch` is a pre-auth transport observer and always performs zero
SciFetch HTTP, token/state reads, or outbound sends. In `personal_instance`, a
strict complete payload on an exact tested stock version may return an internal
`rewrite` after staging a current-task transport lease. Every other branch
returns `None`; no branch grants authority.

SciFetch does not turn generic command/session keywords into trusted multi-user
principals or direct `send_text` / `send_document` capabilities. For a
paper-like personal-instance Weixin/QQ message, `pre_gateway_dispatch`
temporarily stages the documented gateway adapter route without sending or
making a SciFetch HTTP request. After authorization, `pre_llm_call` supplies
`session_id`, `platform`, and `sender_id`. The plugin enables delivery only
when sender/platform match and the normalized authenticated message is either
the raw message or ends with it after a whitespace boundary (to allow only
Hermes-added leading context), with one unique staged chat. Every SciFetch tool
call in that same authenticated turn may reuse the route by `session_id`; the
official session lifecycle hooks remove it after the turn. Each watcher calls
the concrete adapter's native `send_document`. Correlating the newest message
also consumes older staged candidates from the same sender/chat burst so they
cannot leak into a later turn. This is a transport bridge for an
installation-bound private profile, not a multi-user principal contract.

The model-free route is validated only at
`0.18.2@bd740f203b44237dbc5c27a2de4d86ef32af4dde`,
`0.20.0@03fa32c92dd445eb64c7f67434dd91b32c40701d`,
`0.21.2@848075c501`, `0.21.3@6005aa1fd9aac8b1024ace50fec8cd1c85a04bae`,
`0.22.5@bf0203a794a07aac3ff764972ab9087baa4e4f4e`,
and the current `0.23.0@2bd378ed1b8d6bf2192cb26e0a55ff3091e6bd19` integration;
these are exact points, not a
continuous compatibility range. Adding a version means two edits plus two runs:
add it to `TESTED_HERMES_VERSIONS` in `delivery.py` and to
`_DETERMINISTIC_ROUTE_VERSIONS` in `home_delivery.py`, then run the
`test_native_delivery_version_sets_stay_in_sync` unit test and the stock contract
suite from a read-only export of that host:

```bash
git -C <hermes checkout> archive <commit> | tar -x -C /tmp/host
uv pip install --target /tmp/pytest-deps pytest pytest-asyncio
cd /tmp/host && PYTHONDONTWRITEBYTECODE=1 \
  PYTHONPATH=<repo>/src:/tmp/pytest-deps <managed hermes python> -m pytest \
  <repo>/tests/contract/test_hermes_stock_plugin.py -q -c <repo>/pyproject.toml
```

The contract test asserts the running host version is already in the set, so the
edit comes first and the run proves the rest of the surface. Existing
native-delivery entries remain
version-gated, with known types, sizes, and explicit platform budgets. Weixin
uses a bounded 100 MiB personal-profile upload budget;
override it with `SCIFETCH_WEIXIN_NATIVE_UPLOAD_MAX_BYTES`. QQ file delivery
uses a decimal 200 MB local preflight limit, downward-configurable with
`SCIFETCH_QQBOT_NATIVE_UPLOAD_MAX_BYTES`. QQ C2C and ordinary group are
accepted only when the complete stock raw-event relationship distinguishes them
from Guild/Guild DM and an original `message_id` is present. That ID is passed
as `reply_to` for at most three watcher attempts within 60 minutes in C2C or
four within 5 minutes in ordinary groups. This reserves one of QQ's four/five
passive replies for Hermes' ordinary agent response. After the time or count
boundary, the wrapper returns an explicit local deferred result without calling
the adapter. The missing file remains pending until a later authenticated
message for the same QQ target supplies a fresh passive context. Audited Weixin
also accepts `.mp4` when its server content type is `video/mp4` or
`application/octet-stream`; stock Hermes maps that path to native
`MEDIA_VIDEO`. This exception is not shared with QQ or WeCom. Unknown
version/platform/context remains link-only or `unsupported` and never calls a
document sender. A raw `/paper` has no verified same-event lease and returns
`trusted_command_context_unavailable` with zero SciFetch HTTP. The plugin uses
only the `gateway.adapters` mapping documented for the hook; it does not call
private gateway/auth methods or retain a Base adapter fallback.

The implementation follows the official
[plugin guide](https://hermes-agent.nousresearch.com/docs/zh-Hans/user-guide/features/plugins)
and [hook reference](https://hermes-agent.nousresearch.com/docs/zh-Hans/user-guide/features/hooks):

- Tool and hook handlers accept `**kwargs` for forward-compatible host context.
- Slash commands declare `args_hint` so gateway adapters can expose their
  argument field, but the handler still receives only `raw_args` on audited
  stock Hermes.
- `ctx.dispatch_tool()` does not synthesize a parent agent or trusted sender
  identity. Current stock Hermes also intentionally does not register
  `send_message` as an agent-callable tool, so SciFetch does not use that path
  for background delivery.
- `ctx.inject_message()` is CLI-only and returns `False` in gateway mode.
- `pre_gateway_dispatch` exposes the documented gateway adapter before auth.
  A strict personal payload may stage a task-local route and request an internal
  rewrite; ambiguous/model-assisted delivery may stage the older candidate
  route. Neither route can submit, send, or authorize anything. Stock auth and
  slash ACL remain authoritative; multi-user mode still waits for a complete
  official message context.
- `transform_llm_output` uses Hermes' first-non-empty-result rule. Deployments
  that enable another output-transforming plugin must include that plugin in
  compatibility tests because an earlier transform can own the final text.

## Delivery

The plugin watches accepted requests, replays server events after restart, and
delivers a native document only when every preflight input is known: tested
Hermes version, platform/chat type, explicit document and local-path support,
known maximum size, supported file type, known actual size, and a non-Base
document sender.

| Target | Decision |
| --- | --- |
| QQBot strictly classified C2C | Native local-file candidate up to 200,000,000 bytes; within 60 minutes, preserve `message_id` for at most 3 watcher replies; then defer without an adapter call |
| QQBot strictly classified ordinary group | Native local-file candidate up to 200,000,000 bytes; within 5 minutes, preserve `message_id` for at most 4 watcher replies; then defer without an adapter call |
| QQ Guild/Guild DM or incomplete public group/dm | Short link or `unsupported`; no document sender |
| Weixin personal free-form DM on audited Hermes | Native current-chat candidate after the authenticated sender match, up to the bounded profile upload budget (100 MiB by default); approved documents, verified `.zip` archives, and `.mp4` use the stock native media sender |
| WeCom WebSocket | Native candidate up to about 20 MiB with complete preflight |
| WeCom Callback | `document_upload=false`; never call Base text fallback as a document |
| Missing/unknown field, platform, version, type, or size | Short link without `send_document` |

The concrete adapter must return an explicit successful `SendResult`. Missing
sender/chat correlation, adapter errors, and inherited Base text fallback are
never document success.

Plugin `0.2.17` contains one narrow compatibility bridge for audited stock
Weixin adapters that omit structured taxonomy for the exact
`novac2c.cdn.weixin.qq.com` TLS handshake failure. It applies the existing
bounded retry policy, then keeps the file `deferred` and tells the user that it
will be retried after the next paper message for the same session. A
deterministic DOI/URL command also wakes matching pending deliveries, so
recovery does not depend on `/paper`. The plugin does not alter Hermes SSL state
and does not claim native upload success. The next authenticated paper message
for that same target can resume the file under the recovery rules below.

Plugin `0.2.19` keeps that bridge and prefixes a claimed historical terminal
result with `📜【之前请求的结果（非本次请求）】`, its old request ID, and explicit
lines saying that the current request's status is governed only by the new
request ID shown in the ACK above and that the following text is the old
recorded terminal state.

Plugin `0.2.20` removes that automatic terminal-history claim. A fresh `/paper`
submission now returns only its exact current ACK block and tracks only the new
request IDs. Existing `terminal_text_pending` records remain dormant and
unchanged for inspection; they cause no old SciFetch HTTP/SSE/download work and
no old document or text callback. This does not disable recovery for real
pending/deferred files: matching file-delivery records still resume through the
existing `resume_pending_deliveries` path. An old request remains explicitly
inspectable with `/paper status <request_id>` (or its read-only `replay` alias).

Audited Weixin accepts canonical `.zip` plus `application/zip` metadata and the
historical `.zip` plus `application/octet-stream` shape. After the normal
capability and size preflight, the download must pass the existing size/hash
checks and a non-extracting ZIP structure check before `send_document`; an
invalid archive records `unsupported_media_type` through the explicit
link/unsupported path with zero native document calls. QQBot and WeCom do not
inherit the historical octet-stream exception.

QQ watcher callbacks never fall through to an active adapter call after the
captured passive reply window or shared reply budget is exhausted. The request
stays identifier-only in deferred pending state until a later authenticated
message for the same QQ target supplies a fresh passive context; unrelated
targets cannot resume it.

Actionable failure/partial terminal text remains a dormant
`terminal_text_pending` record rather than being represented as a deferred
file. A later fresh request neither deletes nor automatically displays that
record.

Link delivery is `delivered_via_link` only when text sending explicitly reports
success. `None` and failures are `unsupported`. Rate-limit retries honor
`retry_after`; text fallback is never document success.

An incomplete-SI summary reports `si_emitted` as files **obtained** from the
publisher (`已获取 n/m`), not as native sends. Individual delivery receipts
remain the authority for `sent`, `delivered_via_link`, and `unsupported`.

## State and Recovery

State defaults to the current Hermes profile:

```text
$HERMES_HOME/plugins/scifetch-paper/state.json
```

The file mode is `0600`. It contains device-bound user credentials and the
installation/principal binding, never an admin token or server-local file path.
Pending delivery state stores identifiers and cursors, not bearer tokens,
signed URLs, or filesystem paths. Schema v2 may also keep one sanitized,
ordered failure/partial terminal text payload and its callback outcome. Such a
record is dormant across restart: it opens no old SSE, download, or send. The
fresh request path does not claim, delete, append, or otherwise process it;
only active pending/deferred file records enter automatic recovery. Successful
terminal summaries are never stored. A Weixin `success=True` text receipt is
recorded as adapter-accepted-unverified, not as display/read confirmation.

Useful commands:

```text
/scifetch status
/scifetch logout
/paper status <request_id>
```

When Personal Paper Digest is explicitly enabled:

```text
/paper-digest status
/paper-digest due [limit]
```

## Stock Compatibility

Plugin `0.2.27` adds Hermes `0.23.0@2bd378ed1b` to the audited version sets after
the stock contract suite passed on a read-only export of that host (42 passed).
Only the two version constants changed; the delivery surface of the Weixin
adapter and `BasePlatformAdapter.send_document` is identical between `0.22.5` and
`0.23.0`, so no delivery behaviour changed.

Plugin `0.2.24` keeps mixed request summaries scoped to each item, including
separate occurrences of the same DOI and the latest retry outcome.

Plugin `0.2.23` reports native-accepted SI separately from acquisition completion
when browser cleanup fails. Status inspection describes generated files without
claiming they were sent; failed or unknown completion remains visible.

Plugin `0.2.22` integrates the actual managed Hermes `0.22.5` runtime. Scoped
trust, server settings and credential paths use its public profile APIs. Runtime
identity comes from `get_version_info().derived_version`, since managed package
metadata may be `0.0.0`; development/private suffixes and failed probes remain
closed. Only genuinely unavailable older host APIs use the existing legacy path.

The official synchronous command executor copies context into a worker thread.
The plugin transfers only the original task's transport lease and additionally
requires the exact same public `source` object on an off-loop claim. Unrelated
async children, plain workers, wrong sources, expired leases and replays cannot
submit. This is transport ownership after normal host authorization/ACL, never a
multi-user principal. Current isolated-stock and live results are recorded in
the owning task's deployment follow-up; a local registry check alone is not a
real Weixin reply.

Plugin `0.2.21` adds the audited Hermes `0.21.2` release to deterministic
personal routing and native delivery. Explicit DOI and Weixin URL payloads
preserve `!!`, duplicates, authorization/ACL ordering, and the current request
ACK without reaching the model. Unknown versions remain closed. The stock
contract accepts native document implementations inherited from a stock mixin,
while still rejecting the Base adapter's text fallback.

The previous `0.2.21` follow-up additionally admitted the audited Hermes
`0.21.3@6005aa1fd9aac8b1024ace50fec8cd1c85a04bae` point. Its independent
stock run passes all 36 plugin contracts, including native-file failure versus
text success, and the official Weixin/QQ/pre-gateway focused suites. This
did not admit later, prerelease, or private-suffixed versions and did not
establish live recipient delivery. No Hermes source patch is required.

On hosts supporting the official auxiliary-task, task-routed completion, and CLI APIs, it registers
the local `hermes scifetch-resolve` command and owns the `scifetch_resolve` task.
SciFetch can use this command when that auxiliary task uses `openai-codex`:
the host's `ctx.llm.complete(task="scifetch_resolve")` performs one completion
and owns OAuth/token refresh. The server never imports Hermes internals or
copies OAuth tokens. Configure `query_resolution.llm_hermes_command` with the
Hermes executable path if it is absent from the server service's PATH.

The command accepts bounded JSON on stdin, returns a bounded JSON envelope,
and does not run agent tools or submit paper requests. `--check` verifies
registration without a model call. Provider errors return a fixed redacted
error, never raw exception text. There is no gateway slash-command equivalent;
this is local operator IPC. Older hosts without these public APIs (including
hosts whose `complete` method does not accept `task`) retain their
existing paper hooks and commands, and do not register the completion command.

Compatibility tests copy only this plugin into a temporary `HERMES_HOME`; they
do not inject the SciFetch server `src` tree into `sys.path`. The model-free
route covers the exact stock points `0.18.2@bd740f2`, `0.20.0@03fa32c9`,
`0.21.2@848075c501`, `0.21.3@6005aa1fd9`, `0.22.5@bf0203a794`, and the current
`0.23.0@2bd378ed1b` integration. Tests exercise the real stock gateway
authorization/ACL and command path, model sentinel, ordered repeated submissions, pre-auth and
multi-user zero-HTTP behavior, authenticated Weixin/QQ adapter bridges, reload
stability, and the four-channel delivery matrix. No contract test performs a
live platform send, and neither stock checkout carries a SciFetch source diff.

## Isolated Install

For an installation check, use a temporary Hermes profile and the published Git
source. Start from an empty directory and run the install commands above in
that profile. Verify the enabled plugin and its registered tools before adding
any test credentials.

Runtime contract checks use temporary profiles, fake adapters and non-production
credentials. Production paper submissions and native chat-file acceptance are
validated separately through actual authorized messages.

## Durable native delivery and recovery

Each authenticated personal installation owns `delivery_outbox.sqlite3` beside
its token state and one stable installation sender lock. SSE only records exact
request/item/acquisition/generation/artifact readiness. Independent staging,
native and audit workers run after the existing command/tool ACK gate. A native
MAIN acceptance is committed locally before remote audit; only that exact
request and target can then send its ordered SI. Files are checked against the
immutable server grant and local hash/size. Text, links and MEDIA are never
native acceptance. The audited Weixin/QQ routes reject unsupported thread/topic
contexts rather than collapsing them into the parent conversation.

A false/missing result, timeout or lost receipt write after possible submission
remains `unknown`, without automatic replay. Uploads still running after a
timeout retain their target lane; a matching late result can resolve the same
attempt. Native acceptance does not claim recipient delivery or read. Server
audit repair uses stable attempt identities and never repeats file sending.
QQ window/budget preflight and actual platform Retry-After remain enforced;
there is no generic two-second MAIN-to-SI delay.

`/paper status <request_id>` includes local native status. For a stopped unknown
attempt, the explicit authenticated profile command is:

```text
/paper retry <request_id> <artifact_id> <target_binding_id> confirm-duplicate-risk
```

The status output supplies the immutable file and opaque target binding. The
command verifies current request access and exact local ownership. It refuses
without durable proof that the old native call stopped, and it never invents an
official platform reconciliation API. A successful authorization retains the
unknown attempt and queues a linked retry; a fresh authenticated message at the
same target and its ACK are still required. Retrying can duplicate an earlier
platform acceptance. Model tool arguments cannot silently authorize this action.

Old JSON records lacking immutable identity/native evidence are imported once
as quarantined observations, never assumed safely pending or accepted. Historical
failure text remains dormant. Current failure/partial text is stored before its
callback in SQLite and is not attached to a later request.

## Rollback

Stop new sender admission and drain actual native calls, or preserve their
unknown/quarantined state, before taking a restricted ledger/artifact snapshot.
Preserve post-snapshot receipts, pending SI, exact-target pins and audit backlog.
Never restore an older pending database over files sent afterward. An old sender
that cannot represent these facts must stay disabled; restoring acquisition does
not authorize restoring unsafe sending. Moving an installation to another ledger
path requires a reviewed migration under the same stable sender lock.

No real Hermes configuration or checkout change is required. Hermes has no
SciFetch deployment commit to revert. Production rollout and real file sends
remain separately authorized operations.


## Public failure explanations

The plugin includes its own dependency-free `public_errors.py`; distribute it
with the plugin directory. It does not import the SciFetch server package.
Public request/status, submission and file-send explanations use Chinese while
machine categories, retry decisions and native-delivery records remain intact.
Shared offline semantic vectors check agreement with the server presentation.
Acquired files are not called sent until the existing native receipts accept them;
uncertain sends retain uncertainty and do not imply permission to resend.

## Weixin terminal summary recovery

Plugin `0.2.25` retains Weixin terminal summaries with each segment's actual text-send
outcome, including successful file-delivery summaries. File acceptance and
summary text acceptance are independent. A rejected or timed-out summary never
reopens an accepted file or triggers a new download.

Query the original request explicitly in the bot conversation:

```text
/paper status req_...
```

`/paper summary req_...` and `/paper replay req_...` are the same read-only
status aliases. The reply shows available acquisition facts, local native file
acceptance and retained summary outcomes/content. It does not resend files or
invoke a stored old text callback. Missing historical summary observations are
reported as missing; reconstructed current facts are not labelled original text.
Ordinary new paper requests and restarts leave historical summaries dormant.
Platform acceptance is not recipient display/read confirmation.

### Plugin `0.2.26`: download mode copy

Ordinary newly acquired files display `已下载`; cached originals and cached MAIN
with newly completed SI retain their separate source descriptions. Forced-request
ACKs confirm recognition only. Final summaries and read-only status describe the
matching item's confirmed fresh outcome (completed, partial, failed, not attempted
or unconfirmed); missing/contradictory facts never prove success. Persisted older
summary text and native receipts are not rewritten or replayed.

The existing leading `!!` syntax also accepts `！！`, `!！` and `！!`, including
ordered multiline submissions. Only the leading pair is consumed; URL/title
contents, single punctuation and the original authorization payload are preserved.
Server APIs and retrieval/delivery policy are unchanged.
