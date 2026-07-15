# Triage Labels

The engineering skills speak in canonical triage roles. This file maps those roles to the actual GitHub labels used in this repo.

| Role | Label | Meaning |
| --- | --- | --- |
| `bug` | `bug` | Something is broken |
| `enhancement` | `enhancement` | New feature or improvement |
| `needs-triage` | `needs-triage` | Maintainer needs to evaluate this issue |
| `needs-info` | `needs-info` | Waiting on reporter for more information |
| `ready-for-agent` | `ready-for-agent` | Fully specified, ready for an AFK agent |
| `ready-for-human` | `ready-for-human` | Requires human implementation |
| `wontfix` | `wontfix` | Will not be actioned |

Every triaged issue should carry exactly one category label (`bug` or `enhancement`) and exactly one state label (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, or `wontfix`).

## Crate Scopes

Crate scope labels are optional and orthogonal to the category and state roles.
Derive them from the Cargo package name as `scope:<package-name>`, and apply
every crate whose owned code or public contract is in the issue. Do not replace
the four plugin scopes with a broad `scope:plugin` label.

| Cargo package | Scope label |
| --- | --- |
| `hammer-infra` | `scope:hammer-infra` |
| `hammer-core` | `scope:hammer-core` |
| `hammer-component-macros` | `scope:hammer-component-macros` |
| `hammer-runtime` | `scope:hammer-runtime` |
| `hammer-service` | `scope:hammer-service` |
| `hammer-plugin-tun` | `scope:hammer-plugin-tun` |
| `hammer-plugin-ip` | `scope:hammer-plugin-ip` |
| `hammer-plugin-tcp` | `scope:hammer-plugin-tcp` |
| `hammer-plugin-udp` | `scope:hammer-plugin-udp` |
| `hammer-app` | `scope:hammer-app` |
| `hammer-ipc` | `scope:hammer-ipc` |
| `hammer` | `scope:hammer` |
| `hammerctl` | `scope:hammerctl` |

When workspace membership changes, update this table and the GitHub label
vocabulary in the same change.
