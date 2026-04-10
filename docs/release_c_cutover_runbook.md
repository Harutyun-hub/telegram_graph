# Release C Cutover Runbook

## Owner
- Repository owner

## Status
- Draft placeholder
- This runbook must be completed during Release B staging validation before any production split rollout begins.

## Purpose
- Control the production move from single-service `APP_ROLE=all` to `web + worker + Redis`.

## Required Sections
- Deployment order
- Feature-flag posture
- Rollback order
- Restore checkpoints
- Smoke sequence
- Duplicate-processing guard checks
- Redis outage handling
- Materializer rollout order

## Exit Rule
- Release C cannot begin until this runbook is complete and reviewed.
