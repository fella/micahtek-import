# MicahTek Import

Local development environment for parsing MicahTek `.CRD` files, generating deterministic transaction keys, tracking import runs, and preparing donor/gift payloads for HubSpot.

## Local setup

1. Copy `.env.example` to `.env`
2. Start containers:

```bash
docker compose up --build# micahtek-import
