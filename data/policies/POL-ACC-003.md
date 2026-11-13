---
policy_id: POL-ACC-003
title: Data Access and Masking Standard
department: operations
owner: Data Platform Security
effective_date: 2026-03-10
---
# Data Access and Masking Standard

## 1. Column Masking
Customer identifiers in Silver and Gold tables are masked by default for all analyst roles. Unmasked values are visible only to principals granted the pii_reader role.

## 2. Access Reviews
Data owners must recertify every access grant to restricted tables each quarter. Grants that are not recertified within 14 days of the review deadline are revoked automatically.

## 3. Break-Glass Access
Break-glass production access expires automatically after 4 hours. Every break-glass session must be linked to an incident ticket and reviewed by the platform lead.
