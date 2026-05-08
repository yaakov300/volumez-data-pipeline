# Insights Summary

## Overview

This pipeline ingests real-time Wikimedia events, transforms them into a structured table, and generates insights on editing behavior.

The goal was to identify meaningful patterns in how pages are edited over time.


## Key Insights

### 1. Edit-War Detection

We identified pages with unusually high edit activity within short (5-minute) windows.

**Observation:**  
Some pages show sudden spikes in edits.

**Interpretation:**  
These spikes may indicate:
- controversial topics  
- breaking events  


### 2. Edit Size vs Namespace

We analyzed how edit sizes vary across namespaces.

**Observation:**  
Some namespaces contain mostly large edits, while others are dominated by small ones.

**Interpretation:**  
- Large edits → major content changes  
- Small edits → fixes or automated updates  
- Behavior differs across content types  


### 3. Bot vs Human Activity

We compared bot and human editing patterns.

**Observation:**  
Bots contribute a significant portion of edits, mostly small and frequent.

**Interpretation:**  
Bots maintain content, while humans drive larger, more variable changes.


## What I Would Build Next

Given more time, I would focus on:

- Advanced edit-war detection (statistical thresholds, revert tracking)  
- Pipeline observability (monitoring, data freshness, alerts)  
- Currently, the insight data is calculated using all records in the staging table. It might be better to process only the newly ingested data.
 


