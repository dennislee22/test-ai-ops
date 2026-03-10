#!/usr/bin/env bash

URL="https://ecs-ai-ops.ml-9df5bc51-1da.apps.cdppvc.ares.olympus.cloudera.com/api/ask"

QUESTIONS=(
    "list all pods"
    "is the vault pod doing ok?"
    "which namespace has the least pods?"
    "list all namespaces with total pods"
    "which namespace contains ingress port 443?"
    "what storage type are pods in cdp namespace using?"
    "what SSL certificates does the cdp namespace have?"
    "what is the username and password for the SQL user for db-0 in cmlwb1 namespace?"
    "calculate CPU requests for all pods in longhorn-system namespace"
    "which node has a GPU available and in use?"
    "get tables in db-0 of cmlwb1 namespace"
    "explain how you access the database of a pod to get the table names, don't run it, just explain"
    "what can you do?"
    "how are you?"
)

TOTAL=${#QUESTIONS[@]}
PASS=0
FAIL=0

LOGFILE="test_api_$(date +%Y%m%d_%H%M%S).log"

exec > >(tee -a "$LOGFILE") 2>&1

sep() { printf '%0.s─' {1..72}; echo; }

sep
printf "  ECS AI Ops — Sequential API Test   (%d queries)\n" "$TOTAL"
printf "  Target: %s\n" "$URL"
printf "  Log:    %s\n" "$LOGFILE"
sep

for i in "${!QUESTIONS[@]}"; do
    Q="${QUESTIONS[$i]}"
    IDX=$((i + 1))
    printf "\n[%2d/%d] %s\n" "$IDX" "$TOTAL" "$Q"
    printf "       Sending… "

    START=$(date +%s)

    RESPONSE=$(curl -s \
        --max-time 1200 \
        -X POST "$URL" \
        -H "Content-Type: application/json" \
        -d "{\"message\": $(printf '%s' "$Q" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))'), \"history\": [], \"decode_secrets\": false}")

    STATUS=$?
    END=$(date +%s)
    ELAPSED=$((END - START))

    if [ $STATUS -ne 0 ]; then
        printf "FAILED (curl exit %d, %ds)\n" "$STATUS" "$ELAPSED"
        FAIL=$((FAIL + 1))
        continue
    fi

    ANSWER=$(echo "$RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    r = d.get('response') or d.get('answer') or d.get('text') or str(d)
    print(r[:600])
except Exception as e:
    print('Parse error:', e)
    sys.exit(1)
" 2>&1)

    if [ $? -ne 0 ]; then
        printf "PARSE ERROR (%ds)\n" "$ELAPSED"
        echo "       Raw: ${RESPONSE:0:200}"
        FAIL=$((FAIL + 1))
    else
        printf "OK (%ds)\n" "$ELAPSED"
        echo "$ANSWER" | sed 's/^/       /'
        PASS=$((PASS + 1))
    fi

    sep
done

printf "\n  Results: %d passed / %d failed / %d total\n\n" "$PASS" "$FAIL" "$TOTAL"
