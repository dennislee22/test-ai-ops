#!/usr/bin/env bash

DEFAULT_HOST="cpu-ecs-ai-ops-cpu.ml-9df5bc51-1da.apps.cdppvc.ares.olympus.cloudera.com"
HOST="${1:-$DEFAULT_HOST}"
URL="https://${HOST}/chat/stream"

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
LOGFILE="test_api.log"

exec > >(tee "$LOGFILE") 2>&1

sep() { printf '%0.s─' {1..72}; echo; }

sep
printf "  ECS AI Ops — Sequential API Test   (%d queries)\n" "$TOTAL"
printf "  Target: %s\n" "$URL"
printf "  Log:    %s\n" "$LOGFILE"
sep

for i in "${!QUESTIONS[@]}"; do
    Q="${QUESTIONS[$i]}"
    IDX=$((i + 1))
    printf "\n[%2d/%d] [%s] %s\n" "$IDX" "$TOTAL" "$(date '+%d-%b-%Y %H:%M:%S')" "$Q"
    printf "       Sending… [%s]\n" "$(date '+%H:%M:%S')"
    START=$(date +%s)

    BODY=$(printf '%s' "$Q" | python3 -c '
import sys, json
q = sys.stdin.read()
print(json.dumps({"message": q, "history": [], "decode_secrets": False}))
')

    ANSWER=$(curl -s --max-time 1200 --no-buffer \
        -X POST "$URL" \
        -H "Content-Type: application/json" \
        -H "Accept: text/event-stream" \
        -d "$BODY" | python3 -c '
import sys, json
answer = None
for raw_line in sys.stdin:
    line = raw_line.strip()
    if not line.startswith("data:"):
        continue
    try:
        payload = json.loads(line[5:].strip())
    except Exception:
        continue
    if payload.get("type") == "result":
        answer = payload.get("response", "")
        break
if answer is not None:
    print(answer)
else:
    print("NO_RESULT")
    sys.exit(1)
')

    STATUS=$?
    END=$(date +%s)
    ELAPSED=$((END - START))
    printf "       Answered  [%s] — %ds elapsed\n" "$(date '+%H:%M:%S')" "$ELAPSED"

    if [ $STATUS -ne 0 ] || [ "$ANSWER" = "NO_RESULT" ]; then
        printf "FAILED (%ds)\n" "$ELAPSED"
        FAIL=$((FAIL + 1))
    else
        printf "OK (%ds)\n" "$ELAPSED"
        printf "\nQuestion: %s\n" "$Q"
        printf "Answer:\n%s\n" "$ANSWER"
        PASS=$((PASS + 1))
    fi

    sep
done

printf "\n  Results: %d passed / %d failed / %d total\n\n" "$PASS" "$FAIL" "$TOTAL"
